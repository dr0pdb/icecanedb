package storage

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"

	log "github.com/sirupsen/logrus"
)

const (
	defaultSkipListHeight = 18
	defaultNumberLevels   = 12
)

// Storage is the persistent key-value storage struct
// It contains all the necessary information for the storage
type Storage struct {
	dirname, dbName string

	options *Options

	mu *sync.Mutex

	// memtable contains the kv pairs in memory.
	memtable *Memtable

	logFile   file // the db file where the kv data is stored.
	logWriter *logRecordWriter

	ukComparator, ikComparator Comparator

	// snapshotDummy denotes the head of the list of snapshots stored in the db.
	// note that as of the current state of the project, storing this is redundant.
	// If and when we write to disk, this would be required to ensure that we don't delete sst files that are being referenced by a snapshot.
	snapshotDummy Snapshot

	// lastSequenceNumber is the sequence number of the last entry that was inserted in the db.
	// Init to 0 and monotonically increasing.
	lastSequenceNumber uint64
}

// Open opens a storage.
//
// It loads the version set from disk.
// If CreateIfNotExist option is true: It creates the db inside the set directory if it doesn't exist already.
func (s *Storage) Open() error {
	log.WithFields(log.Fields{"storage": s}).Info("storage::storage::Open; started")

	if _, err := s.options.Fs.stat(getDbFileName(s.dirname, s.dbName, dbFileType)); os.IsNotExist(err) {
		// Create the DB if it did not already exist.
		if err := s.createNewDB(); err != nil {
			return err
		}
	}

	logFile, err := s.options.Fs.open(getDbFileName(s.dirname, s.dbName, dbFileType))
	if err != nil {
		log.WithFields(log.Fields{"err": err.Error()}).Error("storage::storage::Open; error in opening db file")
		return err
	}
	defer func() {
		if logFile != nil {
			logFile.Close()
		}
	}()

	log.WithFields(log.Fields{"storage": s}).Info("storage::storage::Open; reading db file")

	// read log file, populate data in memtable
	lgr := newLogRecordReader(logFile)
	for {
		slgr, err := lgr.next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		data, err := ioutil.ReadAll(slgr)
		if err != nil {
			return err
		}

		wb := WriteBatch{data: data}
		seqNum := wb.getSeqNum()

		// write/update in memtable
		for itr := wb.getIterator(); ; seqNum++ {
			kind, ukey, value, ok := itr.next()
			if !ok {
				break
			}

			// update sequence number
			if seqNum > s.lastSequenceNumber {
				s.lastSequenceNumber = seqNum
			}

			ikey := newInternalKey(ukey, kind, seqNum)
			s.memtable.Set(ikey, value)
		}
	}

	log.WithFields(log.Fields{"storage": s}).Info("storage::storage::Open; done reading db file")

	s.logWriter = newLogRecordWriter(logFile)
	s.logFile, logFile = logFile, nil

	log.Info("storage::storage::Open; done")
	return nil
}

// Get returns the value associated with the given key in the storage.
func (s *Storage) Get(key []byte, opts *ReadOptions) ([]byte, error) {
	log.WithFields(log.Fields{
		"key": string(key),
	}).Info("storage::storage::Get")

	log.Info("storage::storage::Get; looking in memtable")

	seqNumber := s.lastSequenceNumber + 1

	if opts != nil {
		if opts.Snapshot != nil {
			seqNumber = opts.Snapshot.SeqNum
		}
	}

	ikey := newInternalKey(key, internalKeyKindSet, seqNumber)

	value, conclusive, err := s.memtable.Get(ikey)
	if err == nil {
		log.WithFields(log.Fields{"value": value}).Info("storage::storage::Get; found key in memtable")
		return value, nil
	}

	if !conclusive {
		log.Info("storage::storage::Get; memtable data unconclusive")
		// TODO: read from sst files.
	}

	return nil, err
}

// Set sets the value for a particular key in the storage layer.
func (s *Storage) Set(key, value []byte, opts *WriteOptions) error {
	log.WithFields(log.Fields{
		"key":   string(key),
		"value": string(value),
		"opts":  opts,
	}).Info("storage::storage::Set")

	var batch WriteBatch
	batch.Set(key, value)
	return s.apply(&batch, opts)
}

// Delete deletes the value for a particular key in the storage layer.
func (s *Storage) Delete(key []byte, opts *WriteOptions) error {
	log.WithFields(log.Fields{
		"key":  string(key),
		"opts": opts,
	}).Info("storage::storage::Delete")

	var batch WriteBatch
	batch.Delete(key)
	return s.apply(&batch, opts)
}

// BatchWrite writes a batch of Set/Delete entries to the storage atomically.
func (s *Storage) BatchWrite(wb *WriteBatch) error {
	log.Info("storage::storage::BatchWrite; started")
	return s.apply(wb, nil)
}

// Scan returns an iterator to iterate the key-value pairs whose key >= target
func (s *Storage) Scan(target []byte) Iterator {
	log.WithFields(log.Fields{"target": string(target)}).Info("storage::storage::Scan; started")
	seqNumber := s.lastSequenceNumber + 1
	ikey := newInternalKey(target, internalKeyKindSet, seqNumber)
	itr := s.memtable.Scan(ikey)
	return itr
}

// GetSnapshot creates a snapshot and returns it.
// It is thread safe and can be called concurrently.
func (s *Storage) GetSnapshot() *Snapshot {
	snap := &Snapshot{
		SeqNum: s.lastSequenceNumber,
	}
	s.appendSnapshot(snap)
	return snap
}

// GetLatestSeqForKey returns the latest seq number of the key
// This can be used to implement transaction support over the storage layer.
// returns the seq number if found in the db otherwise returns 0.
func (s *Storage) GetLatestSeqForKey(key []byte) uint64 {
	log.WithFields(log.Fields{
		"key": string(key),
	}).Info("storage::storage::GetLatestSeqForKey; started")

	log.Info("storage::storage::GetLatestSeqForKey; looking in memtable")

	seqNumber := s.lastSequenceNumber + 1
	ikey := newInternalKey(key, internalKeyKindSet, seqNumber)

	value, conclusive, err := s.memtable.GetLatestSeqForKey(ikey)
	if err == nil && value != 0 {
		log.WithFields(log.Fields{"value": value}).Info("storage::storage::GetLatestSeqForKey; found seq number for key in memtable")
		return value
	}

	if !conclusive {
		// TODO: read from sst files.
	}

	return 0
}

// Close closes the DB after flushing to disk.
func (s *Storage) Close() (err error) {
	log.Info("storage::storage::Close; started")

	if s.logWriter != nil {
		err = s.logWriter.flush()
		if err != nil {
			log.WithFields(log.Fields{"err": err.Error()}).Error("storage::storage::Close; error in flushing")
			return err
		}
	}

	if s.logFile != nil {
		err = s.logFile.Sync()
		if err != nil {
			log.WithFields(log.Fields{"err": err.Error()}).Error("storage::storage::Close; error in syncing log file")
			return err
		}

		err = s.logFile.Close()
		if err != nil {
			log.WithFields(log.Fields{"err": err.Error()}).Error("storage::storage::Close; error in closing log file")
			return err
		}
	}

	log.Info("storage::storage::Close; done")
	return nil
}

// append appends a snapshot to the storage.
func (s *Storage) appendSnapshot(snap *Snapshot) {
	log.Info("storage::storage::appendSnapshot; start")
	s.mu.Lock()
	defer s.mu.Unlock()
	snap.prev = s.snapshotDummy.prev
	snap.prev.next = snap
	snap.next = &s.snapshotDummy
	snap.next.prev = snap
	log.Info("storage::storage::appendSnapshot; done")
}

// apply applies a writeBatch atomically according to write options.
func (s *Storage) apply(wb *WriteBatch, opts *WriteOptions) error {
	log.Info("storage::storage::apply; started")

	if len(wb.data) == 0 {
		log.Info("storage::storage::apply; empty write batch.")
		return nil
	}

	cnt := wb.getCount()
	log.Info(fmt.Sprintf("storage::storage::apply; write batch of count %d.", cnt))

	s.mu.Lock()
	defer s.mu.Unlock()

	seqNum := s.lastSequenceNumber + 1
	wb.setSeqNum(seqNum)
	s.lastSequenceNumber += uint64(cnt)

	log.Info("storage::storage::apply; writing batch to log file")

	// write batch to log file
	w, err := s.logWriter.next()
	if err != nil {
		return err
	}
	if _, err = w.Write(wb.data); err != nil {
		return err
	}
	if opts != nil && opts.Sync {
		if err = s.logWriter.flush(); err != nil {
			return fmt.Errorf("storage: could not flush log entry: %v", err)
		}
		if err = s.logFile.Sync(); err != nil {
			return fmt.Errorf("storage: could not sync log entry: %v", err)
		}
	}

	log.Info("storage::storage::apply; writing batch to memtable")

	// write/update in memtable
	for itr := wb.getIterator(); ; seqNum++ {
		kind, ukey, value, ok := itr.next()
		if !ok {
			break
		}

		ikey := newInternalKey(ukey, kind, seqNum)
		s.memtable.Set(ikey, value)
	}

	if seqNum != s.lastSequenceNumber+1 {
		panic("storage: inconsistent batch count in write batch")
	}

	log.Info("storage::storage::apply; done")
	return nil
}

// createNewDB creates all the files necessary for creating a db in the given directory.
//
// It also populates the version set in the struct.
func (s *Storage) createNewDB() error {
	log.WithFields(log.Fields{"storage": s}).Info("storage::storage: createNewDB")

	// create the db file
	logFile, err := s.options.Fs.create(getDbFileName(s.dirname, s.dbName, dbFileType))
	if err != nil {
		return err
	}
	defer func() {
		if logFile != nil {
			logFile.Close()
		}
	}()

	s.logFile, logFile = logFile, nil
	log.Info("storage::storage::createNewDB; successfully created db.")
	return nil
}

// newStorage creates a new persistent storage according to the given parameters.
func newStorage(dirname, dbName string, ukComparator, ikComparator Comparator, options *Options) (*Storage, error) {
	log.WithFields(log.Fields{
		"dirname": dirname,
	}).Info("storage: newStorage")

	if options != nil {
		if options.Fs == nil {
			options.Fs = DefaultFileSystem
		}
		if options.Cachesz == 0 {
			options.Cachesz = defaultTableCacheSize
		}
	}

	strg := &Storage{
		dirname:      dirname,
		dbName:       dbName,
		options:      options,
		ukComparator: ukComparator,
		ikComparator: ikComparator,
	}

	strg.mu = new(sync.Mutex)

	skipList := NewSkipList(defaultSkipListHeight, ikComparator)
	memtable := NewMemtable(skipList, ikComparator)

	strg.memtable = memtable
	strg.options = options

	// init snapshot list.
	strg.snapshotDummy.prev = &strg.snapshotDummy
	strg.snapshotDummy.next = &strg.snapshotDummy

	return strg, nil
}

// NewStorageWithCustomComparator creates a new persistent storage in the given directory.
//
// The directory should already exist. The library won't create the directory.
// It obtains a lock on the passed in directory hence two processes can't access this directory simultaneously.
// Keys are ordered using the given custom comparator.
// returns a Storage interface implementation.
func NewStorageWithCustomComparator(dirname, dbName string, userKeyComparator Comparator, options *Options) (*Storage, error) {
	internalKeyComparator := newInternalKeyComparator(userKeyComparator)

	return newStorage(dirname, dbName, userKeyComparator, internalKeyComparator, options)
}

// NewStorage creates a new persistent storage in the given directory.
//
// The directory should already exist. The library won't create the directory.
// It obtains a lock on the passed in directory hence two processes can't access this directory simultaneously.
// returns a Storage interface implementation.
func NewStorage(dirname, dbName string, options *Options) (*Storage, error) {
	userKeyComparator := DefaultComparator
	return NewStorageWithCustomComparator(dirname, dbName, userKeyComparator, options)
}
