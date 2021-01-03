package storage

import (
	"fmt"
	"sync"

	"github.com/dr0pdb/icecanedb/internal/common"
	log "github.com/sirupsen/logrus"
)

const (
	defaultSkipListHeight = 18
	defaultNumberLevels   = 12
)

// Storage is the persistent key-value storage struct
// It contains all the necessary information for the storage
type Storage struct {
	dirname string
	options *Options

	mu *sync.Mutex

	// memtable is the current memtable.
	//
	// immMemtable is the memtable that is being compacted right now.
	// it could be nil right now.
	memtable, immMemtable *memtable

	logNumber uint64
	logFile   file
	logWriter *logRecordWriter

	vs *versionSet

	ukComparator, ikComparator Comparator
}

// newStorage creates a new persistent storage according to the given parameters.
func newStorage(dirname string, ukComparator, ikComparator Comparator, options *Options) (*Storage, error) {
	log.WithFields(log.Fields{
		"dirname": dirname,
	}).Info("storage: newStorage")

	var logNumber uint64 = 1

	if options.Fs == nil {
		options.Fs = DefaultFileSystem
	}
	if options.Cachesz == 0 {
		options.Cachesz = defaultTableCacheSize
	}

	strg := &Storage{
		dirname:      dirname,
		options:      options,
		immMemtable:  nil,
		ukComparator: ukComparator,
		ikComparator: ikComparator,
		logNumber:    logNumber,
	}

	skipList := newSkipList(defaultSkipListHeight, ikComparator)
	memtable := newMemtable(skipList, ikComparator)

	strg.memtable = memtable
	strg.options = options

	versions := newVersionSet(dirname, ukComparator, ikComparator, strg.options)
	strg.vs = versions

	return strg, nil
}

// createNewDB creates all the files necessary for creating a db in the given directory.
//
// It also populates the version set in the struct.
func (s *Storage) createNewDB() (ret error) {
	log.WithFields(log.Fields{"storage": s}).Info("storage::storage: createNewDB")

	log.Info("storage::storage: createNewDB; creating manifest file")
	const mno = 1
	mfName := getDbFileName(s.dirname, manifestFileType, mno)
	mf, err := s.options.Fs.create(mfName)
	if err != nil {
		log.WithFields(log.Fields{"error": err.Error()}).Error("storage: createNewDB; failure in creating manifest file")
		return fmt.Errorf("") // TODO: return suitable error message.
	}
	defer func() {
		if ret != nil {
			log.Error("storage::storage: createNewDB; failure in creating db. Deleting created manifest")
			s.options.Fs.remove(mfName)
		}
	}()
	defer mf.Close()

	log.Info("storage::storage: createNewDB; adding contents in the manifest file..")
	ve := versionEdit{
		comparatorName: s.ukComparator.Name(),
		nextFileNumber: mno + 1,
	}
	lrw := newLogRecordWriter(mf)
	lrww, err := lrw.next()
	if err != nil {
		log.WithFields(log.Fields{"error": err.Error()}).Error("storage::storage: createNewDB; error in calling next on log record writer.")
		return err
	}
	err = ve.encode(lrww)
	if err != nil {
		log.WithFields(log.Fields{"error": err.Error()}).Error("storage::storage: createNewDB; error in encoding the vedit to log file.")
		return err
	}
	err = lrw.close()
	if err != nil {
		log.WithFields(log.Fields{"error": err.Error()}).Error("storage::storage: createNewDB; error in closing the log record writer.")
		return err
	}
	log.Info("storage::storage: createNewDB; done adding contents in the manifest file.")

	log.Info("storage::storage: createNewDB; setting current file..")
	err = setCurrentFile(s.dirname, s.options.Fs, mno)
	if err != nil {
		log.WithFields(log.Fields{"error": err.Error()}).Error("storage::storage: createNewDB; failure in setting current file")
		return err
	}

	log.Info("storage::storage: createNewDB; successfully created db.")
	return nil
}

// Open opens a storage.
//
// It loads the version set from disk.
// If CreateIfNotExist option is true: It creates the db inside the set directory if it doesn't exist already.
func (s *Storage) Open() error {
	log.WithFields(log.Fields{"storage": s}).Info("storage: Open")

	err := s.vs.load()
	if err != nil {
		log.WithFields(log.Fields{"storage": s, "err": err.Error()}).Error("storage: Open")

		// create db since CURRENT doesn't exist.
		if _, ok := err.(common.NotFoundError); ok && s.options.CreateIfNotExist {
			log.Info("storage: Open; db not found. creating it.")
			err = s.createNewDB()
			if err != nil {
				log.WithFields(log.Fields{"storage": s, "err": err.Error()}).Error("storage: Open; error while creating the db")
				return err
			}
			log.Info("storage: Open; db created. loading version set")
			err = s.vs.load()
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	log.Info("storage: Open; version set loaded.")

	// TODO: replay any log files that aren't in the manifest.
	var ve versionEdit
	ve.logNumber = s.vs.nextFileNum()

	logFile, err := s.options.Fs.create(getDbFileName(s.dirname, logFileType, ve.logNumber))
	if err != nil {
		return err
	}
	defer func() {
		if logFile != nil {
			logFile.Close()
		}
	}()

	s.logWriter = newLogRecordWriter(logFile)
	s.logFile, logFile = logFile, nil

	// TODO: apply version edit and write new manifest.

	// TODO: recovery and cleanup

	return nil
}

// Get TODO
func (s *Storage) Get(key []byte) ([]byte, error) {
	log.WithFields(log.Fields{
		"key": key,
	}).Info("storage::storage: Get")

	panic("not implemented")
}

// Set TODO
func (s *Storage) Set(key, value []byte, opts *WriteOptions) error {
	log.WithFields(log.Fields{
		"key":   key,
		"value": value,
		"opts":  opts,
	}).Info("storage::storage: Set")

	var batch writeBatch
	batch.set(key, value)
	return s.apply(batch, opts)
}

// Delete TODO
func (s *Storage) Delete(key []byte, opts *WriteOptions) error {
	log.WithFields(log.Fields{
		"key":  key,
		"opts": opts,
	}).Info("storage::storage: Delete")

	var batch writeBatch
	batch.delete(key)
	return s.apply(batch, opts)
}

// Close todo
func (s *Storage) Close() error {
	panic("not implemented")
}

// apply applies a writeBatch atomically according to write options.
func (s *Storage) apply(wb writeBatch, opts *WriteOptions) error {
	log.Info("storage::storage: apply; started")

	if len(wb.data) == 0 {
		log.Info("storage::storage: apply; empty write batch.")
		return nil
	}

	cnt := wb.getCount()

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.makeRoomForWrite(false); err != nil {
		log.Error(fmt.Errorf("storage::storage: apply; error in making room for write. err: %V", err))
		return err
	}

	seqNum := s.vs.lastSequenceNumber + 1
	wb.setSeqNum(seqNum)
	s.vs.lastSequenceNumber += uint64(cnt)

	// write batch to log file
	w, err := s.logWriter.next()
	if err != nil {
		return err
	}
	if _, err = w.Write(wb.data); err != nil {
		return err
	}
	if opts.Sync {
		if err = s.logWriter.flush(); err != nil {
			return fmt.Errorf("storage: could not flush log entry: %v", err)
		}
		if err = s.logFile.Sync(); err != nil {
			return fmt.Errorf("storage: could not sync log entry: %v", err)
		}
	}

	// write/update in memtable
	for itr := wb.getIterator(); ; seqNum++ {
		kind, ukey, value, ok := itr.next()
		if !ok {
			break
		}

		ikey := newInternalKey(ukey, kind, seqNum)
		s.memtable.set(ikey, value)
	}

	if seqNum != s.vs.lastSequenceNumber+1 {
		panic("storage: inconsistent batch count in write batch")
	}

	log.Info("storage::storage: apply; done")
	return nil
}

func (s *Storage) makeRoomForWrite(force bool) error {
	return nil
}

// NewStorageWithCustomComparator creates a new persistent storage in the given directory.
//
// The directory should already exist. The library won't create the directory.
// It obtains a lock on the passed in directory hence two processes can't access this directory simultaneously.
// Keys are ordered using the given custom comparator.
// returns a Storage interface implementation.
func NewStorageWithCustomComparator(dirname string, userKeyComparator Comparator, options *Options) (*Storage, error) {
	internalKeyComparator := newInternalKeyComparator(userKeyComparator)

	return newStorage(dirname, userKeyComparator, internalKeyComparator, options)
}

// NewStorage creates a new persistent storage in the given directory.
//
// The directory should already exist. The library won't create the directory.
// It obtains a lock on the passed in directory hence two processes can't access this directory simultaneously.
// returns a Storage interface implementation.
func NewStorage(dirname string, options *Options) (*Storage, error) {
	userKeyComparator := DefaultComparator
	return NewStorageWithCustomComparator(dirname, userKeyComparator, options)
}
