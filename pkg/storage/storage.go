package storage

import (
	"fmt"
	"sync"

	"github.com/dr0pdb/icecanedb/internal/common"
	log "github.com/sirupsen/logrus"
)

const (
	defaultSkipListHeight = 18
)

// Storage is the persistent key-value storage struct
// It contains all the necessary information for the storage
type Storage struct {
	dirname string
	options Options

	mu sync.Mutex

	// memtable is the current memtable.
	//
	// immMemtable is the memtable that is being compacted right now.
	// it could be nil right now.
	memtable, immMemtable *memtable

	logNumber uint64
	logFile   file

	vs *versionSet

	ukComparator, ikComparator Comparator
}

// newStorage creates a new persistent storage according to the given parameters.
func newStorage(dirname string, ukComparator, ikComparator Comparator, options Options) (*Storage, error) {
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

	versions := newVersionSet(dirname, ukComparator, ikComparator, &strg.options)
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

	// TODO: Write the contents of the newly created manifest file.

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
			return s.createNewDB()
		}

		return err
	}

	return nil
}

// Get TODO
func (s *Storage) Get(key []byte) ([]byte, error) {
	log.WithFields(log.Fields{
		"key": key,
	}).Info("storage: Get")

	panic("not implemented")
}

// Set TODO
func (s *Storage) Set(key, value []byte) error {
	panic("not implemented")
}

// Delete TODO
func (s *Storage) Delete(key []byte) error {
	panic("not implemented")
}

// Close todo
func (s *Storage) Close() error {
	panic("not implemented")
}

// NewStorageWithCustomComparator creates a new persistent storage in the given directory.
//
// It obtains a lock on the passed in directory hence two processes can't access this directory simultaneously.
// Keys are ordered using the given custom comparator.
// returns a Storage interface implementation.
func NewStorageWithCustomComparator(dirname string, userKeyComparator Comparator, options Options) (*Storage, error) {
	internalKeyComparator := newInternalKeyComparator(userKeyComparator)

	return newStorage(dirname, userKeyComparator, internalKeyComparator, options)
}

// NewStorage creates a new persistent storage in the given directory.
//
// It obtains a lock on the passed in directory hence two processes can't access this directory simultaneously.
// returns a Storage interface implementation.
func NewStorage(dirname string, options Options) (*Storage, error) {
	userKeyComparator := DefaultComparator
	return NewStorageWithCustomComparator(dirname, userKeyComparator, options)
}
