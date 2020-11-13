package storage

import (
	"sync"

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

// newStorage creates a new persistent storage according to the given parameters.
func newStorage(dirname string, ukComparator, ikComparator Comparator, options Options) (*Storage, error) {
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

	// assign and load the version set.
	strg.vs = versions
	strg.vs.load()

	return strg, nil
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

// OpenStorageWithCustomComparator opens an existing persistent storage in the given directory.
//
// Keys are ordered using the given custom comparator. Note that this should be the same comparator which was used to create the db.
// Returns an error if the storage doesn't exist.
func OpenStorageWithCustomComparator(dirname string, userKeyComparator Comparator, options Options) (*Storage, error) {
	panic("Not Implemented")
}

// OpenStorage opens an existing persistent storage in the given directory.
//
// Returns an error if the storage doesn't exist.
func OpenStorage(dirname string, options Options) (*Storage, error) {
	panic("Not Implemented")
}
