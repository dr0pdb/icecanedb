package storage

import (
	"sync"

	log "github.com/sirupsen/logrus"
)

const (
	defaultSkipListHeight = 18
)

// Storage is a Key-value store
type Storage interface {
	Get(key []byte) ([]byte, error)

	Set(key, value []byte) error

	Delete(key []byte) error

	Close() error
}

// storage is the persistent key-value storage struct
// It contains all the necessary information for the storage
type storage struct {
	dirname string
	options *Options

	tableCache *tableCache

	mu sync.Mutex

	// memtable is the current memtable.
	//
	// immMemtable is the memtable that is being compacted right now.
	// it could be nil right now.
	memtable, immMemtable *memtable

	logNumber uint64
	logFile   file

	internalKeyComparator Comparator
}

func (s *storage) Get(key []byte) ([]byte, error) {
	log.WithFields(log.Fields{
		"key": key,
	}).Info("storage: Get")

	panic("not implemented")
}

func (s *storage) Set(key, value []byte) error {
	panic("not implemented")
}

func (s *storage) Delete(key []byte) error {
	panic("not implemented")
}

func (s *storage) Close() error {
	panic("not implemented")
}

// newStorage creates a new persistent storage according to the given parameters.
func newStorage(dirname string, internalKeyComparator Comparator, options *Options) (Storage, error) {
	var logNumber uint64 = 1

	if options.fs == nil {
		options.fs = &DefaultFileSystem
	}
	if options.cachesz == 0 {
		options.cachesz = defaultTableCacheSize
	}
	tc := newTableCache(dirname, *options.fs, options.cachesz)

	strg := &storage{
		dirname:               dirname,
		options:               options,
		immMemtable:           nil,
		internalKeyComparator: internalKeyComparator,
		logNumber:             logNumber,
	}

	skipList := newSkipList(defaultSkipListHeight, internalKeyComparator)
	memtable := newMemtable(skipList, internalKeyComparator)

	strg.tableCache = tc
	strg.memtable = memtable

	return strg, nil
}

// NewStorageWithCustomComparator creates a new persistent storage in the given directory.
//
// It obtains a lock on the passed in directory hence two processes can't access this directory simultaneously.
// Keys are ordered using the given custom comparator.
// returns a Storage interface implementation.
func NewStorageWithCustomComparator(dirname string, userKeyComparator Comparator, options *Options) (Storage, error) {
	internalKeyComparator := newInternalKeyComparator(userKeyComparator)

	return newStorage(dirname, internalKeyComparator, options)
}

// NewStorage creates a new persistent storage in the given directory.
//
// It obtains a lock on the passed in directory hence two processes can't access this directory simultaneously.
// returns a Storage interface implementation.
func NewStorage(dirname string, options *Options) (Storage, error) {
	userKeyComparator := DefaultComparator
	return NewStorageWithCustomComparator(dirname, userKeyComparator, options)
}
