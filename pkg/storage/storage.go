package storage

import (
	log "github.com/sirupsen/logrus"
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
	dirname               string
	memtable              Memtable
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
func newStorage(dirname string, memtable Memtable, internalKeyComparator Comparator) Storage {
	return &storage{
		dirname:               dirname,
		memtable:              memtable,
		internalKeyComparator: internalKeyComparator,
	}
}

// NewStorageWithCustomComparator creates a new persistent storage in the given directory.
//
// It obtains a lock on the passed in directory hence two processes can't access this directory simultaneously.
// Keys are ordered using the given custom comparator.
// returns a Storage interface implementation.
func NewStorageWithCustomComparator(dirname string, userKeyComparator Comparator) Storage {
	internalKeyComparator := newInternalKeyComparator(userKeyComparator)
	skipList := NewSkipList(18, internalKeyComparator)
	memtable := NewMemtable(skipList, internalKeyComparator)
	return newStorage(dirname, memtable, internalKeyComparator)
}

// NewStorage creates a new persistent storage in the given directory.
//
// It obtains a lock on the passed in directory hence two processes can't access this directory simultaneously.
// returns a Storage interface implementation.
func NewStorage(dirname string) Storage {
	userKeyComparator := DefaultComparator
	return NewStorageWithCustomComparator(dirname, userKeyComparator)
}
