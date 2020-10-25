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
	dirname    string
	memtable   Memtable
	comparator Comparator
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

// NewStorage creates a new persistent storage
func NewStorage(dirname string, memtable Memtable, userKeyComparator Comparator) Storage {
	return &storage{
		dirname:    dirname,
		memtable:   memtable,
		comparator: userKeyComparator,
	}
}
