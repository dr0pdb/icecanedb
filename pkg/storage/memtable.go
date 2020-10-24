package storage

import (
	"sync"

	log "github.com/sirupsen/logrus"
)

// Memtable is the in-memory store
// It is thread safe and can be accessed concurrently.
type Memtable interface {
	Get(key []byte) ([]byte, error)

	Set(key, value []byte) error

	Delete(key []byte) error
}

type memtable struct {
	mutex      sync.RWMutex
	skiplist   SkipList
	comparator Comparator
}

func (m *memtable) Get(key []byte) ([]byte, error) {
	log.WithFields(log.Fields{
		"key": key,
	}).Info("memtable: Get")

	panic("not implemented")
}

func (m *memtable) Set(key, value []byte) error {
	panic("not implemented")
}

func (m *memtable) Delete(key []byte) error {
	panic("not implemented")
}

// NewMemtable returns a new instance of the Memtable
func NewMemtable(skiplist SkipList, comparator Comparator) Memtable {
	return &memtable{
		skiplist:   skiplist,
		comparator: comparator,
	}
}
