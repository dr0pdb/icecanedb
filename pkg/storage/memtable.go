package storage

import (
	"sync"

	"github.com/dr0pdb/icecanedb/internal/common"
	log "github.com/sirupsen/logrus"
)

// Memtable is the in-memory store
// It is thread safe and can be accessed concurrently.
type Memtable interface {
	// Get finds an element by key.
	//
	// returns a byte slice pointing to the value if the key is found.
	// returns NotFoundError if the key is not found.
	Get(key []byte) ([]byte, error)

	Set(key, value []byte) error

	Delete(key []byte) error
}

type memtable struct {
	mutex      sync.RWMutex
	skipList   SkipList
	comparator Comparator
}

// Get finds an element by key.
//
// returns a byte slice pointing to the value if the key is found.
// returns NotFoundError if the key is not found.
func (m *memtable) Get(key []byte) ([]byte, error) {
	log.WithFields(log.Fields{
		"key": key,
	}).Info("memtable: Get")

	m.mutex.RLock()
	defer m.mutex.RUnlock()

	skipNode := m.skipList.Get(key)
	if skipNode == nil {
		return []byte{}, common.NewNotFoundError("Key not found.")
	}

	return skipNode.Value(), nil
}

func (m *memtable) Set(key, value []byte) error {
	panic("not implemented")
}

func (m *memtable) Delete(key []byte) error {
	panic("not implemented")
}

// NewMemtable returns a new instance of the Memtable
func NewMemtable(skipList SkipList, comparator Comparator) Memtable {
	return &memtable{
		skipList:   skipList,
		comparator: comparator,
	}
}
