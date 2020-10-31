package storage

import (
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

	// Set inserts a value in the memtable associated with the specified key.
	//
	// Overwrites the data if the key already exists.
	// returns a error if something goes wrong
	// error equal to nil represents success.
	Set(key, value []byte) error

	// Delete deletes a value in the memtable associated with the specified key.
	//
	// returns nil if the operation was successful,
	// returns NotFoundError if the key is not found.
	Delete(key []byte) error
}

type memtable struct {
	skipList   *skipList
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

	skipNode := m.skipList.Get(key)
	if skipNode == nil {
		log.WithFields(log.Fields{
			"key": key,
		}).Info("memtable: Get; Key not found in the memtable.")
		return []byte{}, common.NewNotFoundError("Key not found.")
	}

	log.WithFields(log.Fields{
		"key":   key,
		"value": skipNode.Value(),
	}).Info("memtable: Get; Key found in the memtable; returning value")

	return skipNode.Value(), nil
}

// Set inserts a value in the memtable associated with the specified key.
//
// Overwrites the data if the key already exists.
// returns a error if something goes wrong
// error equal to nil represents success.
func (m *memtable) Set(key, value []byte) error {
	log.WithFields(log.Fields{
		"key": key,
	}).Info("memtable: Set")

	skipNode := m.skipList.Set(key, value)
	if skipNode == nil {
		log.WithFields(log.Fields{
			"key": key,
		}).Info("memtable: Set; Key not found in the memtable.")
		return common.NewUnknownError("Key not found.")
	}

	log.WithFields(log.Fields{
		"key":   key,
		"value": skipNode.Value(),
	}).Info("memtable: Set; Wrote the value in the memtable for the given key.")

	return nil
}

// Delete deletes a value in the memtable associated with the specified key.
//
// returns nil if the operation was successful,
// returns NotFoundError if the key is not found.
func (m *memtable) Delete(key []byte) error {
	log.WithFields(log.Fields{
		"key": key,
	}).Info("memtable: Delete")

	skipNode := m.skipList.Delete(key)
	if skipNode == nil {
		log.WithFields(log.Fields{
			"key": key,
		}).Info("memtable: Delete; Key not found in the memtable.")
		return common.NewUnknownError("Key not found.")
	}

	log.WithFields(log.Fields{
		"key": key,
	}).Info("memtable: Delete; Deleted the value in the memtable for the given key.")

	return nil
}

// NewMemtable returns a new instance of the Memtable
func NewMemtable(skipList *skipList, comparator Comparator) Memtable {
	return &memtable{
		skipList:   skipList,
		comparator: comparator,
	}
}
