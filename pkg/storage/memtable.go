package storage

import (
	"github.com/dr0pdb/icecanedb/internal/common"
	log "github.com/sirupsen/logrus"
)

// memtable is the in-memory store
// It is thread safe and can be accessed concurrently.
type memtable struct {
	skipList   *skipList
	comparator Comparator
}

// get finds an element by key.
//
// returns a byte slice pointing to the value if the key is found.
// returns NotFoundError if the key is not found.
func (m *memtable) get(key []byte) ([]byte, error) {
	log.WithFields(log.Fields{
		"key": key,
	}).Info("memtable: get")

	skipNode := m.skipList.get(key)
	if skipNode == nil {
		log.WithFields(log.Fields{
			"key": key,
		}).Info("memtable: get; Key not found in the memtable.")
		return []byte{}, common.NewNotFoundError("Key not found.")
	}

	log.WithFields(log.Fields{
		"key":   key,
		"value": skipNode.getValue(),
	}).Info("memtable: get; Key found in the memtable; returning value")

	return skipNode.getValue(), nil
}

// set inserts a value in the memtable associated with the specified key.
//
// Overwrites the data if the key already exists.
// returns a error if something goes wrong
// error equal to nil represents success.
func (m *memtable) set(key, value []byte) error {
	log.WithFields(log.Fields{
		"key": key,
	}).Info("memtable: set")

	skipNode := m.skipList.set(key, value)
	if skipNode == nil {
		log.WithFields(log.Fields{
			"key": key,
		}).Info("memtable: set; Key not found in the memtable.")
		return common.NewUnknownError("Key not found.")
	}

	log.WithFields(log.Fields{
		"key":   key,
		"value": skipNode.getValue(),
	}).Info("memtable: set; Wrote the value in the memtable for the given key.")

	return nil
}

// delete deletes a value in the memtable associated with the specified key.
//
// returns nil if the operation was successful,
// returns NotFoundError if the key is not found.
func (m *memtable) delete(key []byte) error {
	log.WithFields(log.Fields{
		"key": key,
	}).Info("memtable: delete")

	skipNode := m.skipList.delete(key)
	if skipNode == nil {
		log.WithFields(log.Fields{
			"key": key,
		}).Info("memtable: delete; Key not found in the memtable.")
		return common.NewUnknownError("Key not found.")
	}

	log.WithFields(log.Fields{
		"key": key,
	}).Info("memtable: delete; Deleted the value in the memtable for the given key.")

	return nil
}

// newMemtable returns a new instance of the memtable struct
func newMemtable(skipList *skipList, comparator Comparator) *memtable {
	return &memtable{
		skipList:   skipList,
		comparator: comparator,
	}
}
