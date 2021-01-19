package storage

import (
	"github.com/dr0pdb/icecanedb/internal/common"
	log "github.com/sirupsen/logrus"
)

// memtable is the in-memory store
// It is thread safe and can be accessed concurrently.
type memtable struct {
	skipList   *SkipList
	comparator Comparator
}

// get finds an element by key.
//
// returns a byte slice pointing to the value if the key is found.
// returns NotFoundError if the key is not found.
func (m *memtable) get(ikey []byte) ([]byte, bool, error) {
	log.WithFields(log.Fields{
		"ikey": string(ikey),
	}).Info("memtable: get")

	skipNode := m.skipList.Get(ikey)
	if skipNode == nil ||
		m.comparator.Compare(internalKey(skipNode.getKey()).userKey(), internalKey(ikey).userKey()) != 0 ||
		internalKey(skipNode.getKey()).kind() == internalKeyKindDelete {

		if skipNode != nil && m.comparator.Compare(internalKey(skipNode.getKey()).userKey(), internalKey(ikey).userKey()) == 0 {
			log.WithFields(log.Fields{
				"ikey": string(ikey),
			}).Info("memtable: get; Key not found in the memtable. Found delete entry in the memtable.")

			return []byte{}, true, common.NewNotFoundError("Key not found.")
		}

		log.WithFields(log.Fields{
			"ikey": string(ikey),
		}).Info("memtable: get; Key not found in the memtable.")
		return []byte{}, false, common.NewNotFoundError("Key not found.")
	}

	log.WithFields(log.Fields{
		"ikey":  string(ikey),
		"value": string(skipNode.getValue()),
	}).Info("memtable: get; Key found in the memtable; returning value")

	return skipNode.getValue(), true, nil
}

// set inserts a value in the memtable associated with the specified key.
//
// Overwrites the data if the key already exists.
// returns a error if something goes wrong
// error equal to nil represents success.
func (m *memtable) set(key, value []byte) error {
	log.WithFields(log.Fields{
		"key": string(key),
	}).Info("memtable: set")

	skipNode := m.skipList.Set(key, value)
	if skipNode == nil {
		log.WithFields(log.Fields{
			"key": key,
		}).Info("memtable: set; Key not found in the memtable.")
		return common.NewUnknownError("Key not found.")
	}

	log.WithFields(log.Fields{
		"key":   string(key),
		"value": string(skipNode.getValue()),
	}).Info("memtable: set; Wrote the value in the memtable for the given key.")

	return nil
}

// newMemtable returns a new instance of the memtable struct
func newMemtable(skipList *SkipList, comparator Comparator) *memtable {
	return &memtable{
		skipList:   skipList,
		comparator: comparator,
	}
}
