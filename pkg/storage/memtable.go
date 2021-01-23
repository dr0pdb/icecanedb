package storage

import (
	"github.com/dr0pdb/icecanedb/internal/common"
	log "github.com/sirupsen/logrus"
)

// Memtable is the in-memory store
// It is thread safe and can be accessed concurrently.
type Memtable struct {
	skipList   *SkipList
	comparator Comparator
}

// Get finds an element by key.
//
// returns a byte slice pointing to the value if the key is found.
// returns NotFoundError if the key is not found.
func (m *Memtable) Get(ikey []byte) ([]byte, bool, error) {
	log.WithFields(log.Fields{
		"ikey": string(ikey),
	}).Info("memtable: Get")

	skipNode := m.skipList.Get(ikey)
	if skipNode == nil ||
		m.comparator.Compare(internalKey(skipNode.getKey()).userKey(), internalKey(ikey).userKey()) != 0 ||
		internalKey(skipNode.getKey()).kind() == internalKeyKindDelete {

		if skipNode != nil && m.comparator.Compare(internalKey(skipNode.getKey()).userKey(), internalKey(ikey).userKey()) == 0 {
			log.WithFields(log.Fields{
				"ikey": string(ikey),
			}).Info("memtable: Get; Key not found in the memtable. Found delete entry in the memtable.")

			return []byte{}, true, common.NewNotFoundError("Key not found.")
		}

		log.WithFields(log.Fields{
			"ikey": string(ikey),
		}).Info("memtable: Get; Key not found in the memtable.")
		return []byte{}, false, common.NewNotFoundError("Key not found.")
	}

	log.WithFields(log.Fields{
		"ikey":  string(ikey),
		"value": string(skipNode.getValue()),
	}).Info("memtable: Get; Key found in the memtable; returning value")

	return skipNode.getValue(), true, nil
}

// GetLatestSeqForKey finds an the latest seq number for a key.
//
// returns the latest seq number if the key is present in the memtable.
// returns NotFoundError if the key is not found.
func (m *Memtable) GetLatestSeqForKey(ikey []byte) (uint64, bool, error) {
	log.WithFields(log.Fields{
		"ikey": string(ikey),
	}).Info("memtable: GetLatestSeqForKey")

	skipNode := m.skipList.Get(ikey)
	if skipNode == nil ||
		m.comparator.Compare(internalKey(skipNode.getKey()).userKey(), internalKey(ikey).userKey()) != 0 {

		log.WithFields(log.Fields{
			"ikey": string(ikey),
		}).Info("memtable: GetLatestSeqForKey; Key not found in the memtable.")
		return 0, false, common.NewNotFoundError("Key not found.")
	}

	log.WithFields(log.Fields{
		"ikey":  string(ikey),
		"value": string(skipNode.getValue()),
	}).Info("memtable: Get; Key found in the memtable; returning seq number")

	return internalKey(skipNode.getKey()).sequenceNumber(), true, nil
}

// Set inserts a value in the memtable associated with the specified key.
//
// Overwrites the data if the key already exists.
// returns a error if something goes wrong
// error equal to nil represents success.
func (m *Memtable) Set(key, value []byte) error {
	log.WithFields(log.Fields{
		"key": string(key),
	}).Info("memtable: Set")

	skipNode := m.skipList.Set(key, value)
	if skipNode == nil {
		log.WithFields(log.Fields{
			"key": key,
		}).Info("memtable: Set; Key not found in the memtable.")
		return common.NewUnknownError("Key not found.")
	}

	log.WithFields(log.Fields{
		"key":   string(key),
		"value": string(skipNode.getValue()),
	}).Info("memtable: Set; Wrote the value in the memtable for the given key.")

	return nil
}

// NewMemtable returns a new instance of the memtable struct
func NewMemtable(skipList *SkipList, comparator Comparator) *Memtable {
	return &Memtable{
		skipList:   skipList,
		comparator: comparator,
	}
}
