package mvcc

import (
	"fmt"
	"testing"

	"github.com/dr0pdb/icecanedb/pkg/storage"
	"github.com/dr0pdb/icecanedb/test"
	"github.com/stretchr/testify/assert"
)

var (
	mvcc *MVCC = newTestMVCC()
)

// create a snapshot with the given seq number. set seq to 0, for the default one.
func newTestSnapshot(storage *storage.Storage, seq uint64) *storage.Snapshot {
	snap := storage.GetSnapshot()
	if seq != 0 {
		snap.SeqNum = seq
	}
	return snap
}

func newTestTransaction(storage *storage.Storage, id, seq uint64) *Transaction {
	return newTransaction(id, mvcc, storage, newTestSnapshot(storage, seq), make([]*Transaction, 0))
}

func setupStorage() (*storage.Storage, error) {
	options := &storage.Options{
		CreateIfNotExist: true,
	}

	s, err := storage.NewStorage(test.TestDirectory, options)
	if err != nil {
		return nil, err
	}
	err = s.Open()
	return s, err
}

func addDataBeforeSnapshot(storage *storage.Storage) error {
	for i := range test.TestKeys {
		err := storage.Set(test.TestKeys[i], test.TestValues[i], nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func TestSingleGetSetDelete(t *testing.T) {
	test.CreateTestDirectory(test.TestDirectory)
	defer test.CleanupTestDirectory(test.TestDirectory)

	s, err := setupStorage()
	assert.Nil(t, err, "Unexpected error in creating new storage")

	addDataBeforeSnapshot(s)

	txn := newTestTransaction(s, 1, 0) // seq 0 means the current one.

	for i := range test.TestKeys {
		val, err := txn.Get(test.TestKeys[i], nil)
		assert.Nil(t, err)
		assert.Equal(t, test.TestValues[i], val, fmt.Sprintf("Unexpected value for key%d. Expected %v, found %v", i, test.TestValues[i], val))
	}

	for i := range test.TestKeys {
		err := txn.Set(test.TestKeys[i], test.TestUpdatedValues[i], nil)
		assert.Nil(t, err, fmt.Sprintf("Unexpected error in setting value for key%d.", i))
	}

	// should get new updated values.
	for i := range test.TestKeys {
		val, err := txn.Get(test.TestKeys[i], nil)
		assert.Nil(t, err)
		assert.Equal(t, test.TestUpdatedValues[i], val, fmt.Sprintf("Unexpected value for key%d. Expected %v, found %v", i, test.TestUpdatedValues[i], val))
	}

	for i := range test.TestKeys {
		err := txn.Delete(test.TestKeys[i], nil)
		assert.Nil(t, err, fmt.Sprintf("Unexpected error in deleting value for key%d.", i))
	}

	// should get a not found error
	for i := range test.TestKeys {
		val, err := txn.Get(test.TestKeys[i], nil)
		assert.NotNil(t, err, fmt.Sprintf("Unexpected value for key%d. Expected not found error, found %v", i, val))
	}
}
