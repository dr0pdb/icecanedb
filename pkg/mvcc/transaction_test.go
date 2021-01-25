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

func TestOperationOnAbortedTxn(t *testing.T) {
	test.CreateTestDirectory(test.TestDirectory)
	defer test.CleanupTestDirectory(test.TestDirectory)

	s, err := setupStorage()
	assert.Nil(t, err, "Unexpected error in creating new storage")

	addDataBeforeSnapshot(s)

	txn := newTestTransaction(s, 1, 0) // seq 0 means the current one.

	err = txn.Rollback()
	assert.Nil(t, err, fmt.Sprintf("Unexpected error in rolling back txn."))

	val, err := txn.Get(test.TestKeys[0], nil)
	assert.NotNil(t, err, fmt.Sprintf("Unexpected value for key%d. Expected error due to aborted txn, found %v", 0, val))
}

func TestValidateInvariant(t *testing.T) {
	test.CreateTestDirectory(test.TestDirectory)
	defer test.CleanupTestDirectory(test.TestDirectory)

	s, err := setupStorage()
	assert.Nil(t, err, "Unexpected error in creating new storage")

	addDataBeforeSnapshot(s)

	txn := newTestTransaction(s, 1, 0) // seq 0 means the current one.

	txn.sets[string(test.TestKeys[0])] = string(test.TestValues[0])
	txn.sets[string(test.TestKeys[1])] = string(test.TestValues[1])

	assert.True(t, txn.validateInvariants(), fmt.Sprintf("Validate invariant error; expected true, got false"))

	txn.deletes[string(test.TestKeys[1])] = true

	assert.False(t, txn.validateInvariants(), fmt.Sprintf("Validate invariant error; expected false, got true"))
}

func TestAbortedTxnNoSideEffect(t *testing.T) {
	test.CreateTestDirectory(test.TestDirectory)
	defer test.CleanupTestDirectory(test.TestDirectory)

	s, err := setupStorage()
	assert.Nil(t, err, "Unexpected error in creating new storage")

	addDataBeforeSnapshot(s)

	txn := newTestTransaction(s, 0, 0) // seq 0 means the current one.

	txn.Set(test.TestKeys[0], test.TestUpdatedValues[0], nil)
	err = txn.Rollback()
	assert.Nil(t, err)

	val, err := s.Get(test.TestKeys[0], nil)
	assert.Nil(t, err)
	assert.Equal(t, test.TestValues[0], val, fmt.Sprintf("Unexpected value for key%d. Expected %v, found %v", 0, test.TestValues[0], val))
}

func TestBasicMultipleTxn(t *testing.T) {
	test.CreateTestDirectory(test.TestDirectory)
	defer test.CleanupTestDirectory(test.TestDirectory)

	s, err := setupStorage()
	assert.Nil(t, err, "Unexpected error in creating new storage")

	addDataBeforeSnapshot(s)

	txn := newTestTransaction(s, 1, 0) // seq 0 means the current one.
	txn2 := newTestTransaction(s, 2, 0)

	// txn updates 0 and txn2 updates 1.
	txn.Set(test.TestKeys[0], test.TestUpdatedValues[0], nil)
	txn2.Set(test.TestKeys[1], test.TestUpdatedValues[1], nil)

	// txn shouldn't see the update on key1.
	val, err := txn.Get(test.TestKeys[1], nil)
	assert.Nil(t, err)
	assert.Equal(t, test.TestValues[1], val, fmt.Sprintf("Unexpected value for key%d. Expected %v, found %v", 1, test.TestValues[1], val))

	// txn2 shouldn't see the update on key0.
	val, err = txn2.Get(test.TestKeys[0], nil)
	assert.Nil(t, err)
	assert.Equal(t, test.TestValues[0], val, fmt.Sprintf("Unexpected value for key%d. Expected %v, found %v", 0, test.TestValues[0], val))

	err = txn.Commit()
	assert.Nil(t, err)

	// committing of txn shouldn't affect txn2
	val, err = txn2.Get(test.TestKeys[0], nil)
	assert.Nil(t, err)
	assert.Equal(t, test.TestValues[0], val, fmt.Sprintf("Unexpected value for key%d. Expected %v, found %v", 0, test.TestValues[0], val))

	err = txn2.Commit()
	assert.Nil(t, err)

	val, err = s.Get(test.TestKeys[0], nil)
	assert.Nil(t, err)
	assert.Equal(t, test.TestUpdatedValues[0], val, fmt.Sprintf("Unexpected value for key%d. Expected %v, found %v", 0, test.TestUpdatedValues[0], val))

	val, err = s.Get(test.TestKeys[1], nil)
	assert.Nil(t, err)
	assert.Equal(t, test.TestUpdatedValues[1], val, fmt.Sprintf("Unexpected value for key%d. Expected %v, found %v", 1, test.TestUpdatedValues[1], val))
}

func TestMultipleTxnConcurrent(t *testing.T) {
	test.CreateTestDirectory(test.TestDirectory)
	defer test.CleanupTestDirectory(test.TestDirectory)

	s, err := setupStorage()
	assert.Nil(t, err, "Unexpected error in creating new storage")

	addDataBeforeSnapshot(s)

	// todo: add test
}
