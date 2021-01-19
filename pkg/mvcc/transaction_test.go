package mvcc

import (
	"testing"

	"github.com/dr0pdb/icecanedb/pkg/storage"
	"github.com/dr0pdb/icecanedb/test"
	"github.com/stretchr/testify/assert"
)

var (
	mvcc *MVCC = newTestMVCC()
)

func newTestSnapshot(storage *storage.Storage, seq uint64) *storage.Snapshot {
	snap := storage.GetSnapshot()
	snap.SeqNum = seq
	return snap
}

func newTestTransaction(storage *storage.Storage, id uint64) *Transaction {
	return newTransaction(id, mvcc, storage, newTestSnapshot(storage, 1), make([]*Transaction, 0))
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

func addDataBeforeSnapshot() {

}

func TestBasicGetSetDelete(t *testing.T) {
	test.CreateTestDirectory(test.TestDirectory)
	defer test.CleanupTestDirectory(test.TestDirectory)

	_, err := setupStorage()
	assert.Nil(t, err, "Unexpected error in creating new storage")
}

func TestConcurrentGetSetDelete(t *testing.T) {
	test.CreateTestDirectory(test.TestDirectory)
	defer test.CleanupTestDirectory(test.TestDirectory)
}
