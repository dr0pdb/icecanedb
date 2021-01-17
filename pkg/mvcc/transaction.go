package mvcc

import (
	"github.com/dr0pdb/icecanedb/pkg/storage"
	log "github.com/sirupsen/logrus"
)

// Transaction is the MVCC transaction.
// It implements an optimistic concurrency control protocol.
type Transaction struct {
	// unique transaction id
	id uint64

	// mvcc is the MVCC struct that this txn was created from.
	// required to inform it once the txn is committed/aborted.
	mvcc *MVCC

	// The underlying storage
	storage *storage.Storage

	// snapshot on which the transaction operates
	snapshot *storage.Snapshot

	// concurrent txns at the start. This txn should be invisible to these transactions.
	// The validation phase verifies that before commiting.
	concTxns []*Transaction

	// changes contains all the updates that this transaction has made.
	changes *storage.SkipList

	// aborted indicates if the transaction has been aborted or not.
	aborted bool
}

// newTransaction creates a new transaction.
func newTransaction(id uint64, mvcc *MVCC, storage *storage.Storage, snapshot *storage.Snapshot, concTxns []*Transaction) *Transaction {
	return &Transaction{
		id:       id,
		mvcc:     mvcc,
		storage:  storage,
		snapshot: snapshot,
		concTxns: concTxns,
	}
}

// Commit commits the transaction.
func (t *Transaction) Commit() error {
	panic("not implemented")
}

// Rollback rolls back the transaction
func (t *Transaction) Rollback() error {
	t.aborted = true
	return nil
}

// Set todo
func (t *Transaction) Set(key, value []byte, opts *WriteOptions) error {
	log.WithFields(log.Fields{
		"key":   string(key),
		"value": string(value),
	}).Info("mvcc::transaction::Set; started.")

	// todo: decide a mvcc key first.
	t.changes.Set(key, value)

	return nil
}

// Get todo
func (t *Transaction) Get(key []byte, opts *ReadOptions) ([]byte, error) {
	log.WithFields(log.Fields{
		"key": string(key),
	}).Info("mvcc::transaction::Get; started.")

	// TODO: read from t.changes

	sOpts := mapReadOptsToStorageReadOpts(opts, t.snapshot)
	return t.storage.Get(key, sOpts)
}

// Delete todo
func (t *Transaction) Delete(key []byte, opts *WriteOptions) error {
	log.WithFields(log.Fields{
		"key": string(key),
	}).Info("mvcc::transaction::Delete; started.")

	// We need to store the marker in the skiplist.
	// Also ensure that it works for scenarios where the user deletes a key and then sets a new value for it.

	return nil
}
