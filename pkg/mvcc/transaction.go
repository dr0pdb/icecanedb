package mvcc

import "github.com/dr0pdb/icecanedb/pkg/storage"

// Transaction is the MVCC transaction.
type Transaction struct {
	// unique transaction id
	id uint64

	// The underlying storage
	storage *storage.Storage

	// snapshot on which the transaction operates
	snapshot *storage.Snapshot

	// concurrent txns at the start. This txn should be invisible to these transactions.
	// The validation phase verifies that before commiting.
	concTxns []*Transaction
}

// newTransaction creates a new transaction.
func newTransaction(id uint64, storage *storage.Storage, snapshot *storage.Snapshot, concTxns []*Transaction) *Transaction {
	return &Transaction{
		id:       id,
		storage:  storage,
		snapshot: snapshot,
		concTxns: concTxns,
	}
}

// Commit commits the transaction.
func (t *Transaction) Commit() {
	panic("not implemented")
}

// Rollback rolls back the transaction
func (t *Transaction) Rollback() {
	panic("not implemented")
}
