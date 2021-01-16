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
}

// newTransaction creates a new transaction.
func newTransaction() *Transaction {
	return &Transaction{}
}
