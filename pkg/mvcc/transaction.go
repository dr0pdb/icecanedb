package mvcc

import (
	pb "github.com/dr0pdb/icecanedb/pkg/protogen"
	"github.com/dr0pdb/icecanedb/pkg/raft"
)

// Transaction is the MVCC transaction.
// A single transaction is not thread safe.
// Operations on a single txn should be called sequentially.
type Transaction struct {
	// unique transaction id
	id uint64

	// concurrent txns contains the id of all the active txns at the start of the txn.
	// This txn should be invisible to these transactions.
	// The validation phase verifies that before commiting.
	concTxns []uint64

	// txnMode
	mode pb.TxnMode

	// the underlying raft node.
	rs *raft.Server
}

// TODO: Get, Set, Delete, Commit and Rollback calls

// newTransaction creates a new transaction.
func newTransaction(id uint64, concTxns []uint64, mode pb.TxnMode, rs *raft.Server) *Transaction {
	return &Transaction{
		id:       id,
		concTxns: concTxns,
		mode:     mode,
		rs:       rs,
	}
}
