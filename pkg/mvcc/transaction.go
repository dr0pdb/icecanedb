package mvcc

import (
	"fmt"

	"github.com/dr0pdb/icecanedb/internal/common"
	"github.com/dr0pdb/icecanedb/pkg/storage"
	log "github.com/sirupsen/logrus"
)

// Transaction is the MVCC transaction.
// It implements an optimistic concurrency control protocol.
// A single transaction is not thread safe.
// Operations on a single txn should be called sequentially.
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

	// sets and deletes are the set/delete operations done in this txn respectively.
	// invariant is that a key can only be present in one of the two.
	sets    map[string]string
	deletes map[string]bool

	// committed indicates txn commit status.
	commitInProgress, committed bool

	// aborted indicates txn rollback status.
	abortInProgress, aborted bool
}

// newTransaction creates a new transaction.
func newTransaction(id uint64, mvcc *MVCC, storage *storage.Storage, snapshot *storage.Snapshot, concTxns []*Transaction) *Transaction {
	return &Transaction{
		id:       id,
		mvcc:     mvcc,
		storage:  storage,
		snapshot: snapshot,
		concTxns: concTxns,
		sets:     make(map[string]string),
		deletes:  make(map[string]bool),
		aborted:  false,
	}
}

// Commit commits the transaction.
func (t *Transaction) Commit() error {
	log.WithFields(log.Fields{
		"id": t.id,
	}).Info("mvcc::transaction::Commit; started")

	var err error

	if t.aborted || t.abortInProgress {
		err = common.NewAbortedTransactionError(fmt.Sprintf("txn %d is already aborted or is in progress", t.id))
	}
	if t.committed || t.commitInProgress {
		err = common.NewCommittedTransactionError(fmt.Sprintf("txn %d is already committed or is in progress", t.id))
	}
	if !t.validateInvariants() {
		err = fmt.Errorf("Invalid txn %d. Invariant not satisfied", t.id) // todo: rethink on this. Maybe this should be another error type.
	}
	if err != nil {
		return err
	}

	t.commitInProgress = true
	defer func() {
		t.commitInProgress = false
	}()

	err = t.mvcc.commitTxn(t)
	return err
}

// Rollback rolls back the transaction
func (t *Transaction) Rollback() error {
	if t.aborted {
		return common.NewAbortedTransactionError(fmt.Sprintf("txn %d is already aborted", t.id))
	}
	if t.committed {
		return common.NewCommittedTransactionError(fmt.Sprintf("txn %d is already committed", t.id))
	}

	// Since everything is in memory in this optimistic concurrency control protocol, we can just mark this txn as aborted.
	t.aborted = true
	return nil
}

// Set overwrites the data if the key already exists.
// returns a error if something goes wrong
// error equal to nil represents success.
func (t *Transaction) Set(key, value []byte, opts *WriteOptions) error {
	log.WithFields(log.Fields{
		"key":   string(key),
		"value": string(value),
	}).Info("mvcc::transaction::Set; started.")

	// TODO: These two errors can be combined into InvalidTxnOperationError
	if t.aborted {
		return common.NewAbortedTransactionError(fmt.Sprintf("txn %d is already aborted", t.id))
	}
	if t.committed {
		return common.NewCommittedTransactionError(fmt.Sprintf("txn %d is already committed", t.id))
	}

	skey := string(key)
	if _, found := t.deletes[skey]; found {
		delete(t.deletes, skey)
	}
	t.sets[skey] = string(value)

	log.Info("mvcc::transaction::Set; done.")
	return nil
}

// Get returns the value associated with the given key.
// returns a byte slice pointing to the value if the key is found.
// returns NotFoundError if the key is not found.
func (t *Transaction) Get(key []byte, opts *ReadOptions) ([]byte, error) {
	log.WithFields(log.Fields{
		"key": string(key),
	}).Info("mvcc::transaction::Get; started.")

	if t.aborted {
		return nil, common.NewAbortedTransactionError(fmt.Sprintf("txn %d is already aborted", t.id))
	}
	if t.committed {
		return nil, common.NewCommittedTransactionError(fmt.Sprintf("txn %d is already committed", t.id))
	}

	skey := string(key)
	if _, found := t.deletes[skey]; found {
		return nil, common.NewNotFoundError(fmt.Sprintf("key %v not found", skey))
	}
	if value, found := t.sets[skey]; found {
		return []byte(value), nil
	}

	// This variable hasn't been modified in this txn, so read from the snapshot.
	sOpts := mapReadOptsToStorageReadOpts(opts, t.snapshot)
	return t.storage.Get(key, sOpts)
}

// Delete deletes the data if the key already exists.
// returns an error if something goes wrong
// returns a not found error if the key is already deleted.
// error equal to nil represents success.
func (t *Transaction) Delete(key []byte, opts *WriteOptions) error {
	log.WithFields(log.Fields{
		"key": string(key),
	}).Info("mvcc::transaction::Delete; started.")

	if t.aborted {
		return common.NewAbortedTransactionError(fmt.Sprintf("txn %d is already aborted", t.id))
	}
	if t.committed {
		return common.NewCommittedTransactionError(fmt.Sprintf("txn %d is already committed", t.id))
	}

	skey := string(key)
	if _, found := t.sets[skey]; found {
		delete(t.sets, skey)
	}
	if _, found := t.deletes[skey]; found {
		// when the key has already been deleted. return not found second time.
		return common.NewNotFoundError(fmt.Sprintf("key %v not found", skey))
	}
	t.deletes[skey] = true

	log.Info("mvcc::transaction::Delete; done.")
	return nil
}

// validateInvariants validates the sets/deletes invariant.
// assumes that the lock on txn is already held.
func (t *Transaction) validateInvariants() bool {
	for key := range t.deletes {
		if _, found := t.sets[key]; found {
			return false
		}
	}

	for key := range t.sets {
		if _, found := t.deletes[key]; found {
			return false
		}
	}

	return true
}
