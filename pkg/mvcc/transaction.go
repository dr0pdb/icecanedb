package mvcc

import (
	"fmt"
	"sync"

	"github.com/dr0pdb/icecanedb/internal/common"
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

	// mu protects the sets and deletes data structures.
	mu *sync.RWMutex

	// sets and deletes are the set/delete operations done in this txn respectively.
	// invariant is that a key can only be present in one of the two.
	sets    map[string]string
	deletes map[string]bool

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
		mu:       new(sync.RWMutex),
		sets:     make(map[string]string),
		deletes:  make(map[string]bool),
		aborted:  false,
	}
}

// Commit commits the transaction.
func (t *Transaction) Commit() error {
	panic("not implemented")
}

// Rollback rolls back the transaction
func (t *Transaction) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()

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

	if t.aborted {
		return common.NewAbortedTransactionError(fmt.Sprintf("txn %d is already aborted", t.id))
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	skey := string(key)
	if _, found := t.deletes[skey]; found {
		delete(t.deletes, skey)
	}
	t.sets[skey] = string(value)

	log.Info("mvcc::transaction::Set; done.")
	return nil
}

// Get todo
// returns a byte slice pointing to the value if the key is found.
// returns NotFoundError if the key is not found.
func (t *Transaction) Get(key []byte, opts *ReadOptions) ([]byte, error) {
	log.WithFields(log.Fields{
		"key": string(key),
	}).Info("mvcc::transaction::Get; started.")

	if t.aborted {
		return nil, common.NewAbortedTransactionError(fmt.Sprintf("txn %d is already aborted", t.id))
	}

	skey := string(key)
	t.mu.RLock()
	if _, found := t.deletes[skey]; found {
		t.mu.RUnlock()
		return nil, common.NewNotFoundError(fmt.Sprintf("key %v not found", skey))
	}
	if value, found := t.sets[skey]; found {
		t.mu.RUnlock()
		return []byte(value), nil
	}
	t.mu.RUnlock() // todo: find a better way of doing this.

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

	t.mu.Lock()
	defer t.mu.Unlock()

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
