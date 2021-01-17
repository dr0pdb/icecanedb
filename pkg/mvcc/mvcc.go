package mvcc

import (
	"sync"

	"github.com/dr0pdb/icecanedb/pkg/storage"
	log "github.com/sirupsen/logrus"
)

// MVCC is the Multi Version Concurrency Control layer for transactions
type MVCC struct {
	// underlying storage engine
	storage *storage.Storage

	mu *sync.RWMutex

	// active transactions at the present moment.
	activeTxn map[uint64]*Transaction

	// the next available txn ID.
	// TODO: persist this when we move to on disk storage.
	nextTxnID uint64
}

// NewMVCC creates a new MVCC transactional layer for the storage
func NewMVCC(storage *storage.Storage) *MVCC {
	return &MVCC{
		storage:   storage,
		mu:        new(sync.RWMutex),
		activeTxn: make(map[uint64]*Transaction),
		nextTxnID: 1,
	}
}

// Begin begins a new Transaction
func (m *MVCC) Begin() *Transaction {
	log.WithFields(log.Fields{
		"nextTxnID": m.nextTxnID,
	}).Info("mvcc::mvcc::Begin; started")

	snap := m.storage.GetSnapshot()
	m.mu.Lock()
	defer m.mu.Unlock()

	txn := newTransaction(m.nextTxnID, m, m.storage, snap, mapToSliceTx(m.activeTxn))
	m.activeTxn[m.nextTxnID] = txn
	m.nextTxnID++

	log.Info("mvcc::mvcc::Begin; done")
	return txn
}
