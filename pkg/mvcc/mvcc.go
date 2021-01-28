package mvcc

import (
	"fmt"
	"sync"

	"github.com/dr0pdb/icecanedb/internal/common"
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

func (m *MVCC) commitTxn(t *Transaction) (err error) {
	log.Info("mvcc::mvcc::commitTxn; started")

	m.mu.Lock()
	defer m.mu.Unlock()

	log.WithFields(log.Fields{"id": t.id}).Info("mvcc::mvcc::commitTxn; validation starting")

	// validation phase
	for key := range t.deletes {
		if t.snapshot.SeqNumber() < t.storage.GetLatestSeqForKey([]byte(key)) {
			err = common.NewTransactionCommitError(fmt.Sprintf("error while committing txn %d", t.id))
			break
		}
	}
	for key := range t.sets {
		if t.snapshot.SeqNumber() < t.storage.GetLatestSeqForKey([]byte(key)) {
			err = common.NewTransactionCommitError(fmt.Sprintf("error while committing txn %d", t.id))
			break
		}
	}
	if err != nil {
		t.aborted = true // no way this can succeed, so the txn can be aborted.
		log.WithFields(log.Fields{
			"id": t.id,
		}).Error("mvcc::mvcc::commitTxn; commit failed; aborting txn due to conflicting txn")
		return err
	}

	log.WithFields(log.Fields{"id": t.id}).Info("mvcc::mvcc::commitTxn; validation done")

	// write to storage layer atomically.
	var batch storage.WriteBatch
	for val := range t.deletes {
		batch.Delete([]byte(val))
	}
	for key, value := range t.sets {
		batch.Set([]byte(key), []byte(value))
	}
	err = t.storage.BatchWrite(&batch)
	if err == nil {
		t.committed = true
		log.WithFields(log.Fields{
			"id": t.id,
		}).Info("mvcc::mvcc::commitTxn; commit success")
	} else {
		log.WithFields(log.Fields{
			"id": t.id,
		}).Error("mvcc::mvcc::commitTxn; commit failed")
	}

	return err
}
