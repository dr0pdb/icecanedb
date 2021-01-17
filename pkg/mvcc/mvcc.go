package mvcc

import (
	"sync"

	"github.com/dr0pdb/icecanedb/pkg/storage"
)

// MVCC is the Multi Version Concurrency Control layer for transactions
type MVCC struct {
	// underlying storage engine
	storage *storage.Storage

	mu *sync.RWMutex

	// active transactions at the present moment.
	activeTxn []*Transaction
}

// NewMVCC creates a new MVCC transactional layer for the storage
func NewMVCC(storage *storage.Storage) *MVCC {
	return &MVCC{
		storage: storage,
	}
}

// Begin begins a new Transaction
func (m *MVCC) Begin() *Transaction {
	panic("not implemented")
}
