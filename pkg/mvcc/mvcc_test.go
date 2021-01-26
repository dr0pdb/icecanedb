package mvcc

import (
	"sync"

	"github.com/dr0pdb/icecanedb/pkg/storage"
)

func newTestMVCC(storage *storage.Storage) *MVCC {
	return &MVCC{
		storage: storage,
		mu:      new(sync.RWMutex),
	}
}
