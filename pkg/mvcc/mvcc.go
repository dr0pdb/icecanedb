package mvcc

import "github.com/dr0pdb/icecanedb/pkg/storage"

// MVCC is the Multi Version Concurrency Control for transactions
type MVCC struct {
	// underlying storage engine
	storage *storage.Storage
}
