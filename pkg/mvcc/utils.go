package mvcc

import "github.com/dr0pdb/icecanedb/pkg/storage"

func mapToSliceTx(m map[uint64]*Transaction) []*Transaction {
	slc := make([]*Transaction, 0, len(m))

	for _, v := range m {
		slc = append(slc, v)
	}

	return slc
}

func mapWriteOptsToStorageWriteOpts(opts *WriteOptions) *storage.WriteOptions {
	return &storage.WriteOptions{}
}

func mapReadOptsToStorageReadOpts(opts *ReadOptions, snap *storage.Snapshot) *storage.ReadOptions {
	return &storage.ReadOptions{
		Snapshot: snap,
	}
}
