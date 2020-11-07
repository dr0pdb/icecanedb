package storage

// table is the actual SST that is saved on the file system.
type table struct {
	fileNum uint64

	// the next and prev pointers to the next table entry.
	// only required for caching. They are not encoded on the disk.
	next, prev *table

	// reference count, safely released from the table cache if refcount == 0
	refCount uint32
}

// blockHandle is the handle for a block in the table.
type blockHandle struct {
}
