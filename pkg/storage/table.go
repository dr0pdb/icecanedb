package storage

// table is the actual SST that is saved on the file system.
type table struct {
	fileNum uint64

	// res is the channel which contains reader responsible for reading the contents of the table from disk
	res chan tableReaderOrError

	// the next and prev pointers to the next table entry.
	// only required for caching. They are not encoded on the disk.
	next, prev *table

	// reference count, safely released from the table cache if refcount == 0
	refCount uint32
}

// tableReaderOrError contains a tableReader for reading the contents of the table.
// In case of an error in reading the contents of a table, the error field is populated with the error.
type tableReaderOrError struct {
	reader *tableReader
	err    error
}

// blockHandle is the handle for a block in the table.
type blockHandle struct {
}
