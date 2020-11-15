package storage

// fileMetaData stores the meta data about a file.
type fileMetaData struct {
	fileNum uint64

	size uint64

	smallest, largest internalKey
}

type version struct {
	prev, next *version
}
