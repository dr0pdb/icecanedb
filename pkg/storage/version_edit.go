package storage

type deletedFileEntry struct {
	level   int
	fileNum uint64
}

type newFileEntry struct {
	level int
	meta  fileMetaData
}

// versionEdit stores the data indicating a version edit.
//
// It is used in various situations:
// 1. Creation of db.
// 2. Compaction of the db.
type versionEdit struct {
	// the name of the user key comparator used in the version.
	comparatorName string

	// the next file number available.
	nextFileNumber uint64

	// files deleted during this edit. Usually during compaction.
	deletedFiles []deletedFileEntry

	// newly added files during this edit. Usually during compaction or when one file is full.
	newFiles []newFileEntry
}

func (ve *versionEdit) encode() {}
