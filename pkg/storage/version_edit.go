package storage

import (
	"bytes"
	"encoding/binary"
)

const (
	tagComparatorName = 1
	tagNextFileNumber = 2
	tagDeletedFile    = 3
	tagNewFile        = 4
)

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

// encode encodes the contents of a version edit to be written to a io.Writer.
func (ve *versionEdit) encode() {
	venc := versionEditEncoder{new(bytes.Buffer)}

	if ve.comparatorName != "" {
		venc.writeUvarint(tagComparatorName)
		venc.writeString(ve.comparatorName)
	}

	if ve.nextFileNumber != 0 {
		venc.writeUvarint(tagNextFileNumber)
		venc.writeUvarint(ve.nextFileNumber)
	}

	for _, x := range ve.deletedFiles {
		venc.writeUvarint(tagDeletedFile)
		venc.writeUvarint(uint64(x.level))
		venc.writeUvarint(x.fileNum)
	}

	for _, x := range ve.newFiles {
		venc.writeUvarint(tagNewFile)
		venc.writeUvarint(uint64(x.level))
		venc.writeUvarint(x.meta.fileNum)
		venc.writeUvarint(x.meta.size)
		venc.writeBytes(x.meta.smallest)
		venc.writeBytes(x.meta.largest)
	}
}

// versionEditEncoder is a struct containing the encoded data.
// Provides utility methods on it to encode various data types
type versionEditEncoder struct {
	*bytes.Buffer
}

func (vee versionEditEncoder) writeBytes(b []byte) {
	vee.writeUvarint(uint64(len(b)))
	vee.Write(b)
}

func (vee versionEditEncoder) writeUvarint(u uint64) {
	var buffer [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buffer[:], u)
	vee.Write(buffer[:n])
}

func (vee versionEditEncoder) writeString(s string) {
	vee.writeUvarint(uint64(len(s)))
	vee.WriteString(s)
}
