package storage

import (
	"bytes"
	"encoding/binary"
	"io"
)

const (
	tagComparatorName     = 1
	tagLogNumber          = 2
	tagPrevLogNumber      = 3
	tagLastSequenceNumber = 4
	tagNextFileNumber     = 5
	tagDeletedFile        = 6
	tagNewFile            = 7
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
	deletedFiles map[deletedFileEntry]bool

	// newly added files during this edit. Usually during compaction or when one file is full.
	newFiles []newFileEntry

	// lastSequenceNumber is the last sequence number that was used.
	lastSequenceNumber uint64

	logNumber, prevLogNumber uint64
}

// encode encodes the contents of a version edit to be written to a io.Writer.
func (ve *versionEdit) encode(lgw io.Writer) error {
	venc := versionEditEncoder{new(bytes.Buffer)}

	if ve.comparatorName != "" {
		venc.writeUvarint(tagComparatorName)
		venc.writeString(ve.comparatorName)
	}

	if ve.logNumber != 0 {
		venc.writeUvarint(tagLogNumber)
		venc.writeUvarint(ve.logNumber)
	}

	if ve.prevLogNumber != 0 {
		venc.writeUvarint(tagPrevLogNumber)
		venc.writeUvarint(ve.prevLogNumber)
	}

	if ve.lastSequenceNumber != 0 {
		venc.writeUvarint(tagLastSequenceNumber)
		venc.writeUvarint(ve.lastSequenceNumber)
	}

	if ve.nextFileNumber != 0 {
		venc.writeUvarint(tagNextFileNumber)
		venc.writeUvarint(ve.nextFileNumber)
	}

	for x := range ve.deletedFiles {
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

	lgw.Write(venc.Bytes())
	return nil
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

// encode encodes the contents of a version edit to be written to a io.Writer.
func (ve *versionEdit) decode(lgr io.Reader) error {
	panic("not implemented")
}

// versionEditBuilder accumulates a number of version edits into one.
type versionEditBuilder struct {
	// files deleted
	deletedFiles []deletedFileEntry

	// newly added files
	newFiles []newFileEntry
}
