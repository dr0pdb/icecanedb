package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"

	log "github.com/sirupsen/logrus"
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

var (
	corruptManifestError = errors.New("icecanedb: Manifest entry is corrupted. Fatal!")
)

// This is required for converting a io.Reader to io.ByteReader.
// Example taken from https://tip.golang.org/src/image/gif/reader.go?#L224
type byteReader interface {
	io.Reader
	io.ByteReader
}

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
	log.Info("storage::version_edit: encode; started")
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
	log.Info("storage::version_edit: encode; done")
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

// decode decodes the contents of a io.Reader into the version edit.
func (ve *versionEdit) decode(lgr io.Reader) error {
	log.Info("storage::version_edit: decode; started")
	br, ok := lgr.(byteReader)
	if !ok {
		br = bufio.NewReader(lgr)
	}
	d := versionEditDecoder{br}
	for {
		tag, err := binary.ReadUvarint(br)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		switch tag {
		case tagComparatorName:
			name, err := d.readBytes()
			if err != nil {
				return err
			}
			ve.comparatorName = string(name)

		case tagLogNumber:
			ln, err := d.readUvarint()
			if err != nil {
				return err
			}
			ve.logNumber = ln

		case tagPrevLogNumber:
			pln, err := d.readUvarint()
			if err != nil {
				return err
			}
			ve.prevLogNumber = pln

		case tagLastSequenceNumber:
			lsn, err := d.readUvarint()
			if err != nil {
				return err
			}
			ve.lastSequenceNumber = lsn

		case tagNextFileNumber:
			nfn, err := d.readUvarint()
			if err != nil {
				return err
			}
			ve.nextFileNumber = nfn

		case tagDeletedFile:
			level, err := d.readLevel()
			if err != nil {
				return err
			}
			fn, err := d.readUvarint()
			if err != nil {
				return err
			}
			if ve.deletedFiles == nil {
				ve.deletedFiles = make(map[deletedFileEntry]bool)
			}
			ve.deletedFiles[deletedFileEntry{level, fn}] = true

		case tagNewFile:
			level, err := d.readLevel()
			if err != nil {
				return err
			}
			fn, err := d.readUvarint()
			if err != nil {
				return err
			}
			sz, err := d.readUvarint()
			if err != nil {
				return err
			}
			smallest, err := d.readBytes()
			if err != nil {
				return err
			}
			largest, err := d.readBytes()
			if err != nil {
				return err
			}
			ve.newFiles = append(ve.newFiles, newFileEntry{
				level: level,
				meta: fileMetaData{
					fileNum:  fn,
					size:     sz,
					smallest: smallest,
					largest:  largest,
				},
			})

		default:
			log.Error("storage::version_edit: decode; corrupt manifest!!")
			return corruptManifestError
		}
	}

	log.Info("storage::version_edit: decode; done")
	return nil
}

// versionEditDecoder
type versionEditDecoder struct {
	byteReader
}

func (d versionEditDecoder) readBytes() ([]byte, error) {
	log.Info("storage::version_edit: readBytes; started")
	n, err := d.readUvarint()
	if err != nil {
		return nil, err
	}
	s := make([]byte, n)
	_, err = io.ReadFull(d, s)
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			return nil, corruptManifestError
		}
		return nil, err
	}
	log.Info("storage::version_edit: readBytes; done")
	return s, nil
}

func (d versionEditDecoder) readLevel() (int, error) {
	log.Info("storage::version_edit: readLevel; started")
	u, err := d.readUvarint()
	if err != nil {
		return 0, err
	}
	if u >= defaultNumberLevels {
		return 0, corruptManifestError
	}
	log.Info("storage::version_edit: readLevel; started")
	return int(u), nil
}

func (d versionEditDecoder) readUvarint() (uint64, error) {
	log.Info("storage::version_edit: readUvarint; started")
	u, err := binary.ReadUvarint(d)
	if err != nil {
		if err == io.EOF {
			return 0, corruptManifestError
		}
		return 0, err
	}
	log.Info("storage::version_edit: readUvarint; done")
	return u, nil
}

// versionEditBuilder accumulates a number of version edits into one.
type versionEditBuilder struct {
	// files (file number) deleted per level
	deletedFiles [defaultNumberLevels]map[uint64]bool

	// newly added files (fileMetaData) per level
	newFiles [defaultNumberLevels][]fileMetaData
}

func (veb *versionEditBuilder) append(ve *versionEdit) {
	log.Info("storage::version_edit: append; started")
	for dfe := range ve.deletedFiles {
		if veb.deletedFiles[dfe.level] == nil {
			veb.deletedFiles[dfe.level] = make(map[uint64]bool)
		}
		veb.deletedFiles[dfe.level][dfe.fileNum] = true
	}

	log.Info("storage::version_edit: append; deleted file entries added to builder")

	for _, nfe := range ve.newFiles {
		// if this file is also in deleted set, remove it from that.
		if veb.deletedFiles[nfe.level] != nil {
			delete(veb.deletedFiles[nfe.level], nfe.meta.fileNum)
		}

		veb.newFiles[nfe.level] = append(veb.newFiles[nfe.level], nfe.meta)
	}
	log.Info("storage::version_edit: append; done")
}

// apply applies the versionEditBuilder data to a base version to produce a new version.
// in our incomplete implementation bv would always be nil, but keeping the level db signature to allow future enhancements if needed.
func (veb *versionEditBuilder) apply(bv *version, ikComparator Comparator) (*version, error) {
	log.WithFields(log.Fields{"base-null": bv != nil}).Info("storage::version_edit: apply; started")
	v := new(version)
	for level := range v.files { // store level wise file additions and deletions.
		combined := [2][]fileMetaData{
			nil,
			veb.newFiles[level],
		}

		if bv != nil {
			combined[0] = bv.files[level]
		}

		n := len(combined[0]) + len(combined[1])
		if n == 0 {
			continue
		}

		md := veb.deletedFiles[level]
		for _, addedFiles := range combined {
			for _, f := range addedFiles {
				if md != nil && md[f.fileNum] {
					continue
				}

				v.files[level] = append(v.files[level], f)
			}
		}

		// sort files at each level
		if level == 0 {
			sort.Sort(byFileNum(v.files[level]))
		} else {
			sort.Sort(bySmallest{v.files[level], ikComparator})
		}
	}

	// check final ordering in the version.
	if err := v.checkFiles(ikComparator); err != nil {
		log.Error(fmt.Sprintf("storage::version_edit: apply; error when checking files for sorting order: %v", err))
		return nil, fmt.Errorf("icecanedb: error in version. fatal!: %v", err)
	}
	log.Info("storage::version_edit: apply; done")
	return v, nil
}
