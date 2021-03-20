package storage

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/dr0pdb/icecanedb/internal/common"
	log "github.com/sirupsen/logrus"
)

const (
	unused = 0
)

type versionSet struct {
	dirname string
	options *Options

	ikComparator, ukComparator Comparator

	// The head of the circular linked list containing all the versions.
	// versionDummy.prev is the current version
	versionDummy version

	logNumber, prevLogNumber uint64

	// lastSequenceNumber is the last sequence number that was used.
	lastSequenceNumber uint64

	nextFileNumber     uint64
	manifestFileNumber uint64
}

// load loads a versionSet from the underlying file system.
func (vs *versionSet) load() error {
	log.Info("storage::version_set: load")
	vs.versionDummy.prev = &vs.versionDummy
	vs.versionDummy.next = &vs.versionDummy

	current, err := vs.options.Fs.open(getDbFileName(vs.dirname, currentFileType, unused))
	if err != nil {
		return common.NewNotFoundError(fmt.Sprintf("icecanedb: could not open CURRENT file for DB %q: %v", vs.dirname, err))
	}
	defer current.Close()
	log.Info("storage::version_set: load; reading current file")
	cbuf, err := vs.readCurrentFile(current)
	manifestName := string(cbuf)
	log.Info(fmt.Sprintf("storage::version_set: done reading current file. The manifest file name is %v", manifestName))

	log.Info("storage::version_set: load; open manifest file")
	manifest, err := vs.options.Fs.open(vs.dirname + string(os.PathSeparator) + manifestName)
	if err != nil {
		return common.NewNotFoundError(fmt.Sprintf("icecanedb: could not open MANIFEST file for DB %q: %v", vs.dirname, err))
	}
	defer manifest.Close()

	log.Info("storage::version_set: load; reading manifest file")
	lgr := newLogRecordReader(manifest)
	veb := versionEditBuilder{}
	for {
		slgr, err := lgr.next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		var ve *versionEdit
		err = ve.decode(slgr)
		if err != nil {
			return err
		}

		if ve.comparatorName != "" {
			if ve.comparatorName != vs.ukComparator.Name() {
				return fmt.Errorf("leveldb: manifest file %q for DB %q: "+
					"comparer name from file %q != comparer name from db.Options %q",
					manifestName, vs.dirname, ve.comparatorName, vs.ukComparator.Name())
			}
		}

		veb.append(ve)

		if ve.logNumber != 0 {
			vs.logNumber = ve.logNumber
		}

		if ve.prevLogNumber != 0 {
			vs.prevLogNumber = ve.prevLogNumber
		}

		if ve.nextFileNumber != 0 {
			vs.nextFileNumber = ve.nextFileNumber
		}

		if ve.lastSequenceNumber != 0 {
			vs.lastSequenceNumber = ve.lastSequenceNumber
		}
	}

	log.Info("storage::version_set: load; done reading manifest file")

	// update next file number and mark file numbers as used.
	vs.markFileNumberUsed(vs.logNumber)
	vs.markFileNumberUsed(vs.prevLogNumber)
	vs.manifestFileNumber = vs.nextFileNum()

	newVersion, err := veb.apply(nil, vs.ikComparator)
	if err != nil {
		return err
	}

	vs.append(newVersion)
	log.Info("storage::version_set: load; done")
	return nil
}

// readCurrentFile reads the contents of the current file
func (vs *versionSet) readCurrentFile(current file) ([]byte, error) {
	log.Info("storage::version_set: readCurrentFile")
	stats, err := current.Stat()
	if err != nil {
		return []byte{}, err
	}
	sz := stats.Size()
	if sz == 0 {
		return []byte{}, fmt.Errorf("icecanedb: CURRENT file for DB %q is empty", vs.dirname)
	}
	if sz > 4096 {
		return []byte{}, fmt.Errorf("icecanedb: CURRENT file for DB %q is more than 4096", vs.dirname)
	}

	buf := make([]byte, sz)
	_, err = current.Read(buf)
	if err != nil {
		return []byte{}, err
	}

	if buf[sz-1] != '\n' {
		return []byte{}, fmt.Errorf("icecanedb: CURRENT file for DB %q is malformed", vs.dirname)
	}

	buf = buf[:sz-1]
	log.Info("storage::version_set: readCurrentFile done")
	return buf, nil
}

// logAndApply applies a version edit to the current version to create a new version.
//
// The new version is then persisted and set as the new current version.
//
// mutex mu is assumed to be held. It is released when writing to the disk and held again.
func (vs *versionSet) logAndApply(ve *versionEdit, mu *sync.Mutex) error {
	panic("Not implemented")
}

// markFileNumberUsed marks a file number as being in use.
func (vs *versionSet) markFileNumberUsed(fn uint64) {
	if vs.nextFileNumber <= fn {
		vs.nextFileNumber = fn + 1
	}
}

// nextFileNum returns the next file number and increments it.
func (vs *versionSet) nextFileNum() uint64 {
	res := vs.nextFileNumber
	vs.nextFileNumber++
	return res
}

// currentVersion returns the current version of the version set.
func (vs *versionSet) currentVersion() *version {
	return vs.versionDummy.prev
}

// append appends a version to the version set.
func (vs *versionSet) append(v *version) {
	log.Info("storage::version_set: append; start")
	v.prev = vs.versionDummy.prev
	v.prev.next = v
	v.next = &vs.versionDummy
	v.next.prev = v
	log.Info("storage::version_set: append; done")
}

// newVersionSet creates a new instance of the version set.
//
// it doesn't load the version set from the file system. call load for that.
func newVersionSet(dirname string, ukComparator, ikComparator Comparator, options *Options) *versionSet {
	return &versionSet{
		dirname:      dirname,
		options:      options,
		ukComparator: ukComparator,
		ikComparator: ikComparator,
	}
}
