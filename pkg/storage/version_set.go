package storage

import (
	"fmt"
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

	tableCache *tableCache

	ikComparator, ukComparator Comparator

	// The head of the circular linked list containing all the versions.
	// versionDummy.prev is the current version
	versionDummy version

	logNumber, prevLogNumber uint64
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
	log.Info("storage::version_set: reading current file")
	cbuf, err := vs.readCurrentFile(current)
	manifestName := string(cbuf)
	log.Info(fmt.Sprintf("storage::version_set: done reading current file. The manifest file is %v", manifestName))

	// read the manifest file and load the version edit metadata.

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

// currentVersion returns the current version of the version set.
func (vs *versionSet) currentVersion() *version {
	return vs.versionDummy.prev
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
		tableCache:   newTableCache(dirname, options.Fs, options.Cachesz),
	}
}
