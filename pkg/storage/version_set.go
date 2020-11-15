package storage

import (
	"fmt"
	"sync"
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
	vs.versionDummy.prev = &vs.versionDummy
	vs.versionDummy.next = &vs.versionDummy

	current, err := vs.options.Fs.open(getDbFileName(vs.dirname, currentFileType, unused))
	if err != nil {
		return fmt.Errorf("icecanedb: could not open CURRENT file for DB %q: %v", vs.dirname, err)
	}
	defer current.Close()

	return nil
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
