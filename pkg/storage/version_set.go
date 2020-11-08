package storage

import "fmt"

const (
	unused = 0
)

type versionSet struct {
	dirname string
	options *Options

	tableCache *tableCache

	ikComparator, ukComparator Comparator

	// The head of the circular linked list containing all the versions.
	versionDummy version
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
