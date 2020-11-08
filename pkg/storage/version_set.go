package storage

type versionSet struct {
	dirname string
	options *Options

	tableCache *tableCache
}

// load loads a versionSet from the underlying file system.
func (*versionSet) load() {

}

// newVersionSet creates a new instance of the version set.
//
// it doesn't load the version set from the file system. call load for that.
func newVersionSet(dirname string, internalKeyComparator Comparator, options *Options) *versionSet {
	return &versionSet{
		dirname:    dirname,
		options:    options,
		tableCache: newTableCache(dirname, *options.fs, options.cachesz),
	}
}
