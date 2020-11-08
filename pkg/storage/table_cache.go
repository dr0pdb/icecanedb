package storage

// tableCache is reponsible for caching the contents of a table.
//
// It has a small cache that stores some of the tables in memory.
// If some table is not present, it gets it from underlying file system.
type tableCache struct {
	dirname string
	fs      FileSystem

	// the allocated capacity for the cache.
	cacheSize uint32

	// map from file number to the table node.
	// len (cache) also denotes the length of the cache linked list.
	cache map[uint64]*table

	// the head of the linked list containing the cached tables.
	// If the size of the linked list exceeds cacheSize, then an entry
	// is removed before adding a new one.
	dummy table
}

// newTableCache creates a new table cache instance.
func newTableCache(dirname string, fs FileSystem, cacheSize uint32) *tableCache {
	tc := tableCache{
		dirname:   dirname,
		fs:        fs,
		cacheSize: cacheSize,
		cache:     make(map[uint64]*table),
	}
	tc.dummy.next = &tc.dummy
	tc.dummy.prev = &tc.dummy
	return &tc
}
