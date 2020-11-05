package storage

import (
	"path/filepath"
)

const (
	lockFileName = "LOCK"
)

// tableCache is reponsible for handling everything related to tables.
//
// It has a small cache that stores some of the tables in memory.
// If some table is not present, it gets it from underlying file system.
type tableCache struct {
	dirname   string
	fs        fileSystem
	cacheSize uint32

	cache []*table
}

// newTableCache creates a new table cache instance.
//
// it locks the given directory for exclusive access.
func newTableCache(dirname string, fs fileSystem, cacheSize uint32) *tableCache {
	tc := tableCache{
		dirname:   dirname,
		fs:        fs,
		cacheSize: cacheSize,
		cache:     make([]*table, cacheSize),
	}

	// create lock file to lock the directory.
	// TODO: implement the logic for ensuring that we wrote this file.
	fs.lock(filepath.Join(dirname, lockFileName))

	// getOrCreateCurrent()

	return &tc
}
