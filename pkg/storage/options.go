package storage

const (
	defaultTableCacheSize uint32 = 64
)

// Options defines all of the configuration options available with the storage layer.
type Options struct {
	// Create the database files if it doesn't exist. If set to true, in case the CURRENT file isn't found
	// in the directory. A new CURRENT and other files will be created.
	CreateIfNotExist bool

	// The instance of FileSystem interface that is going to be used to store data.
	// most of the times it is the DefaultFileSystem which uses the default OS file system.
	Fs FileSystem

	// The table cache size.
	// set to zero for defaultTableCacheSize.
	Cachesz uint32

	// Sync indicates whether updates should be synced to the disk. If not set, it could lead to data loss due to sudden errors.
	// set it to true periodically.
	Sync bool
}
