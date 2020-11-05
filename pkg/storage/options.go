package storage

const (
	defaultTableCacheSize uint32 = 64
)

// Options defines all of the configuration options available with the storage layer.
type Options struct {
	// The instance of fileSystem interface that is going to be used to store data.
	// most of the times it is the DefaultFileSystem which uses the default OS file system.
	fs *fileSystem

	// The table cache size.
	// set to zero for defaultTableCacheSize.
	cachesz uint32
}
