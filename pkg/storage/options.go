package storage

const (
	defaultTableCacheSize uint32 = 64
)

// Options defines all of the configuration options available with the storage layer.
type Options struct {
	// The instance of FileSystem interface that is going to be used to store data.
	// most of the times it is the DefaultFileSystem which uses the default OS file system.
	Fs FileSystem

	// The table cache size.
	// set to zero for defaultTableCacheSize.
	Cachesz uint32
}
