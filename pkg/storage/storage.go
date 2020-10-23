package storage

// Storage is a Key-value store
type Storage interface {
	Get(key []byte) (value []byte, err error)

	Set(key, value []byte) error

	Delete(key []byte) error

	Close() error
}

// storage is the persistent key-value storage struct
// It contains all the necessary information for the storage
type storage struct {
	dirname string
}

func (s *storage) Get(key []byte) (value []byte, err error) {
	panic("not implemented")
}

func (s *storage) Set(key, value []byte) error {
	panic("not implemented")
}

func (s *storage) Delete(key []byte) error {
	panic("not implemented")
}

func (s *storage) Close() error {
	panic("not implemented")
}

// NewStorage creates a new persistent storage
func NewStorage(dirname string) Storage {
	return &storage{
		dirname: dirname,
	}
}
