package storage

// Memtable is the in-memory store
type Memtable interface {
}

type memtable struct {
	skiplist SkipList
}

// NewMemtable returns a new instance of the Memtable
func NewMemtable(skiplist SkipList) Memtable {
	return &memtable{
		skiplist: skiplist,
	}
}
