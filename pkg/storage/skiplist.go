package storage

// SkipList is the concurrent probabilistic data structure
type SkipList interface {
}

type skiplist struct {
}

// NewSkipList creates a new SkipList
func NewSkipList() SkipList {
	return &skiplist{}
}
