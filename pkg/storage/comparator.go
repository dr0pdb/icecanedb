package storage

import (
	"bytes"
)

// Comparator defines a total ordering over the []byte key space.
// It is used in Memtable as well as in SST.
type Comparator interface {
	// Compare returns -1, 0, 1 if a is less than, equal to or greater than b respectively.
	// empty slice is assumed to be less than any non-empty slice.
	Compare(a, b []byte) int

	// Name returns the name of the comparator
	//
	// The data is stored in the sorted order determined by a comparator.
	// Hence opening a database with a different comparator than the one it was
	// created with will cause an error
	Name() string
}

// DefaultComparator is the default comparator which uses byte wise ordering.
var DefaultComparator Comparator = defaultComparator{}

type defaultComparator struct{}

func (d defaultComparator) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

func (d defaultComparator) Name() string {
	return "BytewiseComparator"
}
