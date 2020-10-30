package storage

import "bytes"

// internalKey is the key used for memtable and SST in the db.
//
// It consists of the user key along with a 8-byte suffix.
// The 8 byte suffix consists of:
//    - 1 byte segment defining the kind of operation: delete or set
//    - 7 bytes segment defining the sequence number.
type internalKey []byte

type internalKeyKind uint8

const (
	// This is part of the file format and stored on the disk. Don't change
	internalKeyKindDelete internalKeyKind = 0
	internalKeyKindSet    internalKeyKind = 1
)

// newInternalKey generates an internalKey from a userKey, kind and a sequence number.
func newInternalKey(userKey []byte, kind internalKeyKind, sequenceNumber uint64) internalKey {
	panic("Not Implemented")
}

// userKey extracts the user key from the internal key and returns a new slice.
func (ik internalKey) userKey() []byte {
	panic("Not implemented")
}

// kind extracts the key kind from an internal key.
func (ik internalKey) kind() internalKeyKind {
	panic("Not implemented")
}

// sequenceNumber returns the sequence number of the internal key.
func (ik internalKey) sequenceNumber() uint64 {
	panic("Not implemented")
}

// valid returns if the internal key is valid structurally.
func (ik internalKey) valid() bool {
	panic("Not implemented")
}

// internalKeyComparator is the comparator which uses a user key comparator to compare internal key.
//
// keys are first compared for their user key according to the user key comparator.
// ties are broken by comparing sequence number (decreasing) and then by kind (decreasing).
type internalKeyComparator struct {
	userKeyComparator Comparator
}

func (d *internalKeyComparator) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

func (d *internalKeyComparator) Name() string {
	return "InternalKeyComparator"
}

// newInternalKeyComparator creates a new instance of an internalKeyComparator
// returns a pointer to the Comparator interface.
func newInternalKeyComparator(userKeyComparator Comparator) Comparator {
	return &internalKeyComparator{
		userKeyComparator: userKeyComparator,
	}
}
