package storage

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
