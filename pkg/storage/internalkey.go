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
	var ikey internalKey = make(internalKey, len(userKey)+8)

	i := copy(ikey, userKey)
	ikey[i] = uint8(kind)
	ikey[i+1] = uint8(sequenceNumber) // last 1 byte of seq number
	ikey[i+2] = uint8(sequenceNumber >> 8)
	ikey[i+3] = uint8(sequenceNumber >> 16)
	ikey[i+4] = uint8(sequenceNumber >> 24)
	ikey[i+5] = uint8(sequenceNumber >> 32)
	ikey[i+6] = uint8(sequenceNumber >> 40)
	ikey[i+7] = uint8(sequenceNumber >> 48)

	return ikey
}

// userKey extracts the user key from the internal key and returns a new slice.
// assumes that the internal key is valid. Will panic if it isn't.
func (ik internalKey) userKey() []byte {
	return []byte(ik[:len(ik)-8])
}

// kind extracts the key kind from an internal key.
// assumes that the internal key is valid. Will panic if it isn't
func (ik internalKey) kind() internalKeyKind {
	return internalKeyKind(ik[len(ik)-8])
}

// sequenceNumber returns the sequence number of the internal key.
// assumes that the internal key is valid. Will panic if it isn't
func (ik internalKey) sequenceNumber() uint64 {
	var sn uint64 = 0

	i := len(ik) - 7
	sn |= uint64(ik[i])
	sn |= uint64(ik[i+1]) << 8
	sn |= uint64(ik[i+2]) << 16
	sn |= uint64(ik[i+3]) << 24
	sn |= uint64(ik[i+4]) << 32
	sn |= uint64(ik[i+5]) << 40
	sn |= uint64(ik[i+6]) << 48

	return sn
}

// valid returns if the internal key is valid structurally.
func (ik internalKey) valid() bool {
	return len(ik) >= 8
}

// internalKeyComparator is the comparator which uses a user key comparator to compare internal key.
//
// keys are first compared for their user key according to the user key comparator.
// ties are broken by comparing sequence number (decreasing) and then by kind (decreasing).
type internalKeyComparator struct {
	userKeyComparator Comparator
}

func (d *internalKeyComparator) Compare(a, b []byte) int {
	akey, bkey := internalKey(a), internalKey(b)
	if !akey.valid() {
		if bkey.valid() {
			return -1
		}
		return bytes.Compare(a, b) // both invalid, so return byte wise comparison
	}
	if !bkey.valid() {
		return 1
	}

	if ukc := d.userKeyComparator.Compare(akey.userKey(), bkey.userKey()); ukc != 0 {
		return ukc
	}

	if asqn, bsqn := akey.sequenceNumber(), bkey.sequenceNumber(); asqn < bsqn {
		return 1
	} else if asqn > bsqn {
		return -1
	}

	if ak, bk := akey.kind(), bkey.kind(); ak < bk {
		return 1
	} else if ak > bk {
		return -1
	}

	return 0
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
