package storage

import "bytes"

// Iterator interface
type Iterator interface {
	// Checks if the current position of the iterator is valid.
	Valid() bool

	// Move to the first entry of the source.
	// Call Valid() to ensure that the iterator is valid after the seek.
	SeekToFirst()

	// Seek the iterator to the first element whose key is >= target
	// Call Valid() to ensure that the iterator is valid after the seek.
	Seek(target []byte)

	// Moves to the next key-value pair in the source.
	// Call valid() to ensure that the iterator is valid.
	// REQUIRES: Current position of iterator is valid. Panic otherwise.
	Next()

	// Get the key of the current iterator position.
	// REQUIRES: Current position of iterator is valid. Panics otherwise.
	Key() []byte

	// Get the value of the current iterator position.
	// REQUIRES: Current position of iterator is valid. Panics otherwise.
	Value() []byte
}

// KeyValueIterator is a wrapper over the SkipListIterator
type KeyValueIterator struct {
	itr         *SkipListIterator
	prevUserKey []byte

	// the seq number of the query. All the internal keys with a higher seq number
	// are skipped by the iterator
	// this is important for the snapshot isolation guarantees required by mvcc layer.
	querySeqNum uint64
}

var _ Iterator = (*KeyValueIterator)(nil)

// Valid checks if the current position of the iterator is valid.
func (si *KeyValueIterator) Valid() bool {
	return si.itr.Valid()
}

// SeekToFirst moves to the first entry of the source.
// Call Valid() to ensure that the iterator is valid after the seek.
func (si *KeyValueIterator) SeekToFirst() {
	si.itr.SeekToFirst()
}

// Seek the iterator to the first element whose key is >= target
// Call Valid() to ensure that the iterator is valid after the seek.
func (si *KeyValueIterator) Seek(target []byte) {
	si.itr.Seek(target)
}

// Next moves to the next key-value pair in the source.
// Call valid() to ensure that the iterator is valid.
// REQUIRES: Current position of iterator is valid. Panic otherwise.
func (si *KeyValueIterator) Next() {
	si.itr.Next()

	// internally we store a number of values for the same user key.
	// we need to hide this implementation detail from the outside world
	// we only need to return the latest value for a given user key.
	for {
		if !si.Valid() {
			return
		}

		k := si.Key()
		if !bytes.Equal(si.prevUserKey, k) && si.seqNum() <= si.querySeqNum {
			si.prevUserKey = k
			return
		}

		si.itr.Next()
	}
}

// Key returns the key of the current iterator position.
// REQUIRES: Current position of iterator is valid. Panics otherwise.
func (si *KeyValueIterator) Key() []byte {
	// extract the user key out of the internal key
	ikey := internalKey(si.itr.Key())
	uk := ikey.userKey()
	return uk
}

// Value gets the value of the current iterator position.
// REQUIRES: Current position of iterator is valid. Panics otherwise.
func (si *KeyValueIterator) Value() []byte {
	return si.itr.Value()
}

// seqNum returns the seq number of the internal key.
// to support snapshot isolation in scan
func (si *KeyValueIterator) seqNum() uint64 {
	// extract the seq num out of the internal key
	ikey := internalKey(si.itr.Key())
	return ikey.sequenceNumber()
}

func newKeyValueIterator(itr *SkipListIterator, sn uint64) *KeyValueIterator {
	kvItr := &KeyValueIterator{
		itr:         itr,
		querySeqNum: sn,
	}

	if itr.Valid() {
		kvItr.prevUserKey = internalKey(itr.Key()).userKey()
	}

	return kvItr
}
