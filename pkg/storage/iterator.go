package storage

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
	itr *SkipListIterator
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
}

// Key returns the key of the current iterator position.
// REQUIRES: Current position of iterator is valid. Panics otherwise.
func (si *KeyValueIterator) Key() []byte {
	// extract the user key out of the internal key
	ikey := internalKey(si.itr.Key())
	return ikey.userKey()
}

// Value gets the value of the current iterator position.
// REQUIRES: Current position of iterator is valid. Panics otherwise.
func (si *KeyValueIterator) Value() []byte {
	return si.itr.Value()
}
