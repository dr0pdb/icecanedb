package storage

// Iterator interface
type Iterator interface {
	// Checks if the current position of the iterator is valid.
	Valid() bool

	// Move to the first entry of the source.
	// Call Valid() to ensure that the iterator is valid after the seek.
	SeekToFirst()

	// Move to the last entry of the source.
	// Call Valid() to ensure that the iterator is valid after the seek.
	SeekToLast()

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
