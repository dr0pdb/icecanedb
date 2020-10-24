package storage

// Comparator is the comparator used for comparing two byte slices.
type Comparator interface {

	// Compare is the usual comparison function.
	// Returns
	// 		 1 : a < b
	//		 0 : a == b
	//		-1 : a > b
	Compare(a, b []byte) int
}

type internalKey struct {
	userKey        []byte
	sequenceNumber []byte
}
