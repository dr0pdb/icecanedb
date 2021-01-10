package storage

// Snapshot denotes a read-only snapshot of the database.
type Snapshot struct {
	seqNum uint64

	prev, next *Snapshot
}

// seqNumber returns the seq number of the snapshot
func (s *Snapshot) seqNumber() uint64 {
	return s.seqNum
}
