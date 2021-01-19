package storage

// Snapshot denotes a read-only snapshot of the database.
type Snapshot struct {
	SeqNum uint64

	prev, next *Snapshot
}

// SeqNumber returns the seq number of the snapshot
func (s *Snapshot) SeqNumber() uint64 {
	return s.SeqNum
}
