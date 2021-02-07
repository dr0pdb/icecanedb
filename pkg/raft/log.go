package raft

// raftLog is serialized and stored into the raft storage
type raftLog struct {
	command string
	term    uint64
}

// TODO: Add serialization/deserialization logic here.
