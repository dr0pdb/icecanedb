package raft

import (
	"github.com/dr0pdb/icecanedb/pkg/storage"
)

// PeerRole defines the role of a raft peer.
type PeerRole uint64

const (
	leader PeerRole = iota
	follower
	candidate
)

// Progress denotes the progress of a peer.
// It indicates where the peer is in it's log.
type Progress struct {
	Match, Next uint64
}

// Raft todo
type Raft struct {
	// id of the raft node.
	// IMPORTANT: starts from 1 so that Vote = 0 indicates no vote.
	id uint64

	// Term denotes the latest term node has seen.
	// Vote denotes the candidate id that got the vote from this node. 0 is no one.
	Term, Vote uint64

	// stores raft logs
	raftStorage *storage.Storage

	// each peers progress
	allProgress map[uint64]*Progress

	// this peer's role
	role PeerRole

	// applyCh is used to communicate with the wrapper server
	applyCh chan raftServerApplyMsg
	commCh  chan raftServerCommunicationMsg
}

// NewRaft todo
func NewRaft(id uint64, raftStorage *storage.Storage, applyCh chan raftServerApplyMsg, commCh chan raftServerCommunicationMsg) *Raft {
	return &Raft{
		id:          id,
		raftStorage: raftStorage,
		applyCh:     applyCh,
		commCh:      commCh,
	}
}
