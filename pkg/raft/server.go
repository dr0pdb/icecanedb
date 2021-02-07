package raft

import (
	"sync"

	"github.com/dr0pdb/icecanedb/pkg/mvcc"
	"github.com/dr0pdb/icecanedb/pkg/storage"
)

// Server is the icecane kv raft server
type Server struct {
	mu      *sync.Mutex
	id      uint64
	raft    *Raft
	applyCh chan raftServerApplyMsg
	commCh  chan raftServerCommunicationMsg

	// stores actual key-value data
	kvStorage *storage.Storage

	// the mvcc layer for the key-value data
	kvMvcc *mvcc.MVCC

	// snapshot if log size exceeds it. -1 indicates no snapshotting
	// todo: consider passing it to raft.Raft
	maxRaftState int64
}

// NewRaftServer creates a new instance of a Raft server
func NewRaftServer(id uint64, raftStorage, kvStorage *storage.Storage, kvMvcc *mvcc.MVCC) *Server {
	applyCh := make(chan raftServerApplyMsg)
	commCh := make(chan raftServerCommunicationMsg)
	mu := new(sync.Mutex)
	raft := NewRaft(id, raftStorage, applyCh, commCh)

	// TODO: spin up go routines for various processes
	// leader election, handling applyCh and commCh messages

	return &Server{
		id:           id,
		mu:           mu,
		raft:         raft,
		kvStorage:    kvStorage,
		kvMvcc:       kvMvcc,
		applyCh:      applyCh,
		commCh:       commCh,
		maxRaftState: -1,
	}
}
