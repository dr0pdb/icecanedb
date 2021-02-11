package raft

import (
	"context"
	"sync"
	"time"

	"github.com/dr0pdb/icecanedb/pkg/mvcc"
	pb "github.com/dr0pdb/icecanedb/pkg/protogen"
	"github.com/dr0pdb/icecanedb/pkg/storage"
)

const (
	// ElectionTimeout is the duration for which a follower waits before becoming a candidate
	ElectionTimeout = 200 * time.Millisecond
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

//
// grpc server calls
//

// RequestVote is used by the raft candidate to request for votes.
func (s *Server) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return s.raft.requestVote(ctx, request)
}

//
// raft callbacks
//

// NewRaftServer creates a new instance of a Raft server
func NewRaftServer(id uint64, raftStorage, kvStorage *storage.Storage, kvMvcc *mvcc.MVCC) *Server {
	applyCh := make(chan raftServerApplyMsg)
	commCh := make(chan raftServerCommunicationMsg)
	mu := new(sync.Mutex)
	raft := NewRaft(id, raftStorage, applyCh, commCh)

	s := &Server{
		id:           id,
		mu:           mu,
		raft:         raft,
		kvStorage:    kvStorage,
		kvMvcc:       kvMvcc,
		applyCh:      applyCh,
		commCh:       commCh,
		maxRaftState: -1,
	}

	return s
}
