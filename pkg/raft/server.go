package raft

import (
	"context"
	"fmt"
	"sync"

	common "github.com/dr0pdb/icecanedb/pkg/common"
	"github.com/dr0pdb/icecanedb/pkg/mvcc"
	pb "github.com/dr0pdb/icecanedb/pkg/protogen"
	"github.com/dr0pdb/icecanedb/pkg/storage"
	log "github.com/sirupsen/logrus"
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

	kvConfig *common.KVConfig
}

//
// grpc server calls.
// forwarded to raft
//

// RequestVote is used by the raft candidate to request for votes.
func (s *Server) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return s.raft.requestVote(ctx, request)
}

//
// raft callbacks
// forwarded to grpc server or changes are made to the storage layer.
//

func (s *Server) sendRequestVote(voterID uint64, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::sendRequestVote; sending vote to %d peer", voterID))

	return nil, nil
}

// NewRaftServer creates a new instance of a Raft server
func NewRaftServer(kvConfig *common.KVConfig, raftStorage, kvStorage *storage.Storage, kvMvcc *mvcc.MVCC) *Server {
	log.Info("raft::server::NewRaftServer; started")
	applyCh := make(chan raftServerApplyMsg)
	commCh := make(chan raftServerCommunicationMsg)
	mu := new(sync.Mutex)
	raft := NewRaft(kvConfig, raftStorage, applyCh, commCh)

	s := &Server{
		id:           kvConfig.ID,
		mu:           mu,
		raft:         raft,
		kvStorage:    kvStorage,
		kvMvcc:       kvMvcc,
		applyCh:      applyCh,
		commCh:       commCh,
		maxRaftState: -1,
		kvConfig:     kvConfig,
	}
	log.Info("raft::server::NewRaftServer; done")
	return s
}
