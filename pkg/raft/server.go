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
	"google.golang.org/grpc"
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

	// clientConnections contains the grpc client connections made with other raft peers.
	// key: id of the peer.
	clientConnections map[uint64]*grpc.ClientConn
}

//
// grpc server calls.
// forwarded to raft
//

// RequestVote is used by the raft candidate to request for votes.
func (s *Server) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return s.raft.requestVote(ctx, request)
}

// Close cleanups the underlying resources of the raft server.
func (s *Server) Close() {
	log.Info("raft::server::Close; started")
	for _, conn := range s.clientConnections {
		conn.Close()
	}
	log.Info("raft::server::Close; started")
}

//
// raft callbacks
// either grpc calls are made or changes are made to the storage layer.
//

func (s *Server) sendRequestVote(voterID uint64, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::sendRequestVote; sending vote to %d peer", voterID))

	conn, err := s.getOrCreateClientConnection(voterID)
	if err != nil {
		log.Error(fmt.Sprintf("raft::server::sendRequestVote; error in getting conn: %v", err))
		return nil, err
	}

	client := pb.NewIcecaneKVClient(conn)
	resp, err := client.RequestVote(context.Background(), request) //todo: do we need a different context?
	if err != nil {
		log.Error(fmt.Sprintf("raft::server::sendRequestVote; error in grpc request: %v", err))
	} else {
		log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::sendRequestVote; received resp from peer %d, result: %v", voterID, resp.VoteGranted))
	}
	return resp, err
}

//
// raft server utility functions
//

// getOrCreateClientConnection gets or creates a grpc client connection for talking to peer with given id.
// In the case of creation, it caches it the clientConnections map
func (s *Server) getOrCreateClientConnection(voterID uint64) (*grpc.ClientConn, error) {
	log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::getOrCreateClientConnection; peer %d", voterID))
	if conn, ok := s.clientConnections[voterID]; ok {
		return conn, nil
	}
	var p *common.Peer = nil

	for _, peer := range s.kvConfig.Peers {
		if peer.ID == voterID {
			p = &peer
			break
		}
	}

	if p == nil {
		return nil, fmt.Errorf("invalid peer id %d", voterID)
	}

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	opts = append(opts, grpc.WithBlock())
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", p.Address, p.Port), opts...)
	if err != nil {
		return nil, err
	}
	s.clientConnections[voterID] = conn
	return conn, nil
}

// NewRaftServer creates a new instance of a Raft server
func NewRaftServer(kvConfig *common.KVConfig, raftStorage, kvStorage *storage.Storage, kvMvcc *mvcc.MVCC) *Server {
	log.Info("raft::server::NewRaftServer; started")
	applyCh := make(chan raftServerApplyMsg)
	commCh := make(chan raftServerCommunicationMsg)
	mu := new(sync.Mutex)

	s := &Server{
		id:                kvConfig.ID,
		mu:                mu,
		kvStorage:         kvStorage,
		kvMvcc:            kvMvcc,
		applyCh:           applyCh,
		commCh:            commCh,
		maxRaftState:      -1,
		kvConfig:          kvConfig,
		clientConnections: make(map[uint64]*grpc.ClientConn),
	}

	raft := NewRaft(kvConfig, raftStorage, s)
	s.raft = raft

	log.Info("raft::server::NewRaftServer; done")
	return s
}
