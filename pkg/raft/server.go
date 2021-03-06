package raft

import (
	"context"
	"fmt"
	"strings"
	"sync"

	common "github.com/dr0pdb/icecanedb/pkg/common"
	pb "github.com/dr0pdb/icecanedb/pkg/protogen"
	"github.com/dr0pdb/icecanedb/pkg/storage"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// Server is the icecane kv raft server
type Server struct {
	mu   *sync.Mutex
	id   uint64
	raft *Raft

	// stores actual key-value data
	kvStorage *storage.Storage

	kvConfig *common.KVConfig

	// clientConnections contains the grpc client connections made with other raft peers.
	// key: id of the peer.
	clientConnections *common.ProtectedMapUConn
}

//
// grpc server calls.
// forwarded to raft
//

// RequestVote is used by the raft candidate to request for votes.
func (s *Server) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return s.raft.requestVote(ctx, request)
}

// AppendEntries is invoked by leader to replicate log entries; also used as heartbeat
func (s *Server) AppendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return s.raft.appendEntries(ctx, request)
}

// Close cleanups the underlying resources of the raft server.
func (s *Server) Close() {
	log.Info("raft::server::Close; started")
	for _, conn := range s.clientConnections.Iterate() {
		conn.Close()
	}
	log.Info("raft::server::Close; started")
}

//
// mvcc layer calls
//
//

// GetValue returns the value of the key from storage layer.
func (s *Server) GetValue(key []byte) ([]byte, uint64, error) {
	leader := s.raft.getLeaderID()
	if leader != s.id {
		return nil, leader, fmt.Errorf("not a leader")
	}

	return nil, leader, nil
}

// SetValue sets the value of the key and gets it replicated across peers
func (s *Server) SetValue(key, value []byte) (leader uint64, err error) {
	leader = s.raft.getLeaderID()
	if leader != s.id {
		return leader, fmt.Errorf("not a leader")
	}

	return 0, nil
}

// DeleteValue deletes the value of the key and gets it replicated across peers
func (s *Server) DeleteValue(key []byte) (leader uint64, err error) {
	leader = s.raft.getLeaderID()
	if leader != s.id {
		return leader, fmt.Errorf("not a leader")
	}

	return 0, nil
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

func (s *Server) sendAppendEntries(receiverID uint64, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::sendAppendEntries; sending append entries to peer %d", receiverID))

	conn, err := s.getOrCreateClientConnection(receiverID)
	if err != nil {
		log.Error(fmt.Sprintf("raft::server::sendAppendEntries; error in getting conn: %v", err))
		return nil, err
	}

	client := pb.NewIcecaneKVClient(conn)
	resp, err := client.AppendEntries(context.Background(), req) //todo: do we need a different context?
	if err != nil {
		log.Error(fmt.Sprintf("raft::server::sendAppendEntries; error in grpc request: %v", err))
	} else {
		log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::sendAppendEntries; received resp from peer %d, result: %v", receiverID, resp.Success))
	}
	return resp, err
}

// applyEntry applies the raft log to the storage engine.
func (s *Server) applyEntry(rl *raftLog) (err error) {
	log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::applyEntry; term: %d ct: %v cmd: %s", rl.term, rl.ct, rl.command))

	if rl.ct == setCmd {
		parts := strings.Fields(string(rl.command))
		if len(parts) == 3 && parts[0] == "SET" {
			err = s.kvStorage.Set([]byte(parts[1]), []byte(parts[2]), nil)
		} else {
			err = fmt.Errorf("invalid set log command")
		}
	} else {
		parts := strings.Fields(string(rl.command))
		if len(parts) == 2 && parts[0] == "DELETE" {
			err = s.kvStorage.Delete([]byte(parts[1]), nil)
		} else {
			err = fmt.Errorf("invalid delete log command")
		}
	}

	log.WithFields(log.Fields{"id": s.id}).Info("raft::server::applyEntry; done")
	return err
}

//
// raft server utility functions
//

// getOrCreateClientConnection gets or creates a grpc client connection for talking to peer with given id.
// In the case of creation, it caches it the clientConnections map
func (s *Server) getOrCreateClientConnection(voterID uint64) (*grpc.ClientConn, error) {
	log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::getOrCreateClientConnection; peer %d", voterID))
	if conn, ok := s.clientConnections.Get(voterID); ok && conn.GetState().String() == "READY" {
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
	s.clientConnections.Set(voterID, conn)
	return conn, nil
}

// createAndOpenRaftStorage creates a storage and opens it.
func createAndOpenRaftStorage(path string, opts *storage.Options) (*storage.Storage, error) {
	s, err := storage.NewStorage(path, opts)
	if err != nil {
		return nil, err
	}
	err = s.Open()
	return s, err
}

// createAndOpenKVStorage creates a storage and opens it.
func createAndOpenKVStorage(path string, txnComp storage.Comparator, opts *storage.Options) (*storage.Storage, error) {
	s, err := storage.NewStorageWithCustomComparator(path, txnComp, opts)
	if err != nil {
		return nil, err
	}
	err = s.Open()
	return s, err
}

// NewRaftServer creates a new instance of a Raft server
func NewRaftServer(kvConfig *common.KVConfig, raftPath, kvPath string, txnComp storage.Comparator) (*Server, error) {
	log.Info("raft::server::NewRaftServer; started")

	rOpts := &storage.Options{
		CreateIfNotExist: true,
	}
	raftStorage, err := createAndOpenRaftStorage(raftPath, rOpts)
	if err != nil {
		log.Error(fmt.Sprintf("raft::server::NewRaftServer; error in creating raft storage: %v", err))
		return nil, err
	}

	sOpts := &storage.Options{
		CreateIfNotExist: true,
	}
	kvStorage, err := createAndOpenKVStorage(kvPath, txnComp, sOpts)
	if err != nil {
		log.Error(fmt.Sprintf("raft::server::NewRaftServer; error in creating kv storage: %v", err))
		return nil, err
	}

	mu := new(sync.Mutex)
	s := &Server{
		id:                kvConfig.ID,
		mu:                mu,
		kvStorage:         kvStorage,
		kvConfig:          kvConfig,
		clientConnections: common.NewProtectedMapUConn(),
	}

	raft := NewRaft(kvConfig, raftStorage, s)
	s.raft = raft

	log.Info("raft::server::NewRaftServer; done")
	return s, nil
}
