package raft

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	common "github.com/dr0pdb/icecanedb/pkg/common"
	pb "github.com/dr0pdb/icecanedb/pkg/protogen/icecanedbpb"
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
	kvStorage, kvMetaStorage *storage.Storage

	kvConfig *common.KVConfig

	// clientConnections contains the grpc client connections made with other raft peers.
	// key: id of the peer.
	clientConnections *common.ProtectedMapUConn

	// the raft commit idx
	raftCommitIdx uint64

	Th *TestHelpers
}

// TestHelpers contains fields that are used in testing.
type TestHelpers struct {
	// Drop the rpc calls if this is true
	Drop map[uint64]bool
}

type DiagInfo struct {
	Role State

	RaftCommitIdx uint64

	Term uint64
}

//
// grpc server calls.
// forwarded to raft
//

// RequestVote is used by the raft candidate to request for votes.
func (s *Server) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return s.raft.handleRequestVote(ctx, request)
}

// AppendEntries is invoked by leader to replicate log entries; also used as heartbeat
func (s *Server) AppendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return s.raft.handleAppendEntries(ctx, request)
}

// Close cleanups the underlying resources of the raft server.
func (s *Server) Close() {
	log.WithFields(log.Fields{"id": s.id}).Info("raft::server::Close; started")

	// shutdown raft
	s.raft.close()

	// close grpc conns
	for _, conn := range s.clientConnections.Iterate() {
		conn.Close()
	}

	// close storage layers
	s.kvMetaStorage.Close()
	s.kvStorage.Close()

	log.WithFields(log.Fields{"id": s.id}).Info("raft::server::Close; started")
}

//
// mvcc layer calls
//
//

// Scan returns an iterator to iterate over all the kv pairs whose key >= target
func (s *Server) Scan(target []byte) (storage.Iterator, bool, error) {
	log.WithFields(log.Fields{"id": s.id}).Info("raft::server::Scan; started")
	_, _, role := s.raft.getNodeState()
	if role != Leader {
		return nil, false, nil
	}

	itr := s.kvStorage.Scan(target)
	log.WithFields(log.Fields{"id": s.id}).Info("raft::server::Scan; done")
	return itr, true, nil
}

// SetValue sets the value of the key and gets it replicated across peers
func (s *Server) SetValue(key, value []byte, meta bool) (bool, error) {
	log.WithFields(log.Fields{"id": s.id}).Info("raft::server::SetValue; started")
	idx, success, err := s.raft.handleClientSetRequest(key, value, meta)
	if err != nil {
		log.WithFields(log.Fields{"id": s.id}).Error(fmt.Sprintf("raft::server::SetValue; error while setting. err: %+v", err))
		return false, err
	}
	if !success {
		log.WithFields(log.Fields{"id": s.id}).Error("raft::server::SetValue; unsuccessful write")
		return success, nil
	}

	log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::SetValue; waiting for idx %d to be committed", idx))
	for {
		if s.raftCommitIdx >= idx {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}
	log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::SetValue; successfully committed at %d", idx))

	return true, err
}

// DeleteValue deletes the value of the key and gets it replicated across peers
func (s *Server) DeleteValue(key []byte, meta bool) (bool, error) {
	log.WithFields(log.Fields{"id": s.id}).Info("raft::server::DeleteValue; started")

	idx, success, err := s.raft.handleClientDeleteRequest(key, meta)
	if err != nil {
		log.WithFields(log.Fields{"id": s.id}).Error(fmt.Sprintf("raft::server::DeleteValue; error while deleting. err: %+v", err))
		return false, err
	}
	if !success {
		log.WithFields(log.Fields{"id": s.id}).Error("raft::server::DeleteValue; unsuccessful delete")
		return success, nil
	}

	log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::DeleteValue; waiting for idx %d to be committed", idx))
	for {
		if s.raftCommitIdx >= idx {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}
	log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::DeleteValue; successfully committed at %d", idx))

	return true, err
}

// MetaGetValue returns the value of the key from meta storage layer.
func (s *Server) MetaGetValue(key []byte) ([]byte, bool, error) {
	log.WithFields(log.Fields{"id": s.id}).Info("raft::server::MetaGetValue; started")
	_, _, role := s.raft.getNodeState()
	if role != Leader {
		return nil, false, nil
	}

	val, err := s.kvMetaStorage.Get(key, nil)
	log.WithFields(log.Fields{"id": s.id}).Info("raft::server::MetaGetValue; done")
	return val, true, err
}

// MetaScan returns an iterator to iterate over all the kv pairs whose key >= target
func (s *Server) MetaScan(target []byte) (storage.Iterator, bool, error) {
	log.WithFields(log.Fields{"id": s.id}).Info("raft::server::MetaScan; started")
	_, _, role := s.raft.getNodeState()
	if role != Leader {
		return nil, false, nil
	}

	itr := s.kvMetaStorage.Scan(target)
	log.WithFields(log.Fields{"id": s.id}).Info("raft::server::MetaScan; done")
	return itr, true, nil
}

//
// Diagnostic calls
// For testing and debugging
//

// GetDiagnosticInformation returns the diag state of the raft server
func (s *Server) GetDiagnosticInformation() *DiagInfo {
	term, commitIdx, role := s.raft.getNodeState()

	return &DiagInfo{
		Role:          role,
		RaftCommitIdx: commitIdx,
		Term:          term,
	}
}

// GetLogAtIndex returns the raft log present at the given index
// Returns default logs if nothing is found
func (s *Server) GetLogAtIndex(idx uint64) *RaftLog {
	return s.raft.getLogEntryOrDefault(idx)
}

//
// raft callbacks
// either grpc calls are made or changes are made to the storage layer.
//

func (s *Server) sendRequestVote(voterID uint64, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::sendRequestVote; sending vote request to %d peer", voterID))

	if drop, ok := s.Th.Drop[voterID]; ok && drop {
		return nil, fmt.Errorf("dropping request for testing")
	}

	conn, err := s.getOrCreateClientConnection(voterID)
	if err != nil {
		log.Error(fmt.Sprintf("raft::server::sendRequestVote; error in getting conn: %v", err))
		return nil, err
	}

	client := pb.NewIcecaneKVClient(conn)
	resp, err := client.RequestVote(context.Background(), request)
	if err != nil {
		log.Error(fmt.Sprintf("raft::server::sendRequestVote; error in grpc request: %v", err))
	} else {
		log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::sendRequestVote; received resp from peer %d, result: %v", voterID, resp.VoteGranted))
	}
	return resp, err
}

func (s *Server) sendAppendEntries(receiverID uint64, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::sendAppendEntries; sending append entries to peer %d", receiverID))

	if drop, ok := s.Th.Drop[receiverID]; ok && drop {
		return nil, fmt.Errorf("dropping request for testing")
	}

	conn, err := s.getOrCreateClientConnection(receiverID)
	if err != nil {
		log.Error(fmt.Sprintf("raft::server::sendAppendEntries; error in getting conn: %v", err))
		return nil, err
	}

	client := pb.NewIcecaneKVClient(conn)
	resp, err := client.AppendEntries(context.Background(), req)
	if err != nil {
		log.Error(fmt.Sprintf("raft::server::sendAppendEntries; error in grpc request: %v", err))
	} else {
		log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::sendAppendEntries; received resp from peer %d, result: %v", receiverID, resp.Success))
	}
	return resp, err
}

// applyEntry applies the raft log to the storage and meta engines.
func (s *Server) applyEntry(rl *RaftLog, logIndex uint64) (err error) {
	log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::applyEntry; term: %d ct: %v logIndex: %d", rl.Term, rl.Ct, logIndex))

	if rl.Ct == SetCmd {
		err = s.kvStorage.Set(rl.Key, rl.Value, &storage.WriteOptions{Sync: true})
	} else if rl.Ct == DeleteCmd {
		err = s.kvStorage.Delete(rl.Key, &storage.WriteOptions{Sync: true})
	} else if rl.Ct == MetaSetCmd {
		err = s.kvMetaStorage.Set(rl.Key, rl.Value, &storage.WriteOptions{Sync: true})
	} else if rl.Ct == MetaDeleteCmd {
		err = s.kvMetaStorage.Delete(rl.Key, &storage.WriteOptions{Sync: true})
	}

	if err != nil {
		log.WithFields(log.Fields{"id": s.id}).Error(fmt.Sprintf("raft::server::applyEntry; error while applying entry. err: %v", err.Error()))
	} else {
		log.WithFields(log.Fields{"id": s.id}).Info("raft::server::applyEntry; done")
	}

	return err
}

// updateRaftIdx updates the raft index in server
func (s *Server) updateRaftIdx(idx, expected uint64) {
	log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::updateRaftIdx; updating raftLogIndex from %d to %d", expected, idx))
	for {
		if success := atomic.CompareAndSwapUint64(&s.raftCommitIdx, expected, idx); success {
			break
		}
	}
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

// createAndOpenStorage creates a storage and opens it.
func createAndOpenStorage(path, name string, opts *storage.Options) (*storage.Storage, error) {
	s, err := storage.NewStorage(path, name, opts)
	if err != nil {
		return nil, err
	}
	err = s.Open()
	return s, err
}

// createAndOpenKVStorage creates a storage and opens it.
func createAndOpenKVStorage(path, name string, txnComp storage.Comparator, opts *storage.Options) (*storage.Storage, error) {
	s, err := storage.NewStorageWithCustomComparator(path, name, txnComp, opts)
	if err != nil {
		return nil, err
	}
	err = s.Open()
	return s, err
}

// NewRaftServer creates a new instance of a Raft server
func NewRaftServer(kvConfig *common.KVConfig, raftPath, kvPath, kvMetaPath string, txnComp storage.Comparator, ready <-chan interface{}) (*Server, error) {
	log.Info("raft::server::NewRaftServer; started")

	rOpts := &storage.Options{
		CreateIfNotExist: true,
	}
	raftStorage, err := createAndOpenStorage(raftPath, "raft", rOpts)
	if err != nil {
		log.Error(fmt.Sprintf("raft::server::NewRaftServer; error in creating raft storage: %v", err))
		return nil, err
	}

	sOpts := &storage.Options{
		CreateIfNotExist: true,
	}
	kvStorage, err := createAndOpenKVStorage(kvPath, "kv", txnComp, sOpts)
	if err != nil {
		log.Error(fmt.Sprintf("raft::server::NewRaftServer; error in creating kv storage: %v", err))
		return nil, err
	}

	kvMetaStorage, err := createAndOpenStorage(kvMetaPath, "kvmeta", sOpts)
	if err != nil {
		log.Error(fmt.Sprintf("raft::server::NewRaftServer; error in creating kv meta storage: %v", err))
		return nil, err
	}

	mu := new(sync.Mutex)
	s := &Server{
		id:                kvConfig.ID,
		mu:                mu,
		kvStorage:         kvStorage,
		kvMetaStorage:     kvMetaStorage,
		kvConfig:          kvConfig,
		clientConnections: common.NewProtectedMapUConn(),
		Th: &TestHelpers{
			Drop: make(map[uint64]bool),
		},
	}

	raft := NewRaft(kvConfig, raftStorage, s, ready)
	s.raft = raft

	log.Info("raft::server::NewRaftServer; done")
	return s, nil
}
