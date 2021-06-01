/**
 * Copyright 2021 The IcecaneDB Authors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package raft

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	common "github.com/dr0pdb/icecanedb/pkg/common"
	pb "github.com/dr0pdb/icecanedb/pkg/protogen/icecanedbpb"
	"github.com/dr0pdb/icecanedb/pkg/storage"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// IcecaneRaftServer is the raft server interface used by the upper layers
type IcecaneRaftServer interface {
	// RequestVote is used by the raft candidate to request for votes.
	RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)

	// AppendEntries is invoked by leader to replicate log entries; also used as heartbeat
	AppendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)

	// PeerSet is invoked by a raft peer to set the kv if this node is a leader
	PeerSet(ctx context.Context, req *pb.PeerSetRequest) (*pb.PeerSetResponse, error)

	// PeerDelete is invoked by a raft peer to delete a kv if this node is a leader
	PeerDelete(ctx context.Context, req *pb.PeerDeleteRequest) (*pb.PeerDeleteResponse, error)

	// ScanValues returns an iterator to iterate over all the kv pairs whose key >= target
	ScanValues(target []byte) (storage.Iterator, error)

	// SetValue sets the value of the key and gets it replicated across peers
	SetValue(key, value []byte, meta bool) error

	// DeleteValue deletes the value of the key and gets it replicated across peers
	DeleteValue(key []byte, meta bool) error

	// MetaGetValue returns the value of the key from meta storage layer.
	MetaGetValue(key []byte) ([]byte, error)

	// MetaScan returns an iterator to iterate over all the kv pairs whose key >= target
	MetaScan(target []byte) (storage.Iterator, error)

	// GetDiagnosticInformation returns the diag state of the raft server
	GetDiagnosticInformation() *DiagInfo

	// GetLogAtIndex returns the raft log present at the given index
	// Returns default logs if nothing is found
	GetLogAtIndex(idx uint64) *RaftLog

	// Close cleanups the underlying resources of the raft server.
	Close()
}

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
	raftCommitIdx  uint64
	raftAppliedIdx uint64

	Th *TestHelpers
}

var _ IcecaneRaftServer = (*Server)(nil)

// TestHelpers contains fields that are used in testing.
type TestHelpers struct {
	// Drop the rpc calls if this is true
	Drop map[uint64]bool
}

type DiagInfo struct {
	Role State

	RaftCommitIdx  uint64
	RaftAppliedIdx uint64

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

// PeerSet is invoked by a raft peer to set the kv if this node is a leader
func (s *Server) PeerSet(ctx context.Context, req *pb.PeerSetRequest) (*pb.PeerSetResponse, error) {
	log.WithFields(log.Fields{"id": s.id, "req": req}).Info("raft::server::PeerSet; started")
	resp := &pb.PeerSetResponse{
		Success:  false,
		IsLeader: true,
	}

	_, _, role := s.raft.getNodeState()
	if role != Leader {
		resp.IsLeader = false
		return resp, nil
	}

	idx, err := s.raft.handleClientSetRequest(req.Key, req.Value, req.Meta)
	if err != nil {
		log.WithFields(log.Fields{"id": s.id}).Error(fmt.Sprintf("raft::server::PeerSet; error while setting. err: %+v", err))
		return nil, err
	}

	log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::PeerSet; waiting for idx %d to be committed and applied", idx))
	for {
		if s.raftAppliedIdx >= idx {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}

	// check to see if the entry committed is the same as we requested
	// this is required since the leader might have changed right after the set was done
	rl := s.GetLogAtIndex(idx)
	if (rl.Ct != SetCmd && rl.Ct != MetaSetCmd) || !bytes.Equal(rl.Key, req.Key) || !bytes.Equal(rl.Value, req.Value) {
		log.WithFields(log.Fields{"id": s.id}).Error(fmt.Sprintf("raft::server::PeerSet; different log committed at idx: %d", idx))
		return nil, fmt.Errorf("raft internal error: different log committed at the index")
	}

	log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::PeerSet; successfully committed at %d", idx))
	resp.Success = true
	return resp, nil
}

// PeerDelete is invoked by a raft peer to delete a kv if this node is a leader
func (s *Server) PeerDelete(ctx context.Context, req *pb.PeerDeleteRequest) (*pb.PeerDeleteResponse, error) {
	log.WithFields(log.Fields{"id": s.id, "req": req}).Info("raft::server::PeerDelete; started")
	resp := &pb.PeerDeleteResponse{
		Success:  false,
		IsLeader: true,
	}

	_, _, role := s.raft.getNodeState()
	if role != Leader {
		resp.IsLeader = false
		return resp, nil
	}

	idx, err := s.raft.handleClientDeleteRequest(req.Key, req.Meta)
	if err != nil {
		log.WithFields(log.Fields{"id": s.id}).Error(fmt.Sprintf("raft::server::PeerDelete; error while deleting. err: %+v", err))
		return nil, err
	}

	log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::PeerDelete; waiting for idx %d to be committed and applied", idx))
	for {
		if s.raftAppliedIdx >= idx {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}

	// check to see if the entry committed is the same as we requested
	// this is required since the leader might have changed right after the set was done
	rl := s.GetLogAtIndex(idx)
	if (rl.Ct != DeleteCmd && rl.Ct != MetaDeleteCmd) || !bytes.Equal(rl.Key, req.Key) {
		log.WithFields(log.Fields{"id": s.id}).Error(fmt.Sprintf("raft::server::PeerDelete; different log committed at idx: %d", idx))
		return nil, fmt.Errorf("raft internal error: different log committed at the index")
	}

	log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::PeerDelete; successfully committed at %d", idx))
	return resp, nil
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
}

//
// mvcc layer calls
//
//

// Scan returns an iterator to iterate over all the kv pairs whose key >= target
func (s *Server) ScanValues(target []byte) (storage.Iterator, error) {
	log.WithFields(log.Fields{"id": s.id}).Info("raft::server::ScanValues; started")

	// TODO: Ensure that we have the latest values (once we move to async application of raft logs)
	// wait for it if it's not

	itr := s.kvStorage.Scan(target)
	return itr, nil
}

// SetValue sets the value of the key and gets it replicated across peers
func (s *Server) SetValue(key, value []byte, meta bool) error {
	log.WithFields(log.Fields{"id": s.id}).Info("raft::server::SetValue; started")

	_, _, role := s.raft.getNodeState()
	if role == Leader {
		idx, err := s.raft.handleClientSetRequest(key, value, meta)
		if err != nil {
			log.WithFields(log.Fields{"id": s.id}).Error(fmt.Sprintf("raft::server::SetValue; error while setting. err: %+v", err))
			return err
		}

		log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::SetValue; waiting for idx %d to be committed and applied", idx))
		for {
			if s.raftAppliedIdx >= idx {
				break
			}

			time.Sleep(10 * time.Millisecond)
		}

		// check to see if the entry committed is the same as we requested
		// this is required since the leader might have changed right after the set was done
		rl := s.GetLogAtIndex(idx)
		if (rl.Ct != SetCmd && rl.Ct != MetaSetCmd) || !bytes.Equal(rl.Key, key) || !bytes.Equal(rl.Value, value) {
			log.WithFields(log.Fields{"id": s.id}).Error(fmt.Sprintf("raft::server::SetValue; different log committed at idx: %d", idx))
			return fmt.Errorf("raft internal error: different log committed at the index")
		}

		log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::SetValue; successfully committed at %d", idx))
	} else {
		success := false

		// call raft peer on behalf of the client
		for i := range s.kvConfig.Peers {
			p := s.kvConfig.Peers[i]
			isLeader, err := s.sendPeerSetRequest(p.ID, key, value, meta)
			if isLeader {
				if err != nil {
					return err
				}

				success = true
				break
			}
		}

		if !success {
			return fmt.Errorf("unsuccessful write")
		}
	}

	return nil
}

// DeleteValue deletes the value of the key and gets it replicated across peers
func (s *Server) DeleteValue(key []byte, meta bool) error {
	log.WithFields(log.Fields{"id": s.id}).Info("raft::server::DeleteValue; started")

	_, _, role := s.raft.getNodeState()
	if role == Leader {
		idx, err := s.raft.handleClientDeleteRequest(key, meta)
		if err != nil {
			log.WithFields(log.Fields{"id": s.id}).Error(fmt.Sprintf("raft::server::DeleteValue; error while deleting. err: %+v", err))
			return err
		}

		log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::DeleteValue; waiting for idx %d to be committed and applied", idx))
		for {
			if s.raftAppliedIdx >= idx {
				break
			}

			time.Sleep(10 * time.Millisecond)
		}

		// check to see if the entry committed is the same as we requested
		// this is required since the leader might have changed right after the set was done
		rl := s.GetLogAtIndex(idx)
		if (rl.Ct != DeleteCmd && rl.Ct != MetaDeleteCmd) || !bytes.Equal(rl.Key, key) {
			log.WithFields(log.Fields{"id": s.id}).Error(fmt.Sprintf("raft::server::DeleteValue; different log committed at idx: %d", idx))
			return fmt.Errorf("raft internal error: different log committed at the index")
		}

		log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::DeleteValue; successfully committed at %d", idx))
	} else {
		success := false

		// call raft peer on behalf of the client
		for i := range s.kvConfig.Peers {
			p := s.kvConfig.Peers[i]
			isLeader, err := s.sendPeerDeleteRequest(p.ID, key, meta)
			if isLeader {
				if err != nil {
					return err
				}
				success = true
				break
			}
		}

		if !success {
			return fmt.Errorf("unsuccessful write")
		}
	}

	return nil
}

// MetaGetValue returns the value of the key from meta storage layer.
func (s *Server) MetaGetValue(key []byte) ([]byte, error) {
	log.WithFields(log.Fields{"id": s.id}).Info("raft::server::MetaGetValue; started")

	// TODO: Ensure that we have the latest values (once we move to async application of raft logs)
	// wait for it if it's not

	val, err := s.kvMetaStorage.Get(key, nil)
	return val, err
}

// MetaScan returns an iterator to iterate over all the kv pairs whose key >= target
func (s *Server) MetaScan(target []byte) (storage.Iterator, error) {
	log.WithFields(log.Fields{"id": s.id}).Info("raft::server::MetaScan; started")

	// TODO: Ensure that we have the latest values (once we move to async application of raft logs)
	// wait for it if it's not

	itr := s.kvMetaStorage.Scan(target)
	return itr, nil
}

// GetLogAtIndex returns the raft log present at the given index
// Returns default logs if nothing is found
func (s *Server) GetLogAtIndex(idx uint64) *RaftLog {
	return s.raft.getLogEntryOrDefault(idx)
}

//
// Diagnostic calls
// For testing and debugging
//

// GetDiagnosticInformation returns the diag state of the raft server
func (s *Server) GetDiagnosticInformation() *DiagInfo {
	term, commitIdx, role := s.raft.getNodeState()

	return &DiagInfo{
		Role:           role,
		RaftCommitIdx:  commitIdx,
		Term:           term,
		RaftAppliedIdx: s.raftAppliedIdx,
	}
}

func (s *Server) sendPeerSetRequest(peerID uint64, key, value []byte, meta bool) (bool, error) {
	log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::sendPeerSetRequest; sending set request to peer %d", peerID))

	if drop, ok := s.Th.Drop[peerID]; ok && drop {
		return true, fmt.Errorf("dropping request for testing")
	}

	conn, err := s.getOrCreateClientConnection(peerID)
	if err != nil {
		log.Error(fmt.Sprintf("raft::server::sendPeerSetRequest; error in getting conn: %v", err))
		return true, err
	}

	client := pb.NewIcecaneKVClient(conn)
	request := &pb.PeerSetRequest{
		Key:   key,
		Value: value,
		Meta:  meta,
	}
	resp, err := client.PeerSet(context.Background(), request)
	if err != nil {
		log.Error(fmt.Sprintf("raft::server::sendPeerSetRequest; error in grpc request: %v", err))
		return true, err
	}
	if !resp.IsLeader {
		return false, nil
	}
	if !resp.Success {
		return true, fmt.Errorf("unsuccessful write at %d", peerID)
	}

	return true, err
}

func (s *Server) sendPeerDeleteRequest(peerID uint64, key []byte, meta bool) (bool, error) {
	log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::sendPeerDeleteRequest; sending set request to peer %d", peerID))

	if drop, ok := s.Th.Drop[peerID]; ok && drop {
		return true, fmt.Errorf("dropping request for testing")
	}

	conn, err := s.getOrCreateClientConnection(peerID)
	if err != nil {
		log.Error(fmt.Sprintf("raft::server::sendPeerDeleteRequest; error in getting conn: %v", err))
		return true, err
	}

	client := pb.NewIcecaneKVClient(conn)
	request := &pb.PeerDeleteRequest{
		Key:  key,
		Meta: meta,
	}
	resp, err := client.PeerDelete(context.Background(), request)
	if err != nil {
		log.Error(fmt.Sprintf("raft::server::sendPeerDeleteRequest; error in grpc request: %v", err))
		return true, err
	}
	if !resp.IsLeader {
		return false, nil
	}
	if !resp.Success {
		return true, fmt.Errorf("unsuccessful write")
	}

	return true, err
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
		s.mu.Lock()
		s.raftAppliedIdx = logIndex
		s.mu.Unlock()
		log.WithFields(log.Fields{"id": s.id}).Info("raft::server::applyEntry; done")
	}

	return err
}

// updateRaftIdx updates the raft index in server
func (s *Server) updateRaftIdx(idx uint64) {
	log.WithFields(log.Fields{"id": s.id}).Info(fmt.Sprintf("raft::server::updateRaftIdx; updating raftLogIndex from %d to %d", s.raftCommitIdx, idx))
	s.mu.Lock()
	if idx > s.raftCommitIdx {
		s.raftCommitIdx = idx
	}
	s.mu.Unlock()
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

	return s, nil
}
