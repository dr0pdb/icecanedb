package icecanekv

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dr0pdb/icecanedb/pkg/common"
	"github.com/dr0pdb/icecanedb/pkg/mvcc"
	pb "github.com/dr0pdb/icecanedb/pkg/protogen"
	"github.com/dr0pdb/icecanedb/pkg/raft"
	log "github.com/sirupsen/logrus"
	codes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// KVServer is the Key-value server that receives and processes requests from clients.
// Presently, it forwards all the requests to raft.Server
// TODO: A lot of these fields may not be required. For eg. We won't be using kvMvcc directly but only via raftServer.
type KVServer struct {
	pb.UnimplementedIcecaneKVServer

	// raft storage path
	raftPath string

	// the raft server
	raftServer *raft.Server

	// the key-value storage path
	kvPath string

	// the mvcc layer for the key-value data
	kvMvcc *mvcc.MVCC

	// kvConfig is the config
	kvConfig *common.KVConfig
}

//
// grpc server calls - incoming to this server from other peers/client
//

// RequestVote is used by the raft candidate to request for votes. The current server has to respond to this req by casting vote or decling.
func (kvs *KVServer) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return kvs.raftServer.RequestVote(ctx, request)
}

// AppendEntries is invoked by leader to replicate log entries; also used as heartbeat
func (kvs *KVServer) AppendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return kvs.raftServer.AppendEntries(ctx, request)
}

// Get gets the value of a key.
func (kvs *KVServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RawGet not implemented")
}

// Set sets the value of a key.
func (kvs *KVServer) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RawGet not implemented")
}

// Delete deletes the value of a key.
func (kvs *KVServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RawGet not implemented")
}

// BeginTxn begins a MVCC transaction providing ACID guarantees.
func (kvs *KVServer) BeginTxn(ctx context.Context, req *pb.BeginTxnRequest) (*pb.BeginTxnResponse, error) {
	return kvs.kvMvcc.BeginTxn(ctx, req)
}

// CommitTxn attempts to commit a MVCC txn
func (kvs *KVServer) CommitTxn(ctx context.Context, req *pb.CommitTxnRequest) (*pb.CommitTxnResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RawGet not implemented")
}

// RollbackTxn rollsback a MVCC txn
func (kvs *KVServer) RollbackTxn(ctx context.Context, req *pb.RollbackTxnRequest) (*pb.RollbackTxnResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RawGet not implemented")
}

// Close cleans up the kv server
func (kvs *KVServer) Close() {
	log.Info("icecanekv::icecanekv::Close; started")
	kvs.raftServer.Close()
	log.Info("icecanekv::icecanekv::Close; done")
}

// NewKVServer creates a new instance of KV Server
func NewKVServer(kvConfig *common.KVConfig) (*KVServer, error) {
	log.Info("icecanekv::icecanekv::NewKVServer; started")
	raftPath, kvPath, err := prepareDirectories(kvConfig)
	if err != nil {
		return nil, err
	}

	txnComp := mvcc.NewtxnKeyComparator()
	raftServer, err := raft.NewRaftServer(kvConfig, raftPath, kvPath, txnComp)
	if err != nil {
		log.Error(fmt.Sprintf("icecanekv::icecanekv::NewKVServer; error in creating raft server: %v", err))
		return nil, err
	}

	kvMvcc := mvcc.NewMVCC(raftServer)
	log.Info("icecanekv::icecanekv::NewKVServer; done")
	return &KVServer{
		raftPath:   raftPath,
		kvPath:     kvPath,
		kvMvcc:     kvMvcc,
		raftServer: raftServer,
		kvConfig:   kvConfig,
	}, nil
}

//
// static utility functions
//

// prepareDirectories creates relevant kv server directories inside the db path.
func prepareDirectories(kvConfig *common.KVConfig) (string, string, error) {
	log.Info("icecanekv::icecanekv::prepareDirectories; started")
	dbPath := kvConfig.DbPath
	kvPath := filepath.Join(dbPath, "kv")
	raftPath := filepath.Join(dbPath, "raft")

	err := os.MkdirAll(kvPath, os.ModePerm)
	if err != nil {
		return "", "", err
	}
	err = os.MkdirAll(raftPath, os.ModePerm)
	if err != nil {
		return "", "", err
	}

	log.Info("icecanekv::icecanekv::prepareDirectories; done")
	return kvPath, raftPath, err
}
