package icecanekv

import (
	"context"
	"os"
	"path/filepath"

	"github.com/dr0pdb/icecanedb/pkg/mvcc"
	pb "github.com/dr0pdb/icecanedb/pkg/protogen"
	"github.com/dr0pdb/icecanedb/pkg/raft"
	"github.com/dr0pdb/icecanedb/pkg/storage"
	log "github.com/sirupsen/logrus"
	codes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// KVServer is the Key-value server that receives and processes requests from clients.
// Presently, it forwards all the requests to raft.Server
// TODO: A lot of these fields may not be required. For eg. We won't be using kvMvcc directly but only via raftServer.
type KVServer struct {
	pb.UnimplementedIcecaneKVServer

	// stores raft logs
	raftStorage *storage.Storage
	raftPath    string

	// the raft server
	raftServer *raft.Server

	// stores actual key-value data
	kvStorage *storage.Storage
	kvPath    string

	// the mvcc layer for the key-value data
	kvMvcc *mvcc.MVCC
}

//
// grpc server calls - incoming to this server from other peers/client
//

// RawGet returns the value associated with the given key.
func (kvs *KVServer) RawGet(context.Context, *pb.RawGetRequest) (*pb.RawGetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RawGet not implemented")
}

// RawPut puts the value in the db for the given key.
func (kvs *KVServer) RawPut(context.Context, *pb.RawPutRequest) (*pb.RawPutResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RawPut not implemented")
}

// RawDelete deletes the value in the db for the given key.
func (kvs *KVServer) RawDelete(context.Context, *pb.RawDeleteRequest) (*pb.RawDeleteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RawDelete not implemented")
}

// RequestVote is used by the raft candidate to request for votes. The current server has to respond to this req by casting vote or decling.
func (kvs *KVServer) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return kvs.raftServer.RequestVote(ctx, request)
}

//
// grpc client calls - outgoing from this server to other peers
//

// SendRequestVote is used by the raft candidate to send the request for votes to other peers.
func (kvs *KVServer) SendRequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SendRequestVote not implemented")
}

// NewKVServer creates a new instance of KV Server
func NewKVServer(kvConfig *KVConfig) (*KVServer, error) {
	log.Info("icecanekv::icecanekv::NewKVServer; started")
	raftPath, kvPath, err := prepareDirectories(kvConfig)
	if err != nil {
		return nil, err
	}

	raftStorage, err := storage.NewStorage(raftPath, nil)
	if err != nil {
		return nil, err
	}

	kvStorage, err := storage.NewStorage(kvPath, nil)
	if err != nil {
		return nil, err
	}

	kvMvcc := mvcc.NewMVCC(kvStorage)
	raftServer := raft.NewRaftServer(kvConfig.ID, raftStorage, kvStorage, kvMvcc)

	log.Info("icecanekv::icecanekv::NewKVServer; done")
	return &KVServer{
		raftStorage: raftStorage,
		raftPath:    raftPath,
		kvStorage:   kvStorage,
		kvPath:      kvPath,
		kvMvcc:      kvMvcc,
		raftServer:  raftServer,
	}, nil
}

// prepareDirectories creates relevant kv server directories inside the db path.
func prepareDirectories(kvConfig *KVConfig) (string, string, error) {
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
