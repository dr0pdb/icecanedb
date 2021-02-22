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
	"github.com/dr0pdb/icecanedb/pkg/storage"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
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

	// kvConfig is the config
	kvConfig *common.KVConfig

	// clientConnections contains the grpc client connections made with other raft peers.
	// key: id of the peer.
	clientConnections map[uint64]*grpc.ClientConn
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

//
// kv server utility functions
//

// getOrCreateClientConnection gets or creates a grpc client connection for talking to peer with given id.
// In the case of creation, it caches it the clientConnections map
func (kvs *KVServer) getOrCreateClientConnection(id uint64) (*grpc.ClientConn, error) {
	if conn, ok := kvs.clientConnections[id]; ok {
		return conn, nil
	}
	var p *common.Peer = nil

	for _, peer := range kvs.kvConfig.Peers {
		if peer.ID == id {
			p = &peer
			break
		}
	}

	if p == nil {
		return nil, fmt.Errorf("invalid peer id %d", id)
	}

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	opts = append(opts, grpc.WithBlock())
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", p.Address, p.Port), opts...)
	if err != nil {
		return nil, err
	}
	kvs.clientConnections[id] = conn
	return conn, nil
}

// Close cleans up the kv server
func (kvs *KVServer) Close() {
	log.Info("icecanekv::icecanekv::Close; started")

	for _, conn := range kvs.clientConnections {
		defer conn.Close()
	}

	log.Info("icecanekv::icecanekv::Close; done")
}

// NewKVServer creates a new instance of KV Server
func NewKVServer(kvConfig *common.KVConfig) (*KVServer, error) {
	log.Info("icecanekv::icecanekv::NewKVServer; started")
	raftPath, kvPath, err := prepareDirectories(kvConfig)
	if err != nil {
		return nil, err
	}

	rOpts := &storage.Options{
		CreateIfNotExist: true,
	}
	raftStorage, err := createAndOpenStorage(raftPath, rOpts)
	if err != nil {
		log.Error(fmt.Sprintf("icecanekv::icecanekv::NewKVServer; error %V in creating raft storage", err))
		return nil, err
	}

	sOpts := &storage.Options{
		CreateIfNotExist: true,
	}
	kvStorage, err := createAndOpenStorage(kvPath, sOpts)
	if err != nil {
		log.Error(fmt.Sprintf("icecanekv::icecanekv::NewKVServer; error %V in creating kv storage", err))
		return nil, err
	}

	kvMvcc := mvcc.NewMVCC(kvStorage)
	raftServer := raft.NewRaftServer(kvConfig, raftStorage, kvStorage, kvMvcc)

	log.Info("icecanekv::icecanekv::NewKVServer; done")
	return &KVServer{
		raftStorage:       raftStorage,
		raftPath:          raftPath,
		kvStorage:         kvStorage,
		kvPath:            kvPath,
		kvMvcc:            kvMvcc,
		raftServer:        raftServer,
		kvConfig:          kvConfig,
		clientConnections: make(map[uint64]*grpc.ClientConn),
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

// createAndOpenStorage creates a storage and opens it.
func createAndOpenStorage(path string, opts *storage.Options) (*storage.Storage, error) {
	s, err := storage.NewStorage(path, opts)
	if err != nil {
		return nil, err
	}
	err = s.Open()
	return s, err
}
