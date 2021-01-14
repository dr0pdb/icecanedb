package icecanekv

import (
	"os"
	"path/filepath"

	"github.com/dr0pdb/icecanedb/pkg/storage"
	log "github.com/sirupsen/logrus"
)

// KVServer is the Key-value server that receives and processes requests from clients.
type KVServer struct {
	// stores raft logs
	raftStorage *storage.Storage

	// stores actual key-value data
	kvStorage *storage.Storage
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

	log.Info("icecanekv::icecanekv::NewKVServer; done")
	return &KVServer{
		raftStorage: raftStorage,
		kvStorage:   kvStorage,
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
