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
	"context"
	"fmt"
	"os"
	"sync"

	pb "github.com/dr0pdb/icecanedb/pkg/protogen/icecanedbpb"
	"github.com/dr0pdb/icecanedb/pkg/storage"
)

// TestRaftServer mimics the interface of a RaftServer but doesn't do any replication.
// It directs stores the value in local storage.
// thread safe
type TestRaftServer struct {
	storage, metaStorage *storage.Storage
	testDirectory        string
	IsLeader             bool
	mu                   *sync.Mutex
}

var _ IcecaneRaftServer = (*TestRaftServer)(nil)

func NewTestRaftServer(testDirectory string, isLeader bool, txnComp storage.Comparator) *TestRaftServer {
	t := &TestRaftServer{
		testDirectory: testDirectory,
		IsLeader:      isLeader,
		mu:            new(sync.Mutex),
	}

	s, _ := storage.NewStorageWithCustomComparator(testDirectory, "testdb", txnComp, &storage.Options{CreateIfNotExist: true})
	metas, _ := storage.NewStorage(testDirectory, "testdbmeta", &storage.Options{CreateIfNotExist: true})

	t.storage = s
	t.metaStorage = metas
	return t
}

func (t *TestRaftServer) Init() error {
	err := t.storage.Open()
	if err != nil {
		return err
	}

	err = t.metaStorage.Open()
	if err != nil {
		return err
	}

	return nil
}

// RequestVote is used by the raft candidate to request for votes.
func (t *TestRaftServer) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	panic("not implemented")
}

// AppendEntries is invoked by leader to replicate log entries; also used as heartbeat
func (t *TestRaftServer) AppendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	panic("not implemented")
}

// Scan returns an iterator to iterate over all the kv pairs whose key >= target
func (t *TestRaftServer) ScanValues(target []byte) (storage.Iterator, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.IsLeader {
		return nil, fmt.Errorf("not a leader")
	}

	it := t.storage.Scan(target)
	return it, nil
}

// SetValue sets the value of the key and gets it replicated across peers
func (t *TestRaftServer) SetValue(key, value []byte, meta bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.IsLeader {
		return fmt.Errorf("not a leader")
	}

	var err error
	if meta {
		err = t.metaStorage.Set(key, value, &storage.WriteOptions{Sync: true})
	} else {
		err = t.storage.Set(key, value, &storage.WriteOptions{Sync: true})
	}

	return err
}

// DeleteValue deletes the value of the key and gets it replicated across peers
func (t *TestRaftServer) DeleteValue(key []byte, meta bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.IsLeader {
		return fmt.Errorf("not a leader")
	}

	var err error
	if meta {
		err = t.metaStorage.Delete(key, &storage.WriteOptions{Sync: true})
	} else {
		err = t.storage.Delete(key, &storage.WriteOptions{Sync: true})
	}

	return err
}

// MetaGetValue returns the value of the key from meta storage layer.
func (t *TestRaftServer) MetaGetValue(key []byte) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	val, err := t.metaStorage.Get(key, nil)
	return val, err
}

// MetaScan returns an iterator to iterate over all the kv pairs whose key >= target
func (t *TestRaftServer) MetaScan(target []byte) (storage.Iterator, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	it := t.metaStorage.Scan(target)
	return it, nil
}

// GetDiagnosticInformation returns the diag state of the raft server
func (t *TestRaftServer) GetDiagnosticInformation() *DiagInfo {
	panic("not implemented")
}

// GetLogAtIndex returns the raft log present at the given index
// Returns default logs if nothing is found
func (t *TestRaftServer) GetLogAtIndex(idx uint64) *RaftLog {
	panic("not implemented")
}

// PeerSet is invoked by a raft peer to set the kv if this node is a leader
func (t *TestRaftServer) PeerSet(ctx context.Context, req *pb.PeerSetRequest) (*pb.PeerSetResponse, error) {
	panic("not implemented")
}

// PeerDelete is invoked by a raft peer to delete a kv if this node is a leader
func (t *TestRaftServer) PeerDelete(ctx context.Context, req *pb.PeerDeleteRequest) (*pb.PeerDeleteResponse, error) {
	panic("not implemented")
}

func (t *TestRaftServer) SetLeadership(isLeader bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.IsLeader = isLeader
}

// Close cleanups the underlying resources of the raft server.
func (t *TestRaftServer) Close() {
	t.storage.Close()
	t.metaStorage.Close()
	os.RemoveAll(t.testDirectory)
}
