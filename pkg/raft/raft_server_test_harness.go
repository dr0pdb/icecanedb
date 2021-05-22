package raft

import (
	"context"
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
func (t *TestRaftServer) Scan(target []byte) (storage.Iterator, bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.IsLeader {
		return nil, false, nil
	}

	it := t.storage.Scan(target)
	return it, true, nil
}

// SetValue sets the value of the key and gets it replicated across peers
func (t *TestRaftServer) SetValue(key, value []byte, meta bool) (bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.IsLeader {
		return false, nil
	}

	var err error
	if meta {
		err = t.metaStorage.Set(key, value, &storage.WriteOptions{Sync: true})
	} else {
		err = t.storage.Set(key, value, &storage.WriteOptions{Sync: true})
	}

	return err == nil, err
}

// DeleteValue deletes the value of the key and gets it replicated across peers
func (t *TestRaftServer) DeleteValue(key []byte, meta bool) (bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.IsLeader {
		return false, nil
	}

	var err error
	if meta {
		err = t.metaStorage.Delete(key, &storage.WriteOptions{Sync: true})
	} else {
		err = t.storage.Delete(key, &storage.WriteOptions{Sync: true})
	}

	return err == nil, err
}

// MetaGetValue returns the value of the key from meta storage layer.
func (t *TestRaftServer) MetaGetValue(key []byte) ([]byte, bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.IsLeader {
		return nil, false, nil
	}

	val, err := t.metaStorage.Get(key, nil)
	return val, true, err
}

// MetaScan returns an iterator to iterate over all the kv pairs whose key >= target
func (t *TestRaftServer) MetaScan(target []byte) (storage.Iterator, bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.IsLeader {
		return nil, false, nil
	}

	it := t.metaStorage.Scan(target)
	return it, true, nil
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
