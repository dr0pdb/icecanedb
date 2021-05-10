package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dr0pdb/icecanedb/pkg/common"
)

var (
	peers []common.Peer = []common.Peer{
		{
			ID:      1,
			Address: "127.0.0.1",
			Port:    "9001",
		},
		{
			ID:      2,
			Address: "127.0.0.1",
			Port:    "9002",
		},
		{
			ID:      3,
			Address: "127.0.0.1",
			Port:    "9003",
		},
		{
			ID:      4,
			Address: "127.0.0.1",
			Port:    "9004",
		},
		{
			ID:      5,
			Address: "127.0.0.1",
			Port:    "9005",
		},
	}
)

type raftServerTestHarness struct {
	servers []*Server
}

func newRaftServerTestHarness() *raftServerTestHarness {
	var servers []*Server

	ready := make([]chan interface{}, 5)

	for i := uint64(1); i <= 5; i++ {
		config := &common.KVConfig{
			ID:       i,
			DbPath:   fmt.Sprintf("/tmp/icecanetesting/%d", i),
			LogLevel: "info",
			Address:  "127.0.0.1",
			Port:     fmt.Sprint(9000 + i),
		}

		var p []common.Peer
		for j := uint64(0); j < 5; j++ {
			if i != j {
				p = append(p, peers[j])
			}
		}
		config.Peers = p

		kvPath := filepath.Join(config.DbPath, "kv")
		kvMetaPath := filepath.Join(config.DbPath, "kvmeta")
		raftPath := filepath.Join(config.DbPath, "raft")
		os.MkdirAll(kvPath, os.ModePerm)
		os.MkdirAll(kvMetaPath, os.ModePerm)
		os.MkdirAll(raftPath, os.ModePerm)

		ready[i-1] = make(chan interface{})
		server, _ := NewRaftServer(config, raftPath, kvPath, kvMetaPath, nil, ready[i-1])
		servers = append(servers, server)
	}

	for i := 0; i < 5; i++ {
		ready[i] <- struct{}{}
	}

	return &raftServerTestHarness{
		servers: servers,
	}
}

func (s *raftServerTestHarness) teardown() {
	for i := 0; i < 5; i++ {
		s.servers[i].Close()
	}
}

// checks if there exists a single leader in the raft cluster
// retries 8 times
func (s *raftServerTestHarness) checkSingleLeader(t *testing.T) (uint64, uint64) {
	for attempt := 0; attempt < 8; attempt++ {
		leaderId := uint64(0)
		leaderTerm := uint64(0)

		for i := uint64(1); i <= 5; i++ {
			term, role := s.servers[i-1].raft.getNodeState()

			if role == leader {
				if leaderId == 0 {
					leaderId = i
					leaderTerm = term
				} else {
					t.Errorf("%d and %d both claim to be the leader", leaderId, i)
				}
			}
		}

		if leaderId != 0 {
			return leaderId, leaderTerm
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Errorf("no leader found")
	return 0, 0
}

func TestRaftBasic(t *testing.T) {
	th := newRaftServerTestHarness()
	defer th.teardown()

	th.checkSingleLeader(t)
}
