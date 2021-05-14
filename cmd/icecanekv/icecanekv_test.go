package main

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/dr0pdb/icecanedb/pkg/common"
	"github.com/dr0pdb/icecanedb/pkg/icecanekv"
	icecanedbpb "github.com/dr0pdb/icecanedb/pkg/protogen/icecanedbpb"
	"github.com/dr0pdb/icecanedb/pkg/raft"
	"github.com/dr0pdb/icecanedb/test"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
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

	testDirectory = "/tmp/icecanetesting/"
)

type icecanekvTestHarness struct {
	configs     []*common.KVConfig
	grpcServers []*grpc.Server
	kvServers   []*icecanekv.KVServer
	connected   map[uint64]bool
}

func newIcecaneKVTestHarness() *icecanekvTestHarness {
	var kvServers []*icecanekv.KVServer
	var grpcServers []*grpc.Server
	var configs []*common.KVConfig
	connected := make(map[uint64]bool)

	// setup each grpc server
	for i := uint64(1); i <= 5; i++ {
		config := &common.KVConfig{
			ID:         i,
			DbPath:     fmt.Sprintf("%s/%d", testDirectory, i),
			Address:    "127.0.0.1",
			Port:       fmt.Sprint(9000 + i),
			LogMVCC:    false,
			LogRaft:    true,
			LogStorage: false,
		}

		var p []common.Peer
		for j := uint64(1); j <= 5; j++ {
			if i != j {
				p = append(p, peers[j-1])
			}
		}
		config.Peers = p

		os.RemoveAll(fmt.Sprintf("%s/%d", testDirectory, i))

		server, err := icecanekv.NewKVServer(config)
		if err != nil {
			log.Fatalf("%V", err)
		}

		var alivePolicy = keepalive.EnforcementPolicy{
			MinTime:             2 * time.Second, // If a client pings more than once every 2 seconds, terminate the connection
			PermitWithoutStream: true,            // Allow pings even when there are no active streams
		}

		grpcServer := grpc.NewServer(
			grpc.KeepaliveEnforcementPolicy(alivePolicy),
			grpc.InitialWindowSize(1<<30),
			grpc.InitialConnWindowSize(1<<30),
			grpc.MaxRecvMsgSize(10*1024*1024),
		)

		icecanedbpb.RegisterIcecaneKVServer(grpcServer, server)
		reflection.Register(grpcServer) // Register reflection service on gRPC server.

		configs = append(configs, config)
		kvServers = append(kvServers, server)
		grpcServers = append(grpcServers, grpcServer)
		connected[i] = true
	}

	// start the servers parallely
	for i := 0; i < 5; i++ {
		go func(grpcServer *grpc.Server, config *common.KVConfig) {
			listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%s", config.Port))
			if err != nil {
				log.Fatalf("%V", err)
			}

			err = grpcServer.Serve(listener)
			if err != nil {
				log.Fatal(err)
			}
		}(grpcServers[i], configs[i])
	}

	return &icecanekvTestHarness{
		configs:     configs,
		kvServers:   kvServers,
		grpcServers: grpcServers,
		connected:   connected,
	}
}

func (s *icecanekvTestHarness) teardown() {
	for i := 0; i < 5; i++ {
		s.kvServers[i].Close()
		s.grpcServers[i].Stop()
		os.RemoveAll(fmt.Sprintf("%s/%d", testDirectory, i+1))
	}
}

func (s *icecanekvTestHarness) disconnectPeer(id uint64) {
	// calls to id should be dropped
	for i := uint64(1); i <= 5; i++ {
		s.kvServers[i-1].RaftServer.Th.Drop[id] = true
	}

	// from id: every call should be dropped
	for i := uint64(1); i <= 5; i++ {
		s.kvServers[id-1].RaftServer.Th.Drop[i] = true
	}

	s.connected[id] = false
}

// checks if there exists a single leader in the raft cluster
// retries 8 times
func (s *icecanekvTestHarness) checkSingleLeader(t *testing.T) (uint64, uint64) {
	for attempt := 0; attempt < 8; attempt++ {
		leaderId := uint64(0)
		leaderTerm := uint64(0)

		for i := uint64(1); i <= 5; i++ {
			// a disconnected could be lagging behind
			if !s.connected[i] {
				continue
			}

			diag := s.kvServers[i-1].RaftServer.GetDiagnosticInformation()

			if diag.Role == raft.Leader {
				if leaderId == 0 {
					leaderId = i
					leaderTerm = diag.Term
				} else {
					t.Errorf("%d and %d both claim to be the leader", leaderId, i)
				}
			}
		}

		if leaderId != 0 {
			return leaderId, leaderTerm
		}

		time.Sleep(150 * time.Millisecond)
	}

	t.Errorf("no leader found")
	return 0, 0
}

func TestRaftElectionBasic(t *testing.T) {
	th := newIcecaneKVTestHarness()
	defer th.teardown()

	th.checkSingleLeader(t)
}

// without network partitions, leader and it's term should remain same
func TestRaftElectionStability(t *testing.T) {
	th := newIcecaneKVTestHarness()
	defer th.teardown()

	leaderID, leaderTerm := th.checkSingleLeader(t)

	for i := 0; i < 15; i++ {
		time.Sleep(time.Second)
		newLeaderID, newLeaderTerm := th.checkSingleLeader(t)
		assert.Equal(t, leaderID, newLeaderID, "leaderid is not equal to newLeader id")
		assert.Equal(t, leaderTerm, newLeaderTerm, "leader term is not equal to new leader term")
	}
}

func TestRaftElectionLeaderDisconnectBasic(t *testing.T) {
	th := newIcecaneKVTestHarness()
	defer th.teardown()

	leaderID, leaderTerm := th.checkSingleLeader(t)
	th.disconnectPeer(leaderID)

	time.Sleep(2 * time.Second)

	newLeaderID, newLeaderTerm := th.checkSingleLeader(t)
	assert.NotEqual(t, newLeaderID, leaderID, fmt.Sprintf("error: newLeaderID still same as previous leaderID. newLeaderID: %d, leaderID: %d", newLeaderID, leaderID))
	assert.Greater(t, newLeaderTerm, leaderTerm, "error: newLeaderTerm <= leaderTerm.")
}

func TestRaftLeaderWriteSucceeds(t *testing.T) {
	th := newIcecaneKVTestHarness()
	defer th.teardown()

	leaderId, _ := th.checkSingleLeader(t)

	// should be inserted at idx 1
	success, err := th.kvServers[leaderId-1].RaftServer.SetValue(test.TestKeys[0], test.TestValues[0], false)
	assert.Nil(t, err, "Unexpected error while writing to leader")
	assert.True(t, success, "Unexpected failure in writing to leader")

	// changes should be visible on the leader instantly
	rl := th.kvServers[leaderId-1].RaftServer.GetLogAtIndex(1)
	assert.Equal(t, test.TestKeys[0], rl.Key, "leader: saved key and returned key from log is not same")
	assert.Equal(t, test.TestValues[0], rl.Value, "leader: saved key and returned key from log is not same")

	time.Sleep(2 * time.Second)

	// check on each node
	for i := 0; i < 5; i++ {
		rl2 := th.kvServers[i].RaftServer.GetLogAtIndex(1)
		assert.Equal(t, test.TestKeys[0], rl2.Key, fmt.Sprintf("On id: %d saved key and returned key from log is not same", i+1))
		assert.Equal(t, test.TestValues[0], rl2.Value, fmt.Sprintf("On id: %d saved key and returned key from log is not same", i+1))
	}
}
