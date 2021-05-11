package main

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/dr0pdb/icecanedb/pkg/common"
	"github.com/dr0pdb/icecanedb/pkg/icecanekv"
	icecanedbpb "github.com/dr0pdb/icecanedb/pkg/protogen/icecanedbpb"
	"github.com/dr0pdb/icecanedb/pkg/raft"
	log "github.com/sirupsen/logrus"
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
)

type icecanekvTestHarness struct {
	configs     []*common.KVConfig
	grpcServers []*grpc.Server
	kvServers   []*icecanekv.KVServer
}

func newRaftServerTestHarness() *icecanekvTestHarness {
	var kvServers []*icecanekv.KVServer
	var grpcServers []*grpc.Server
	var configs []*common.KVConfig

	// setup each grpc server
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
	}
}

func (s *icecanekvTestHarness) teardown() {
	for i := 0; i < 5; i++ {
		s.kvServers[i].Close()
		s.grpcServers[i].Stop()
	}
}

// checks if there exists a single leader in the raft cluster
// retries 8 times
func (s *icecanekvTestHarness) checkSingleLeader(t *testing.T) (uint64, uint64) {
	for attempt := 0; attempt < 8; attempt++ {
		leaderId := uint64(0)
		leaderTerm := uint64(0)

		for i := uint64(1); i <= 5; i++ {
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

		time.Sleep(time.Second)
	}

	t.Errorf("no leader found")
	return 0, 0
}

func TestRaftBasic(t *testing.T) {
	th := newRaftServerTestHarness()
	defer th.teardown()

	th.checkSingleLeader(t)
}
