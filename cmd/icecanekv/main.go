package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dr0pdb/icecanedb/pkg/common"
	"github.com/dr0pdb/icecanedb/pkg/icecanekv"
	icecanedbpb "github.com/dr0pdb/icecanedb/pkg/protogen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	log "github.com/sirupsen/logrus"
)

func main() {
	configFilePath := os.Getenv("ICECANEKV_CONFIG_FILE")
	conf := common.NewDefaultKVConfig()

	if configFilePath != "" {
		conf.LoadFromFile(configFilePath)
	}
	err := conf.Validate()
	if err != nil {
		log.Fatalf("%V", err)
	}

	server, err := icecanekv.NewKVServer(conf)
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
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%s", conf.Port))
	if err != nil {
		log.Fatalf("%V", err)
	}

	subSignal(grpcServer)

	err = grpcServer.Serve(listener)
	if err != nil {
		log.Fatal(err)
	}
	log.Info("Server stopped.")
}

// https://gobyexample.com/signals
func subSignal(grpcServer *grpc.Server) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Infof("Exiting server")
		grpcServer.Stop()
	}()
}
