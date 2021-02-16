package main

import (
	"flag"
	"time"

	"github.com/dr0pdb/icecanedb/pkg/icecanekv"
	icecanedbpb "github.com/dr0pdb/icecanedb/pkg/protogen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	log "github.com/sirupsen/logrus"
)

var (
	configFilePath = flag.String("configFilePath", "", "defines the config file path for the kv server.")
)

func main() {
	flag.Parse()
	conf := icecanekv.NewDefaultKVConfig()

	if *configFilePath != "" {
		conf.LoadFromFile(*configFilePath)
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
}
