/**
 * Copyright 2020 The IcecaneDB Authors. All rights reserved.
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

package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dr0pdb/icecanedb/pkg/common"
	"github.com/dr0pdb/icecanedb/pkg/icecanekv"
	icecanedbpb "github.com/dr0pdb/icecanedb/pkg/protogen/icecanedbpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	log "github.com/sirupsen/logrus"
)

var (
	configFilePath     = "/etc/icecanekv.yaml"
	configFilePathFlag = flag.String("configFilePath", "", "overrides the default config file path")
	logFile            = false
	logLevel           = flag.String("logLevel", "", "sets the log level. Options: info, warn, debug, error. Default: info")
)

func main() {
	flag.Parse()
	log.SetFormatter(&log.JSONFormatter{})
	log.Info("icecanekvmain::main::main; starting")
	conf := common.NewDefaultKVConfig()
	if *configFilePathFlag != "" {
		configFilePath = *configFilePathFlag
	}
	conf.LoadFromFile(configFilePath)

	err := conf.Validate()
	if err != nil {
		log.Fatalf("%V", err)
	}

	if logFile {
		f, err := os.OpenFile(fmt.Sprintf("/tmp/logfile-%d", conf.ID), os.O_WRONLY|os.O_CREATE, 0755)
		if err == nil {
			log.SetOutput(f)
			defer f.Close()
		}
	}
	if logLevel != nil {
		level := log.InfoLevel

		switch *logLevel {
		case "warn":
			level = log.WarnLevel

		case "debug":
			level = log.DebugLevel
		}

		log.SetLevel(level)
	}

	server, err := icecanekv.NewKVServer(conf)
	if err != nil {
		log.Fatalf("%V", err)
	}

	log.Info("icecanekvmain::main::main; setting up grpc server")

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
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%s", conf.Port))
	if err != nil {
		log.Fatalf("%V", err)
	}

	log.Info(fmt.Sprintf("icecanekvmain::main::main; grpc server listening on port %s", conf.Port))

	subSignal(server, grpcServer)

	err = grpcServer.Serve(listener)
	if err != nil {
		log.Fatal(err)
	}
	log.Info("icecanekvmain::main::main; grpc server stopped.")
}

// https://gobyexample.com/signals
func subSignal(server *icecanekv.KVServer, grpcServer *grpc.Server) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Infof("icecanekvmain::main::main; exiting server")
		server.Close()
		grpcServer.Stop()
	}()
}
