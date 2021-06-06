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

package icecanesql

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/dr0pdb/icecanedb/pkg/common"
	pb "github.com/dr0pdb/icecanedb/pkg/protogen/icecanedbpb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// rpcRepository is responsible for communicating with the kv service
type rpcRepository struct {
	kvConn *grpc.ClientConn
	conf   *common.ClientConfig
}

// newRpcRepository creates a new rpc repository layer
func newRpcRepository(conf *common.ClientConfig) *rpcRepository {
	return &rpcRepository{
		conf:   conf,
		kvConn: nil,
	}
}

// createAndStoreConn creates and caches the grpc connection to the leader
func (r *rpcRepository) createAndStoreConn() error {
	if r.kvConn == nil || r.kvConn.GetState().String() != "READY" {
		sid := rand.Intn(len(r.conf.Servers)) + 1

		var opts []grpc.DialOption
		opts = append(opts, grpc.WithInsecure())
		opts = append(opts, grpc.WithBlock())
		conn, err := grpc.Dial(fmt.Sprintf("%s:%s", r.conf.Servers[sid-1].Address, r.conf.Servers[sid-1].Port), opts...)
		if err != nil {
			return err
		}

		r.kvConn = conn
	}

	return nil
}

// get makes a Get RPC call to the kv store
func (r *rpcRepository) get(key []byte, txnID uint64) (k, v []byte, err error) {
	err = r.createAndStoreConn()
	if err != nil {
		return nil, nil, err
	}

	client := pb.NewIcecaneKVClient(r.kvConn)
	req := &pb.GetRequest{
		Key:   key,
		TxnId: txnID,
	}

	resp, err := client.Get(context.Background(), req)
	if err != nil {
		log.Error(fmt.Sprintf("icecanesql::rpc::get; error in grpc request: %v", err))
		return nil, nil, err
	} else if resp.Error != nil {
		log.Error(fmt.Sprintf("icecanesql::rpc::get; error response from the kv server: %v", resp.Error))
		return nil, nil, fmt.Errorf(resp.Error.Message)
	}

	k = resp.Kv.Key
	v = resp.Kv.Value

	return k, v, nil
}

// set makes a Set RPC call to the kv store
func (r *rpcRepository) set(key, value []byte, txnID uint64) (bool, error) {
	err := r.createAndStoreConn()
	if err != nil {
		return false, err
	}

	client := pb.NewIcecaneKVClient(r.kvConn)
	req := &pb.SetRequest{
		Key:   key,
		Value: value,
		TxnId: txnID,
	}

	resp, err := client.Set(context.Background(), req)
	if err != nil {
		log.Error(fmt.Sprintf("icecanesql::rpc::set; error in grpc request: %v", err))
		return false, err
	} else if resp.Error != nil {
		log.Error(fmt.Sprintf("icecanesql::rpc::set; error response from the kv server: %v", resp.Error))
		return false, fmt.Errorf(resp.Error.Message)
	}

	return resp.Success, nil
}
