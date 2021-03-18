package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/dr0pdb/icecanedb/pkg/common"
	"github.com/dr0pdb/icecanedb/test"
	"google.golang.org/grpc"

	pb "github.com/dr0pdb/icecanedb/pkg/protogen"
	log "github.com/sirupsen/logrus"
)

var (
	// cache of grpc connections
	clientConnections map[uint64]*grpc.ClientConn = make(map[uint64]*grpc.ClientConn)

	// cluster nodes
	cluster map[uint64]*common.Peer = make(map[uint64]*common.Peer)

	// cached leaderID
	leaderID uint64
)

/*
	A sample application to utilize and test the icecane kv cluster
*/

func setup() {
	for i := uint64(1); i <= 5; i++ {
		p := &common.Peer{
			ID:      i,
			Address: "127.0.0.1",
			Port:    strconv.Itoa(int(9000 + i)),
		}

		cluster[i] = p
	}
}

func set(key, value []byte, txnID uint64) (*pb.SetResponse, error) {
	for {
		conn, err := getOrCreateClientConnection(leaderID)
		if err != nil {
			log.Error(fmt.Sprintf("error in getting conn: %v", err))
			return nil, err
		}

		client := pb.NewIcecaneKVClient(conn)

		req := &pb.SetRequest{
			Key:   key,
			Value: value,
			TxnId: txnID,
		}

		resp, err := client.Set(context.Background(), req)
		if err != nil {
			log.Error(fmt.Sprintf("error in grpc request: %v", err))
		} else {
			log.Info(fmt.Sprintf("received resp from peer %d, result: %v", leaderID, resp))
		}

		if !resp.Success && resp.LeaderId != leaderID {
			log.Info(fmt.Sprintf("different leader. updating"))
			leaderID = resp.LeaderId
			continue
		}

		return resp, err
	}
}

func setNoTxn(key, value []byte) (*pb.SetResponse, error) {
	return set(key, value, 0)
}

func get(key []byte, txnID uint64) (*pb.GetResponse, error) {
	for {
		conn, err := getOrCreateClientConnection(leaderID)
		if err != nil {
			log.Error(fmt.Sprintf("error in getting conn: %v", err))
			return nil, err
		}

		client := pb.NewIcecaneKVClient(conn)

		req := &pb.GetRequest{
			Key:   key,
			TxnId: txnID,
		}

		resp, err := client.Get(context.Background(), req)
		if err != nil {
			log.Error(fmt.Sprintf("error in grpc request: %v", err))
		} else {
			log.Info(fmt.Sprintf("received resp from peer %d, result: %v", leaderID, resp))
		}

		if resp.LeaderId != leaderID {
			log.Info(fmt.Sprintf("different leader. updating"))
			leaderID = resp.LeaderId
			continue
		}

		return resp, err
	}
}

func getNoTxn(key []byte) (*pb.GetResponse, error) {
	return get(key, 0)
}

func delete(key []byte, txnID uint64) (*pb.DeleteResponse, error) {
	for {
		conn, err := getOrCreateClientConnection(leaderID)
		if err != nil {
			log.Error(fmt.Sprintf("error in getting conn: %v", err))
			return nil, err
		}

		client := pb.NewIcecaneKVClient(conn)

		req := &pb.DeleteRequest{
			Key:   key,
			TxnId: txnID,
		}

		resp, err := client.Delete(context.Background(), req)
		if err != nil {
			log.Error(fmt.Sprintf("error in grpc request: %v", err))
		} else {
			log.Info(fmt.Sprintf("received resp from peer %d, result: %v", leaderID, resp))
		}

		if resp.LeaderId != leaderID {
			log.Info(fmt.Sprintf("different leader. updating"))
			leaderID = resp.LeaderId
			continue
		}

		return resp, err
	}
}

func deleteNoTxn(key []byte) (*pb.DeleteResponse, error) {
	return delete(key, 0)
}

func simpleCRUD() {
	for i := 0; i < 5; i++ {
		setNoTxn(test.TestKeys[i], test.TestValues[i])
		resp, err := getNoTxn(test.TestKeys[i])
		log.Info(fmt.Sprintf("resp: %v, err: %v", resp, err))
	}
}

// getOrCreateClientConnection gets or creates a grpc client connection for talking to peer with given id.
// In the case of creation, it caches it the clientConnections map
func getOrCreateClientConnection(voterID uint64) (*grpc.ClientConn, error) {
	if conn, ok := clientConnections[voterID]; ok && conn.GetState().String() == "READY" {
		return conn, nil
	}
	var p *common.Peer = nil

	for _, peer := range cluster {
		if peer.ID == voterID {
			p = peer
			break
		}
	}

	if p == nil {
		return nil, fmt.Errorf("invalid peer id %d", voterID)
	}

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	opts = append(opts, grpc.WithBlock())
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", p.Address, p.Port), opts...)
	if err != nil {
		return nil, err
	}
	clientConnections[voterID] = conn
	return conn, nil
}

func main() {
	leaderID = 1
	setup()

	simpleCRUD()
}
