package icecanekv

import (
	"context"
	"testing"
	"time"

	"github.com/dr0pdb/icecanedb/pkg/protogen/icecanedbpb"
	"github.com/dr0pdb/icecanedb/test"
	"github.com/stretchr/testify/assert"
)

/*
	Tests which target both MVCC and Raft layer
*/

func TestMvccCrudBasic(t *testing.T) {
	th := newIcecaneKVTestHarness()
	defer th.teardown()

	leaderId, _ := th.checkSingleLeader(t)

	req := &icecanedbpb.SetRequest{
		Key:   test.TestKeys[0],
		Value: test.TestValues[0],
	}
	resp, err := th.kvServers[leaderId-1].Set(context.Background(), req)
	assert.Nil(t, err, "Unexpected error while writing key-value to leader")
	assert.NotNil(t, resp, "Set response unexpectedly null while writing key-value to leader")
	assert.True(t, resp.Success, "Unexpected failure while writing key-value to leader")
	assert.Nil(t, resp.Error, "Unexpected error resp while writing key-value to leader")

	getReq := &icecanedbpb.GetRequest{
		Key: test.TestKeys[0],
	}
	getResp, err := th.kvServers[leaderId-1].Get(context.Background(), getReq)
	assert.Nil(t, err, "Unexpected error while getting key-value from leader")
	assert.NotNil(t, getResp, "Get response unexpectedly null while getting key-value from leader")
	assert.Nil(t, getResp.Error, "Unexpected error resp while getting key-value from leader")
	assert.True(t, getResp.Found, "Value not found for the key while getting key-value from leader")
	assert.Equal(t, test.TestValues[0], getResp.Kv.GetValue(), "Unexpected error resp while getting key-value from leader")

	// update a new value for the key
	req = &icecanedbpb.SetRequest{
		Key:   test.TestKeys[0],
		Value: test.TestUpdatedValues[0],
	}
	resp, err = th.kvServers[leaderId-1].Set(context.Background(), req)
	assert.Nil(t, err, "Unexpected error while writing key-value to leader")
	assert.NotNil(t, resp, "Set response unexpectedly null while writing key-value to leader")
	assert.True(t, resp.Success, "Unexpected failure while writing key-value to leader")
	assert.Nil(t, resp.Error, "Unexpected error resp while writing key-value to leader")

	getReq = &icecanedbpb.GetRequest{
		Key: test.TestKeys[0],
	}
	getResp, err = th.kvServers[leaderId-1].Get(context.Background(), getReq)
	assert.Nil(t, err, "Unexpected error while getting key-value from leader")
	assert.NotNil(t, getResp, "Get response unexpectedly null while getting key-value from leader")
	assert.Nil(t, getResp.Error, "Unexpected error resp while getting key-value from leader")
	assert.True(t, getResp.Found, "Value not found for the key while getting key-value from leader")
	assert.Equal(t, test.TestUpdatedValues[0], getResp.Kv.GetValue(), "Unexpected error resp while getting key-value from leader")

	// delete the kv pair
	deleteReq := &icecanedbpb.DeleteRequest{
		Key: test.TestKeys[0],
	}
	deleteResp, err := th.kvServers[leaderId-1].Delete(context.Background(), deleteReq)
	assert.Nil(t, err, "Unexpected error while deleting key-value from leader")
	assert.NotNil(t, deleteResp, "Delete response unexpectedly null while deleting key-value from leader")
	assert.True(t, deleteResp.Success, "Unexpected failure while deleting key-value from leader")
	assert.Nil(t, deleteResp.Error, "Unexpected error resp while deleting key-value from leader")

	getReq = &icecanedbpb.GetRequest{
		Key: test.TestKeys[0],
	}
	getResp, err = th.kvServers[leaderId-1].Get(context.Background(), getReq)
	assert.Nil(t, err, "Unexpected error while getting key-value from leader")
	assert.NotNil(t, getResp, "Get response unexpectedly null while getting key-value from leader")
	assert.Nil(t, getResp.Error, "Unexpected error resp while getting key-value from leader")
	assert.False(t, getResp.Found, "Value found unexpectedly for the key while getting key-value from leader")
}

func TestMvccCommitDurable(t *testing.T) {
	th := newIcecaneKVTestHarness()
	defer th.teardown()

	leaderId, _ := th.checkSingleLeader(t)

	// begin txn
	btxnReq := &icecanedbpb.BeginTxnRequest{
		Mode: icecanedbpb.TxnMode_ReadWrite,
	}
	btxnResp, err := th.kvServers[leaderId-1].BeginTxn(context.Background(), btxnReq)
	assert.Nil(t, err, "Unexpected error while beginning a txn")
	assert.True(t, btxnResp.Success, "Success false when beginning a txn")

	// set kv
	req := &icecanedbpb.SetRequest{
		Key:   test.TestKeys[0],
		Value: test.TestValues[0],
		TxnId: btxnResp.TxnId,
	}
	resp, err := th.kvServers[leaderId-1].Set(context.Background(), req)
	assert.Nil(t, err, "Unexpected error while writing key-value to leader")
	assert.NotNil(t, resp, "Set response unexpectedly null while writing key-value to leader")
	assert.True(t, resp.Success, "Unexpected failure while writing key-value to leader")
	assert.Nil(t, resp.Error, "Unexpected error resp while writing key-value to leader")

	// get request with the same txn id
	getReq := &icecanedbpb.GetRequest{
		Key:   test.TestKeys[0],
		TxnId: btxnResp.TxnId,
	}
	getResp, err := th.kvServers[leaderId-1].Get(context.Background(), getReq)
	assert.Nil(t, err, "Unexpected error while getting key-value from leader")
	assert.NotNil(t, getResp, "Get response unexpectedly null while getting key-value from leader")
	assert.Nil(t, getResp.Error, "Unexpected error resp while getting key-value from leader")
	assert.True(t, getResp.Found, "Value not found for the key while getting key-value from leader")
	assert.Equal(t, test.TestValues[0], getResp.Kv.GetValue(), "Unexpected error resp while getting key-value from leader")

	// get req without txn id shouldn't read the value as it's not committed yet
	getReq = &icecanedbpb.GetRequest{
		Key: test.TestKeys[0],
	}
	getResp, err = th.kvServers[leaderId-1].Get(context.Background(), getReq)
	assert.Nil(t, err, "Unexpected error while getting key-value from leader")
	assert.NotNil(t, getResp, "Get response unexpectedly null while getting key-value from leader")
	assert.Nil(t, getResp.Error, "Unexpected error resp while getting key-value from leader")
	assert.False(t, getResp.Found, "Value found for the key while getting key-value from leader")

	// commit txn
	commitTxnReq := &icecanedbpb.CommitTxnRequest{
		TxnId: btxnResp.TxnId,
	}
	commitTxnResp, err := th.kvServers[leaderId-1].CommitTxn(context.Background(), commitTxnReq)
	assert.Nil(t, err, "Unexpected error while committing a txn")
	assert.True(t, commitTxnResp.Success, "Success false when committing a txn")

	// get request without txn id should read as it's committed now
	getReq = &icecanedbpb.GetRequest{
		Key: test.TestKeys[0],
	}
	getResp, err = th.kvServers[leaderId-1].Get(context.Background(), getReq)
	assert.Nil(t, err, "Unexpected error while getting key-value from leader")
	assert.NotNil(t, getResp, "Get response unexpectedly null while getting key-value from leader")
	assert.Nil(t, getResp.Error, "Unexpected error resp while getting key-value from leader")
	assert.True(t, getResp.Found, "Value not found for the key while getting key-value from leader")
	assert.Equal(t, test.TestValues[0], getResp.Kv.GetValue(), "Unexpected error resp while getting key-value from leader")
}

func TestMvccGetSetWithLeaderChange(t *testing.T) {
	th := newIcecaneKVTestHarness()
	defer th.teardown()

	leaderId, leaderTerm := th.checkSingleLeader(t)

	req := &icecanedbpb.SetRequest{
		Key:   test.TestKeys[0],
		Value: test.TestValues[0],
	}
	resp, err := th.kvServers[leaderId-1].Set(context.Background(), req)
	assert.Nil(t, err, "Unexpected error while writing key-value to leader")
	assert.NotNil(t, resp, "Set response unexpectedly null while writing key-value to leader")
	assert.True(t, resp.Success, "Unexpected failure while writing key-value to leader")
	assert.Nil(t, resp.Error, "Unexpected error resp while writing key-value to leader")

	getReq := &icecanedbpb.GetRequest{
		Key: test.TestKeys[0],
	}
	getResp, err := th.kvServers[leaderId-1].Get(context.Background(), getReq)
	assert.Nil(t, err, "Unexpected error while getting key-value from leader")
	assert.NotNil(t, getResp, "Get response unexpectedly null while getting key-value from leader")
	assert.Nil(t, getResp.Error, "Unexpected error resp while getting key-value from leader")
	assert.True(t, getResp.Found, "Value not found for the key while getting key-value from leader")
	assert.Equal(t, test.TestValues[0], getResp.Kv.GetValue(), "Unexpected error resp while getting key-value from leader")

	th.disconnectPeer(leaderId)

	// sleep while another leader is elected
	time.Sleep(500 * time.Millisecond)

	newLeaderID, newLeaderTerm := th.checkSingleLeader(t)
	assert.Greater(t, newLeaderTerm, leaderTerm, "error: newLeaderTerm <= leaderTerm")

	// write new kv pair
	req = &icecanedbpb.SetRequest{
		Key:   test.TestKeys[1],
		Value: test.TestValues[1],
	}
	resp, err = th.kvServers[newLeaderID-1].Set(context.Background(), req)
	assert.Nil(t, err, "Unexpected error while writing key-value to leader")
	assert.NotNil(t, resp, "Set response unexpectedly null while writing key-value to leader")
	assert.True(t, resp.Success, "Unexpected failure while writing key-value to leader")
	assert.Nil(t, resp.Error, "Unexpected error resp while writing key-value to leader")

	// get first kv pair
	getReq = &icecanedbpb.GetRequest{
		Key: test.TestKeys[0],
	}
	getResp, err = th.kvServers[newLeaderID-1].Get(context.Background(), getReq)
	assert.Nil(t, err, "PostDisconnect: Unexpected error while getting key-value from leader")
	assert.NotNil(t, getResp, "PostDisconnect: Get response unexpectedly null while getting key-value from leader")
	assert.Nil(t, getResp.Error, "PostDisconnect: Unexpected error resp while getting key-value from leader")
	assert.True(t, getResp.Found, "PostDisconnect: Value not found for the key while getting key-value from leader")
	assert.Equal(t, test.TestValues[0], getResp.Kv.Value, "PostDisconnect: Unexpected error resp while getting key-value from leader")
}
