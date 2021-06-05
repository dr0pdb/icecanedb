package icecanekv

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dr0pdb/icecanedb/pkg/protogen/icecanedbpb"
	"github.com/dr0pdb/icecanedb/test"
	"github.com/stretchr/testify/assert"
)

/*
	Tests which target both MVCC and Raft layer
*/

// trying to get a non-existent (deleted) key should return found = false
func TestGetDeleteKeyThrowsError(t *testing.T) {
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

	// delete the kv pair
	deleteReq := &icecanedbpb.DeleteRequest{
		Key: test.TestKeys[0],
	}
	deleteResp, err := th.kvServers[leaderId-1].Delete(context.Background(), deleteReq)
	assert.Nil(t, err, "Unexpected error while deleting key-value from leader")
	assert.NotNil(t, deleteResp, "Delete response unexpectedly null while deleting key-value from leader")
	assert.True(t, deleteResp.Success, "Unexpected failure while deleting key-value from leader")
	assert.Nil(t, deleteResp.Error, "Unexpected error resp while deleting key-value from leader")

	// get the pair
	getReq = &icecanedbpb.GetRequest{
		Key: test.TestKeys[0],
	}
	getResp, err = th.kvServers[leaderId-1].Get(context.Background(), getReq)
	assert.Nil(t, err, "Unexpected error while getting key-value from leader")
	assert.NotNil(t, getResp, "Get response unexpectedly null while getting key-value from leader")
	assert.Nil(t, getResp.Error, "Unexpected error resp while getting key-value from leader")
	assert.False(t, getResp.Found, "Value found unexpectedly for the key while getting key-value from leader")
}

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

// begin a txn, do a set. disconnect the leader and then commit txn. It should be successful
func TestMvccTxnCommitWithLeaderChange(t *testing.T) {
	th := newIcecaneKVTestHarness()
	defer th.teardown()

	leaderId, leaderTerm := th.checkSingleLeader(t)

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

	// disconnect leader
	th.disconnectPeer(leaderId)

	// sleep while another leader is elected
	time.Sleep(500 * time.Millisecond)

	newLeaderID, newLeaderTerm := th.checkSingleLeader(t)
	assert.Greater(t, newLeaderTerm, leaderTerm, "error: newLeaderTerm <= leaderTerm")

	// commit txn
	commitTxnReq := &icecanedbpb.CommitTxnRequest{
		TxnId: btxnResp.TxnId,
	}
	commitTxnResp, err := th.kvServers[newLeaderID-1].CommitTxn(context.Background(), commitTxnReq)
	assert.Nil(t, err, "Unexpected error while committing a txn")
	assert.True(t, commitTxnResp.Success, "Success false when committing a txn")
}

// begin a txn, do a set. disconnect the leader and then rollback txn. It should be successful
func TestMvccTxnRollbackWithLeaderChange(t *testing.T) {
	th := newIcecaneKVTestHarness()
	defer th.teardown()

	leaderId, leaderTerm := th.checkSingleLeader(t)

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

	// disconnect leader
	th.disconnectPeer(leaderId)

	// sleep while another leader is elected
	time.Sleep(500 * time.Millisecond)

	newLeaderID, newLeaderTerm := th.checkSingleLeader(t)
	assert.Greater(t, newLeaderTerm, leaderTerm, "error: newLeaderTerm <= leaderTerm")

	// rollback txn
	rollbackTxnReq := &icecanedbpb.RollbackTxnRequest{
		TxnId: btxnResp.TxnId,
	}
	rollbackTxnResp, err := th.kvServers[newLeaderID-1].RollbackTxn(context.Background(), rollbackTxnReq)
	assert.Nil(t, err, "Unexpected error while rolling back a txn")
	assert.True(t, rollbackTxnResp.Success, "Success false when rolling back a txn")
}

func TestConcurrentNonConflictingWrites(t *testing.T) {
	th := newIcecaneKVTestHarness()
	defer th.teardown()

	leaderId, _ := th.checkSingleLeader(t)

	n := 10
	testKeys := make([][]byte, n)
	for i := 0; i < n; i++ {
		testKeys[i] = []byte(fmt.Sprintf("testkey%d", i))
	}

	wg := &sync.WaitGroup{}
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()

			// begin txn
			txn, err := th.kvServers[leaderId-1].BeginTxn(context.Background(), &icecanedbpb.BeginTxnRequest{Mode: icecanedbpb.TxnMode_ReadWrite})
			assert.Nil(t, err, "Unexpected error while beginning a txn")

			// write
			sresp, err := th.kvServers[leaderId-1].Set(context.Background(), &icecanedbpb.SetRequest{TxnId: txn.TxnId, Key: testKeys[idx], Value: test.TestValues[0]})
			assert.Nil(t, err, "Unexpected error during set request")
			assert.True(t, sresp.Success, "Error nil but success=false in set request")
			assert.Nil(t, sresp.Error, "Unexpected error resp during set request")

			// read
			rresp, err := th.kvServers[leaderId-1].Get(context.Background(), &icecanedbpb.GetRequest{Key: testKeys[idx], TxnId: txn.TxnId})
			assert.Nil(t, err, "Unexpected error during get request")
			assert.True(t, rresp.Found, "Error nil but found=false in get request")
			assert.Nil(t, rresp.Error, "Unexpected error resp during get request")
			assert.Equal(t, test.TestValues[0], rresp.Kv.Value, "get response value doesn't match with expected value")

			// update
			sresp, err = th.kvServers[leaderId-1].Set(context.Background(), &icecanedbpb.SetRequest{TxnId: txn.TxnId, Key: testKeys[idx], Value: test.TestValues[1]})
			assert.Nil(t, err, "Unexpected error during set request")
			assert.True(t, sresp.Success, "Error nil but success=false in set request")
			assert.Nil(t, sresp.Error, "Unexpected error resp during set request")

			// read updated value
			rresp, err = th.kvServers[leaderId-1].Get(context.Background(), &icecanedbpb.GetRequest{Key: testKeys[idx], TxnId: txn.TxnId})
			assert.Nil(t, err, "Unexpected error during get request")
			assert.True(t, rresp.Found, "Error nil but found=false in get request")
			assert.Nil(t, rresp.Error, "Unexpected error resp during get request")
			assert.Equal(t, test.TestValues[1], rresp.Kv.Value, "get response value doesn't match with expected value")

			// delete
			dresp, err := th.kvServers[leaderId-1].Delete(context.Background(), &icecanedbpb.DeleteRequest{TxnId: txn.TxnId, Key: testKeys[idx]})
			assert.Nil(t, err, "Unexpected error during delete request")
			assert.True(t, dresp.Success, "Error nil but success=false in delete request")
			assert.Nil(t, dresp.Error, "Unexpected error resp during delete request")

			// read after deleting value
			rresp, err = th.kvServers[leaderId-1].Get(context.Background(), &icecanedbpb.GetRequest{Key: testKeys[idx], TxnId: txn.TxnId})
			assert.Nil(t, err, "Unexpected error during get request")
			assert.False(t, rresp.Found, "Error nil but found=true in get request. Expected value to not be found")
			assert.Nil(t, rresp.Error, "Unexpected error resp during get request")

			// commit
			cresp, err := th.kvServers[leaderId-1].CommitTxn(context.Background(), &icecanedbpb.CommitTxnRequest{TxnId: txn.TxnId})
			assert.Nil(t, err, "Unexpected error during commit request")
			assert.True(t, cresp.Success, "Error nil but success=false in commit request")

			// read after commit without passing txn id
			rresp, err = th.kvServers[leaderId-1].Get(context.Background(), &icecanedbpb.GetRequest{Key: testKeys[idx]})
			assert.Nil(t, err, "Unexpected error during get request")
			assert.False(t, rresp.Found, "Error nil but found=true in get request. expected value to not be found")
			assert.Nil(t, rresp.Error, "Unexpected error resp during get request")
		}(i)
	}

	wg.Wait()
}
