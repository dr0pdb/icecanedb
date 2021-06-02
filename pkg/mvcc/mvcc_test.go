/**
 * Copyright 2021 The IcecaneDB Authors. All rights reserved.
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

package mvcc

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/dr0pdb/icecanedb/pkg/protogen/icecanedbpb"
	"github.com/dr0pdb/icecanedb/test"
	"github.com/stretchr/testify/assert"
)

var (
	testDirectory = "/tmp/icecanetesting/"
)

func TestNextTxnId(t *testing.T) {
	h := newMvccTestHarness(testDirectory, true)
	err := h.init()
	assert.Nil(t, err, "Unexpected error while initiating test harness")
	defer h.cleanup()

	id, err := h.mvcc.getNextTxnID()
	assert.Nil(t, err, "Unexpected error while getting a txn id")
	assert.Equal(t, uint64(1), id, "expected first txn id to be 1")

	nxtID, err := h.mvcc.getNextTxnID()
	assert.Nil(t, err, "Unexpected error while getting next txn id")
	assert.Equal(t, uint64(2), nxtID, "expected next txn id to be 2")
}

func TestSingleTxnGetSet(t *testing.T) {
	h := newMvccTestHarness(testDirectory, true)
	err := h.init()
	assert.Nil(t, err, "Unexpected error while initiating test harness")
	defer h.cleanup()

	// begin txn
	txn, err := h.mvcc.BeginTxn(context.Background(), &icecanedbpb.BeginTxnRequest{Mode: icecanedbpb.TxnMode_ReadWrite})
	assert.Nil(t, err, "Unexpected error while beginning a txn")

	// write
	sresp, err := h.mvcc.Set(context.Background(), &icecanedbpb.SetRequest{TxnId: txn.TxnId, Key: test.TestKeys[0], Value: test.TestValues[0]})
	assert.Nil(t, err, "Unexpected error during set request")
	assert.True(t, sresp.Success, "Error nil but success=false in set request")
	assert.Nil(t, sresp.Error, "Unexpected error resp during set request")

	// read
	rresp, err := h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0], TxnId: txn.TxnId})
	assert.Nil(t, err, "Unexpected error during get request")
	assert.True(t, rresp.Found, "Error nil but found=false in get request")
	assert.Nil(t, rresp.Error, "Unexpected error resp during get request")
	assert.Equal(t, test.TestValues[0], rresp.Kv.Value, "get response value doesn't match with expected value")

	// commit
	cresp, err := h.mvcc.CommitTxn(context.Background(), &icecanedbpb.CommitTxnRequest{TxnId: txn.TxnId})
	assert.Nil(t, err, "Unexpected error during commit request")
	assert.True(t, cresp.Success, "Error nil but success=false in commit request")

	// read after commit without passing txn id
	rresp, err = h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0]})
	assert.Nil(t, err, "Unexpected error during get request")
	assert.True(t, rresp.Found, "Error nil but found=false in get request")
	assert.Nil(t, rresp.Error, "Unexpected error resp during get request")
	assert.Equal(t, test.TestValues[0], rresp.Kv.Value, "get response value doesn't match with expected value")
}

func TestSingleTxnScan(t *testing.T) {
	h := newMvccTestHarness(testDirectory, true)
	err := h.init()
	assert.Nil(t, err, "Unexpected error while initiating test harness")
	defer h.cleanup()

	// begin txn
	txn, err := h.mvcc.BeginTxn(context.Background(), &icecanedbpb.BeginTxnRequest{Mode: icecanedbpb.TxnMode_ReadWrite})
	assert.Nil(t, err, "Unexpected error while beginning a txn")

	// write
	for i := 0; i < 5; i++ {
		sresp, err := h.mvcc.Set(context.Background(), &icecanedbpb.SetRequest{TxnId: txn.TxnId, Key: test.TestKeys[i], Value: test.TestValues[i]})
		assert.Nil(t, err, "Unexpected error during set request")
		assert.True(t, sresp.Success, "Error nil but success=false in set request")
		assert.Nil(t, sresp.Error, "Unexpected error resp during set request")
	}

	// update key 0 to have updated value.
	sresp, err := h.mvcc.Set(context.Background(), &icecanedbpb.SetRequest{TxnId: txn.TxnId, Key: test.TestKeys[0], Value: test.TestUpdatedValues[0]})
	assert.Nil(t, err, "Unexpected error during set request")
	assert.True(t, sresp.Success, "Error nil but success=false in set request")
	assert.Nil(t, sresp.Error, "Unexpected error resp during set request")

	// scan with the txn id - for key 0 we should see updated value and for the rest, it should be initial value
	scanResp, err := h.mvcc.Scan(context.Background(), &icecanedbpb.ScanRequest{TxnId: txn.TxnId, StartKey: test.TestKeys[0], MaxReadings: 100})
	assert.Nil(t, err, "Unexpected error during scan request")
	assert.Equal(t, test.TestKeys[0], scanResp.Entries[0].Key, "get response key doesn't match with expected key")
	assert.Equal(t, test.TestUpdatedValues[0], scanResp.Entries[0].Value, "get response value doesn't match with expected value")

	for i := 1; i < 5; i++ {
		assert.Equal(t, test.TestKeys[i], scanResp.Entries[i].Key, "get response key doesn't match with expected key")
		assert.Equal(t, test.TestValues[i], scanResp.Entries[i].Value, "get response value doesn't match with expected value")
	}

	// scan without txn id. Entries should be empty
	scanResp, err = h.mvcc.Scan(context.Background(), &icecanedbpb.ScanRequest{StartKey: test.TestKeys[0], MaxReadings: 100})
	assert.Nil(t, err, "Unexpected error during scan request")
	assert.Equal(t, 0, len(scanResp.Entries), "expected entries to be empty")
}

func TestCRUDImplicitTxns(t *testing.T) {
	h := newMvccTestHarness(testDirectory, true)
	err := h.init()
	assert.Nil(t, err, "Unexpected error while initiating test harness")
	defer h.cleanup()

	// write
	sresp, err := h.mvcc.Set(context.Background(), &icecanedbpb.SetRequest{Key: test.TestKeys[0], Value: test.TestValues[0]})
	assert.Nil(t, err, "Unexpected error during set request")
	assert.True(t, sresp.Success, "Error nil but success=false in set request")
	assert.Nil(t, sresp.Error, "Unexpected error resp during set request")

	// read
	rresp, err := h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0]})
	assert.Nil(t, err, "Unexpected error during get request")
	assert.True(t, rresp.Found, "Error nil but found=false in get request")
	assert.Nil(t, rresp.Error, "Unexpected error resp during get request")
	assert.Equal(t, test.TestValues[0], rresp.Kv.Value, "get response value doesn't match with expected value")

	// update
	sresp, err = h.mvcc.Set(context.Background(), &icecanedbpb.SetRequest{Key: test.TestKeys[0], Value: test.TestValues[1]})
	assert.Nil(t, err, "Unexpected error during set request")
	assert.True(t, sresp.Success, "Error nil but success=false in set request")
	assert.Nil(t, sresp.Error, "Unexpected error resp during set request")

	// read updated value
	rresp, err = h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0]})
	assert.Nil(t, err, "Unexpected error during get request")
	assert.True(t, rresp.Found, "Error nil but found=false in get request")
	assert.Nil(t, rresp.Error, "Unexpected error resp during get request")
	assert.Equal(t, test.TestValues[1], rresp.Kv.Value, "get response value doesn't match with expected value")

	// delete
	dresp, err := h.mvcc.Delete(context.Background(), &icecanedbpb.DeleteRequest{Key: test.TestKeys[0]})
	assert.Nil(t, err, "Unexpected error during delete request")
	assert.True(t, dresp.Success, "Error nil but success=false in delete request")
	assert.Nil(t, dresp.Error, "Unexpected error resp during delete request")

	// read after deleting value
	rresp, err = h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0]})
	assert.Nil(t, err, "Unexpected error during get request")
	assert.False(t, rresp.Found, "Error nil but found=true in get request. Expected value to not be found")
	assert.Nil(t, rresp.Error, "Unexpected error resp during get request")
}

func TestSingleTxnCRUD(t *testing.T) {
	h := newMvccTestHarness(testDirectory, true)
	err := h.init()
	assert.Nil(t, err, "Unexpected error while initiating test harness")
	defer h.cleanup()

	// begin txn
	txn, err := h.mvcc.BeginTxn(context.Background(), &icecanedbpb.BeginTxnRequest{Mode: icecanedbpb.TxnMode_ReadWrite})
	assert.Nil(t, err, "Unexpected error while beginning a txn")

	// write
	sresp, err := h.mvcc.Set(context.Background(), &icecanedbpb.SetRequest{TxnId: txn.TxnId, Key: test.TestKeys[0], Value: test.TestValues[0]})
	assert.Nil(t, err, "Unexpected error during set request")
	assert.True(t, sresp.Success, "Error nil but success=false in set request")
	assert.Nil(t, sresp.Error, "Unexpected error resp during set request")

	// read
	rresp, err := h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0], TxnId: txn.TxnId})
	assert.Nil(t, err, "Unexpected error during get request")
	assert.True(t, rresp.Found, "Error nil but found=false in get request")
	assert.Nil(t, rresp.Error, "Unexpected error resp during get request")
	assert.Equal(t, test.TestValues[0], rresp.Kv.Value, "get response value doesn't match with expected value")

	// update
	sresp, err = h.mvcc.Set(context.Background(), &icecanedbpb.SetRequest{TxnId: txn.TxnId, Key: test.TestKeys[0], Value: test.TestValues[1]})
	assert.Nil(t, err, "Unexpected error during set request")
	assert.True(t, sresp.Success, "Error nil but success=false in set request")
	assert.Nil(t, sresp.Error, "Unexpected error resp during set request")

	// read updated value
	rresp, err = h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0], TxnId: txn.TxnId})
	assert.Nil(t, err, "Unexpected error during get request")
	assert.True(t, rresp.Found, "Error nil but found=false in get request")
	assert.Nil(t, rresp.Error, "Unexpected error resp during get request")
	assert.Equal(t, test.TestValues[1], rresp.Kv.Value, "get response value doesn't match with expected value")

	// delete
	dresp, err := h.mvcc.Delete(context.Background(), &icecanedbpb.DeleteRequest{TxnId: txn.TxnId, Key: test.TestKeys[0]})
	assert.Nil(t, err, "Unexpected error during delete request")
	assert.True(t, dresp.Success, "Error nil but success=false in delete request")
	assert.Nil(t, dresp.Error, "Unexpected error resp during delete request")

	// read after deleting value
	rresp, err = h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0], TxnId: txn.TxnId})
	assert.Nil(t, err, "Unexpected error during get request")
	assert.False(t, rresp.Found, "Error nil but found=true in get request. Expected value to not be found")
	assert.Nil(t, rresp.Error, "Unexpected error resp during get request")

	// commit
	cresp, err := h.mvcc.CommitTxn(context.Background(), &icecanedbpb.CommitTxnRequest{TxnId: txn.TxnId})
	assert.Nil(t, err, "Unexpected error during commit request")
	assert.True(t, cresp.Success, "Error nil but success=false in commit request")

	// read after commit without passing txn id
	rresp, err = h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0]})
	assert.Nil(t, err, "Unexpected error during get request")
	assert.False(t, rresp.Found, "Error nil but found=true in get request. expected value to not be found")
	assert.Nil(t, rresp.Error, "Unexpected error resp during get request")
}

func TestSingleTxnRollback(t *testing.T) {
	h := newMvccTestHarness(testDirectory, true)
	err := h.init()
	assert.Nil(t, err, "Unexpected error while initiating test harness")
	defer h.cleanup()

	// begin txn
	txn, err := h.mvcc.BeginTxn(context.Background(), &icecanedbpb.BeginTxnRequest{Mode: icecanedbpb.TxnMode_ReadWrite})
	assert.Nil(t, err, "Unexpected error while beginning a txn")

	// write
	sresp, err := h.mvcc.Set(context.Background(), &icecanedbpb.SetRequest{TxnId: txn.TxnId, Key: test.TestKeys[0], Value: test.TestValues[0]})
	assert.Nil(t, err, "Unexpected error during set request")
	assert.True(t, sresp.Success, "Error nil but success=false in set request")
	assert.Nil(t, sresp.Error, "Unexpected error resp during set request")

	// read
	rresp, err := h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0], TxnId: txn.TxnId})
	assert.Nil(t, err, "Unexpected error during get request")
	assert.True(t, rresp.Found, "Error nil but found=false in get request")
	assert.Nil(t, rresp.Error, "Unexpected error resp during get request")
	assert.Equal(t, test.TestValues[0], rresp.Kv.Value, "get response value doesn't match with expected value")

	// rollback
	rollResp, err := h.mvcc.RollbackTxn(context.Background(), &icecanedbpb.RollbackTxnRequest{TxnId: txn.TxnId})
	assert.Nil(t, err, "Unexpected error during rollback request")
	assert.True(t, rollResp.Success, "Error nil but success=false in rollback request")

	// read after rollback without passing txn id
	rresp, err = h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0]})
	assert.Nil(t, err, "Unexpected error during get request")
	assert.False(t, rresp.Found, "Error nil but found=true in get request when expected false")
	assert.Nil(t, rresp.Error, "Unexpected error resp during get request")
	assert.Nil(t, rresp.GetKv(), "get response value doesn't match with expected value")
}

func TestMultipleTxnSequential(t *testing.T) {
	h := newMvccTestHarness(testDirectory, true)
	err := h.init()
	assert.Nil(t, err, "Unexpected error while initiating test harness")
	defer h.cleanup()

	wg := &sync.WaitGroup{}
	wg.Add(2)

	// begin txn
	txn, err := h.mvcc.BeginTxn(context.Background(), &icecanedbpb.BeginTxnRequest{Mode: icecanedbpb.TxnMode_ReadWrite})
	assert.Nil(t, err, "txn1: Unexpected error while beginning a txn")

	// write
	sresp, err := h.mvcc.Set(context.Background(), &icecanedbpb.SetRequest{TxnId: txn.TxnId, Key: test.TestKeys[0], Value: test.TestValues[0]})
	assert.Nil(t, err, "txn1: Unexpected error during set request")
	assert.True(t, sresp.Success, "txn1: Error nil but success=false in set request")
	assert.Nil(t, sresp.Error, "txn1: Unexpected error resp during set request")

	// read
	rresp, err := h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0], TxnId: txn.TxnId})
	assert.Nil(t, err, "txn1: Unexpected error during get request")
	assert.True(t, rresp.Found, "txn1: Error nil but found=false in get request")
	assert.Nil(t, rresp.Error, "txn1: Unexpected error resp during get request")
	assert.Equal(t, test.TestValues[0], rresp.Kv.Value, "txn1: get response value doesn't match with expected value")

	// commit
	cresp, err := h.mvcc.CommitTxn(context.Background(), &icecanedbpb.CommitTxnRequest{TxnId: txn.TxnId})
	assert.Nil(t, err, "Unexpected error during commit request")
	assert.True(t, cresp.Success, "Error nil but success=false in commit request")

	// read after commit
	rresp, err = h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0]})
	assert.Nil(t, err, "txn1: Unexpected error during get request")
	assert.True(t, rresp.Found, "txn1: Error nil but found=false in get request")
	assert.Nil(t, rresp.Error, "txn1: Unexpected error resp during get request")
	assert.Equal(t, test.TestValues[0], rresp.Kv.Value, "txn1: get response value doesn't match with expected value")

	// begin txn2
	txn2, err := h.mvcc.BeginTxn(context.Background(), &icecanedbpb.BeginTxnRequest{Mode: icecanedbpb.TxnMode_ReadWrite})
	assert.Nil(t, err, "txn2: Unexpected error while beginning a txn")

	// write
	sresp, err = h.mvcc.Set(context.Background(), &icecanedbpb.SetRequest{TxnId: txn2.TxnId, Key: test.TestKeys[0], Value: test.TestValues[1]})
	assert.Nil(t, err, "txn2: Unexpected error during set request")
	assert.True(t, sresp.Success, "txn2: Error nil but success=false in set request")
	assert.Nil(t, sresp.Error, "txn2: Unexpected error resp during set request")

	// read
	rresp, err = h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0], TxnId: txn2.TxnId})
	assert.Nil(t, err, "txn2: Unexpected error during get request")
	assert.True(t, rresp.Found, "txn2: Error nil but found=false in get request")
	assert.Nil(t, rresp.Error, "txn2: Unexpected error resp during get request")
	assert.Equal(t, test.TestValues[1], rresp.Kv.Value, "txn2: get response value doesn't match with expected value")

	// commit
	cresp, err = h.mvcc.CommitTxn(context.Background(), &icecanedbpb.CommitTxnRequest{TxnId: txn2.TxnId})
	assert.Nil(t, err, "Unexpected error during commit request")
	assert.True(t, cresp.Success, "Error nil but success=false in commit request")

	// read after commit
	rresp, err = h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0]})
	assert.Nil(t, err, "txn2: Unexpected error during get request")
	assert.True(t, rresp.Found, "txn2: Error nil but found=false in get request")
	assert.Nil(t, rresp.Error, "txn2: Unexpected error resp during get request")
	assert.Equal(t, test.TestValues[1], rresp.Kv.Value, "txn2: get response value doesn't match with expected value")
}

// Two txns doing a write on different keys. Both of them should succeed
func TestMultipleTxnNonConflicting(t *testing.T) {
	h := newMvccTestHarness(testDirectory, true)
	err := h.init()
	assert.Nil(t, err, "Unexpected error while initiating test harness")
	defer h.cleanup()

	wg := &sync.WaitGroup{}
	wg.Add(2)

	// txn1
	go func() {
		defer wg.Done()

		// begin txn
		txn, err := h.mvcc.BeginTxn(context.Background(), &icecanedbpb.BeginTxnRequest{Mode: icecanedbpb.TxnMode_ReadWrite})
		assert.Nil(t, err, "txn1: Unexpected error while beginning a txn")

		// write
		sresp, err := h.mvcc.Set(context.Background(), &icecanedbpb.SetRequest{TxnId: txn.TxnId, Key: test.TestKeys[0], Value: test.TestValues[0]})
		assert.Nil(t, err, "txn1: Unexpected error during set request")
		assert.True(t, sresp.Success, "txn1: Error nil but success=false in set request")
		assert.Nil(t, sresp.Error, "txn1: Unexpected error resp during set request")

		// read
		rresp, err := h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0], TxnId: txn.TxnId})
		assert.Nil(t, err, "txn1: Unexpected error during get request")
		assert.True(t, rresp.Found, "txn1: Error nil but found=false in get request")
		assert.Nil(t, rresp.Error, "txn1: Unexpected error resp during get request")
		assert.Equal(t, test.TestValues[0], rresp.Kv.Value, "txn1: get response value doesn't match with expected value")

		// commit
		cresp, err := h.mvcc.CommitTxn(context.Background(), &icecanedbpb.CommitTxnRequest{TxnId: txn.TxnId})
		assert.Nil(t, err, "txn1: Unexpected error during commit request")
		assert.True(t, cresp.Success, "txn1: Error nil but success=false in commit request")

		// read after commit without passing txn id
		rresp, err = h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0]})
		assert.Nil(t, err, "txn1: Unexpected error during get request")
		assert.True(t, rresp.Found, "txn1: Error nil but found=false in get request")
		assert.Nil(t, rresp.Error, "txn1: Unexpected error resp during get request")
		assert.Equal(t, test.TestValues[0], rresp.Kv.Value, "txn1: get response value doesn't match with expected value")
	}()

	// txn2
	go func() {
		defer wg.Done()

		// begin txn
		txn, err := h.mvcc.BeginTxn(context.Background(), &icecanedbpb.BeginTxnRequest{Mode: icecanedbpb.TxnMode_ReadWrite})
		assert.Nil(t, err, "txn2: Unexpected error while beginning a txn")

		// write
		sresp, err := h.mvcc.Set(context.Background(), &icecanedbpb.SetRequest{TxnId: txn.TxnId, Key: test.TestKeys[1], Value: test.TestValues[1]})
		assert.Nil(t, err, "txn2: Unexpected error during set request")
		assert.True(t, sresp.Success, "txn2: Error nil but success=false in set request")
		assert.Nil(t, sresp.Error, "txn2: Unexpected error resp during set request")

		// read
		rresp, err := h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[1], TxnId: txn.TxnId})
		assert.Nil(t, err, "txn2: Unexpected error during get request")
		assert.True(t, rresp.Found, "txn2: Error nil but found=false in get request")
		assert.Nil(t, rresp.Error, "txn2: Unexpected error resp during get request")
		assert.Equal(t, test.TestValues[1], rresp.Kv.Value, "txn2: get response value doesn't match with expected value")

		// commit
		cresp, err := h.mvcc.CommitTxn(context.Background(), &icecanedbpb.CommitTxnRequest{TxnId: txn.TxnId})
		assert.Nil(t, err, "txn2: Unexpected error during commit request")
		assert.True(t, cresp.Success, "txn2: Error nil but success=false in commit request")

		// read after commit without passing txn id
		rresp, err = h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[1]})
		assert.Nil(t, err, "txn2: Unexpected error during get request")
		assert.True(t, rresp.Found, "txn2: Error nil but found=false in get request")
		assert.Nil(t, rresp.Error, "txn2: Unexpected error resp during get request")
		assert.Equal(t, test.TestValues[1], rresp.Kv.Value, "txn2: get response value doesn't match with expected value")
	}()

	wg.Wait()

	// read in another thread after both are committed
	rresp, err := h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[1]})
	assert.Nil(t, err, "Unexpected error during get request")
	assert.True(t, rresp.Found, "Error nil but found=false in get request")
	assert.Nil(t, rresp.Error, "Unexpected error resp during get request")
	assert.Equal(t, test.TestValues[1], rresp.Kv.Value, "get response value doesn't match with expected value")

	// read in another thread after both are committed
	rresp, err = h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0]})
	assert.Nil(t, err, "Unexpected error during get request")
	assert.True(t, rresp.Found, "Error nil but found=false in get request")
	assert.Nil(t, rresp.Error, "Unexpected error resp during get request")
	assert.Equal(t, test.TestValues[0], rresp.Kv.Value, "get response value doesn't match with expected value")
}

// Two txns doing a write on the same key. Exactly one of them should succeed
func TestMultipleTxnConflicting(t *testing.T) {
	h := newMvccTestHarness(testDirectory, true)
	err := h.init()
	assert.Nil(t, err, "Unexpected error while initiating test harness")
	defer h.cleanup()

	wg := &sync.WaitGroup{}
	wg.Add(2)

	// begin txn
	txn, err := h.mvcc.BeginTxn(context.Background(), &icecanedbpb.BeginTxnRequest{Mode: icecanedbpb.TxnMode_ReadWrite})
	assert.Nil(t, err, "txn1: Unexpected error while beginning a txn")

	// begin txn2
	txn2, err := h.mvcc.BeginTxn(context.Background(), &icecanedbpb.BeginTxnRequest{Mode: icecanedbpb.TxnMode_ReadWrite})
	assert.Nil(t, err, "txn2: Unexpected error while beginning a txn")

	successOne := false
	successTwo := false

	// txn1
	go func(txn *icecanedbpb.BeginTxnResponse) {
		defer wg.Done()

		// write
		sresp, err := h.mvcc.Set(context.Background(), &icecanedbpb.SetRequest{TxnId: txn.TxnId, Key: test.TestKeys[0], Value: test.TestValues[0]})
		if err == nil && sresp.Success {
			successOne = true
			assert.Nil(t, err, "txn1: Unexpected error during set request")
			assert.True(t, sresp.Success, "txn1: Error nil but success=false in set request")
			assert.Nil(t, sresp.Error, "txn1: Unexpected error resp during set request")

			// read
			rresp, err := h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0], TxnId: txn.TxnId})
			assert.Nil(t, err, "txn1: Unexpected error during get request")
			assert.True(t, rresp.Found, "txn1: Error nil but found=false in get request")
			assert.Nil(t, rresp.Error, "txn1: Unexpected error resp during get request")
			assert.Equal(t, test.TestValues[0], rresp.Kv.Value, "txn1: get response value doesn't match with expected value")

			// commit
			cresp, err := h.mvcc.CommitTxn(context.Background(), &icecanedbpb.CommitTxnRequest{TxnId: txn.TxnId})
			assert.Nil(t, err, "Unexpected error during commit request")
			assert.True(t, cresp.Success, "Error nil but success=false in commit request")

			// read after commit
			rresp, err = h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0]})
			assert.Nil(t, err, "txn1: Unexpected error during get request")
			assert.True(t, rresp.Found, "txn1: Error nil but found=false in get request")
			assert.Nil(t, rresp.Error, "txn1: Unexpected error resp during get request")
			assert.Equal(t, test.TestValues[0], rresp.Kv.Value, "txn1: get response value doesn't match with expected value")
		} else {
			rollResp, err := h.mvcc.RollbackTxn(context.Background(), &icecanedbpb.RollbackTxnRequest{TxnId: txn.TxnId})
			assert.Nil(t, err, "txn1: Unexpected error during rollback request")
			assert.True(t, rollResp.Success, "txn1: Error nil but success=false in rollback request")
		}
	}(txn)

	// txn2
	go func(txn *icecanedbpb.BeginTxnResponse) {
		defer wg.Done()

		// write
		sresp, err := h.mvcc.Set(context.Background(), &icecanedbpb.SetRequest{TxnId: txn.TxnId, Key: test.TestKeys[0], Value: test.TestValues[1]})
		if err != nil && sresp.Success {
			successTwo = true

			assert.Nil(t, err, "txn2: Unexpected error during set request")
			assert.True(t, sresp.Success, "txn2: Error nil and success=false in set request")
			assert.Nil(t, sresp.Error, "txn2: Unexpected error resp during set request")

			// read
			rresp, err := h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0], TxnId: txn.TxnId})
			assert.Nil(t, err, "txn2: Unexpected error during get request")
			assert.True(t, rresp.Found, "txn2: Error nil and found=false in get request")
			assert.Nil(t, rresp.Error, "txn2: Unexpected error resp during get request")
			assert.Equal(t, test.TestValues[1], rresp.Kv.Value, "txn2: get response value doesn't match with expected value")

			// commit
			cresp, err := h.mvcc.CommitTxn(context.Background(), &icecanedbpb.CommitTxnRequest{TxnId: txn.TxnId})
			assert.Nil(t, err, "Unexpected error during commit request")
			assert.True(t, cresp.Success, "Error nil and success=false in commit request")

			// read after commit without passing txn id
			rresp, err = h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0]})
			assert.Nil(t, err, "txn2: Unexpected error during get request")
			assert.True(t, rresp.Found, "txn2: Error nil and found=false in get request")
			assert.Nil(t, rresp.Error, "txn2: Unexpected error resp during get request")
			assert.Equal(t, test.TestValues[1], rresp.Kv.Value, "txn2: get response value doesn't match with expected value")
		} else {
			rollResp, err := h.mvcc.RollbackTxn(context.Background(), &icecanedbpb.RollbackTxnRequest{TxnId: txn.TxnId})
			assert.Nil(t, err, "txn2: Unexpected error during rollback request")
			assert.True(t, rollResp.Success, "txn2: Error nil and success=false in rollback request")
		}
	}(txn2)

	wg.Wait()

	assert.True(t, successOne || successTwo, "both txns failed to commit")
	assert.False(t, successOne && successTwo, "two conflicting txns successfully committed")

	if successOne {
		rresp, err := h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0]})
		assert.Nil(t, err, "Unexpected error during get request")
		assert.True(t, rresp.Found, "Error nil but found=false in get request")
		assert.Nil(t, rresp.Error, "Unexpected error resp during get request")
		assert.Equal(t, test.TestValues[0], rresp.Kv.Value, "get response value doesn't match with expected value")
	} else {
		rresp, err := h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[1]})
		assert.Nil(t, err, "Unexpected error during get request")
		assert.True(t, rresp.Found, "Error nil but found=false in get request")
		assert.Nil(t, rresp.Error, "Unexpected error resp during get request")
		assert.Equal(t, test.TestValues[1], rresp.Kv.Value, "get response value doesn't match with expected value")
	}
}

func TestSnapshotIsolation(t *testing.T) {
	h := newMvccTestHarness(testDirectory, true)
	err := h.init()
	assert.Nil(t, err, "Unexpected error while initiating test harness")
	defer h.cleanup()

	// write
	sresp, err := h.mvcc.Set(context.Background(), &icecanedbpb.SetRequest{Key: test.TestKeys[0], Value: test.TestValues[0]})
	assert.Nil(t, err, "Unexpected error during set request")
	assert.True(t, sresp.Success, "Error nil but success=false in set request")
	assert.Nil(t, sresp.Error, "Unexpected error resp during set request")

	// begin txn
	txn, err := h.mvcc.BeginTxn(context.Background(), &icecanedbpb.BeginTxnRequest{Mode: icecanedbpb.TxnMode_ReadWrite})
	assert.Nil(t, err, "txn1: Unexpected error while beginning a txn")

	// update after the txn begins
	sresp, err = h.mvcc.Set(context.Background(), &icecanedbpb.SetRequest{Key: test.TestKeys[0], Value: test.TestValues[1]})
	assert.Nil(t, err, "Unexpected error during set request")
	assert.True(t, sresp.Success, "Error nil but success=false in set request")
	assert.Nil(t, sresp.Error, "Unexpected error resp during set request")

	// read through the txn should read the older value due to snapshot isolation
	rresp, err := h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0], TxnId: txn.TxnId})
	assert.Nil(t, err, "Unexpected error during get request")
	assert.True(t, rresp.Found, "Error nil but found=false in get request")
	assert.Nil(t, rresp.Error, "Unexpected error resp during get request")
	assert.Equal(t, test.TestValues[0], rresp.Kv.Value, "get response value doesn't match with expected value")

	// read without the txn should read the latest value
	rresp, err = h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0]})
	assert.Nil(t, err, "Unexpected error during get request")
	assert.True(t, rresp.Found, "Error nil but found=false in get request")
	assert.Nil(t, rresp.Error, "Unexpected error resp during get request")
	assert.Equal(t, test.TestValues[1], rresp.Kv.Value, "get response value doesn't match with expected value")
}

// 100 concurrent transactions writing to non conflicting keys. They all should succeed
func TestTxn100NonConflictingTxns(t *testing.T) {
	h := newMvccTestHarness(testDirectory, true)
	err := h.init()
	assert.Nil(t, err, "Unexpected error while initiating test harness")
	defer h.cleanup()

	testKeys := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		testKeys[i] = []byte(fmt.Sprintf("testkey%d", i))
	}

	for i := 0; i < 100; i++ {
		go func(idx int) {
			// begin txn
			txn, err := h.mvcc.BeginTxn(context.Background(), &icecanedbpb.BeginTxnRequest{Mode: icecanedbpb.TxnMode_ReadWrite})
			assert.Nil(t, err, "Unexpected error while beginning a txn")

			// write
			sresp, err := h.mvcc.Set(context.Background(), &icecanedbpb.SetRequest{TxnId: txn.TxnId, Key: testKeys[idx], Value: test.TestValues[0]})
			assert.Nil(t, err, "Unexpected error during set request")
			assert.True(t, sresp.Success, "Error nil but success=false in set request")
			assert.Nil(t, sresp.Error, "Unexpected error resp during set request")

			// read
			rresp, err := h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: testKeys[idx], TxnId: txn.TxnId})
			assert.Nil(t, err, "Unexpected error during get request")
			assert.True(t, rresp.Found, "Error nil but found=false in get request")
			assert.Nil(t, rresp.Error, "Unexpected error resp during get request")
			assert.Equal(t, test.TestValues[0], rresp.Kv.Value, "get response value doesn't match with expected value")

			// update
			sresp, err = h.mvcc.Set(context.Background(), &icecanedbpb.SetRequest{TxnId: txn.TxnId, Key: testKeys[idx], Value: test.TestValues[1]})
			assert.Nil(t, err, "Unexpected error during set request")
			assert.True(t, sresp.Success, "Error nil but success=false in set request")
			assert.Nil(t, sresp.Error, "Unexpected error resp during set request")

			// read updated value
			rresp, err = h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: testKeys[idx], TxnId: txn.TxnId})
			assert.Nil(t, err, "Unexpected error during get request")
			assert.True(t, rresp.Found, "Error nil but found=false in get request")
			assert.Nil(t, rresp.Error, "Unexpected error resp during get request")
			assert.Equal(t, test.TestValues[1], rresp.Kv.Value, "get response value doesn't match with expected value")

			// delete
			dresp, err := h.mvcc.Delete(context.Background(), &icecanedbpb.DeleteRequest{TxnId: txn.TxnId, Key: testKeys[idx]})
			assert.Nil(t, err, "Unexpected error during delete request")
			assert.True(t, dresp.Success, "Error nil but success=false in delete request")
			assert.Nil(t, dresp.Error, "Unexpected error resp during delete request")

			// read after deleting value
			rresp, err = h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: testKeys[idx], TxnId: txn.TxnId})
			assert.Nil(t, err, "Unexpected error during get request")
			assert.False(t, rresp.Found, "Error nil but found=true in get request. Expected value to not be found")
			assert.Nil(t, rresp.Error, "Unexpected error resp during get request")

			// commit
			cresp, err := h.mvcc.CommitTxn(context.Background(), &icecanedbpb.CommitTxnRequest{TxnId: txn.TxnId})
			assert.Nil(t, err, "Unexpected error during commit request")
			assert.True(t, cresp.Success, "Error nil but success=false in commit request")

			// read after commit without passing txn id
			rresp, err = h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: testKeys[idx]})
			assert.Nil(t, err, "Unexpected error during get request")
			assert.False(t, rresp.Found, "Error nil but found=true in get request. expected value to not be found")
			assert.Nil(t, rresp.Error, "Unexpected error resp during get request")
		}(i)
	}
}
