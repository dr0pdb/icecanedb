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
	"testing"

	"github.com/dr0pdb/icecanedb/pkg/protogen/icecanedbpb"
	"github.com/dr0pdb/icecanedb/test"
	"github.com/stretchr/testify/assert"
)

var (
	testDirectory = "/tmp/icecanetesting/"
)

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
	assert.Equal(t, "", sresp.Error, "Unexpected error resp during set request")

	// read
	rresp, err := h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0], TxnId: txn.TxnId})
	assert.Nil(t, err, "Unexpected error during get request")
	assert.True(t, rresp.Found, "Error nil but found=false in get request")
	assert.Equal(t, "", rresp.Error, "Unexpected error resp during get request")
	assert.Equal(t, test.TestValues[0], rresp.Value, "get response value doesn't match with expected value")

	// commit
	cresp, err := h.mvcc.CommitTxn(context.Background(), &icecanedbpb.CommitTxnRequest{TxnId: txn.TxnId})
	assert.Nil(t, err, "Unexpected error during commit request")
	assert.True(t, cresp.Success, "Error nil but success=false in commit request")

	// read after commit without passing txn id
	rresp, err = h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0]})
	assert.Nil(t, err, "Unexpected error during get request")
	assert.True(t, rresp.Found, "Error nil but found=false in get request")
	assert.Equal(t, "", rresp.Error, "Unexpected error resp during get request")
	assert.Equal(t, test.TestValues[0], rresp.Value, "get response value doesn't match with expected value")
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
	assert.Equal(t, "", sresp.Error, "Unexpected error resp during set request")

	// read
	rresp, err := h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0], TxnId: txn.TxnId})
	assert.Nil(t, err, "Unexpected error during get request")
	assert.True(t, rresp.Found, "Error nil but found=false in get request")
	assert.Equal(t, "", rresp.Error, "Unexpected error resp during get request")
	assert.Equal(t, test.TestValues[0], rresp.Value, "get response value doesn't match with expected value")

	// rollback
	rollResp, err := h.mvcc.RollbackTxn(context.Background(), &icecanedbpb.RollbackTxnRequest{TxnId: txn.TxnId})
	assert.Nil(t, err, "Unexpected error during rollback request")
	assert.True(t, rollResp.Success, "Error nil but success=false in rollback request")

	// read after rollback without passing txn id
	rresp, err = h.mvcc.Get(context.Background(), &icecanedbpb.GetRequest{Key: test.TestKeys[0]})
	assert.Nil(t, err, "Unexpected error during get request")
	assert.False(t, rresp.Found, "Error nil but found=true in get request when expected false")
	assert.Equal(t, "", rresp.Error, "Unexpected error resp during get request")
	assert.Equal(t, []byte(nil), rresp.Value, "get response value doesn't match with expected value")
}
