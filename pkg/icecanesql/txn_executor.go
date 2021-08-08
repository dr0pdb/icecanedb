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

package icecanesql

import log "github.com/sirupsen/logrus"

type BeginTxnExecutor struct {
	rpc      *rpcRepository
	readOnly bool
}

var _ Executor = (*BeginTxnExecutor)(nil)

func (ex *BeginTxnExecutor) Execute(_txnID uint64) Result {
	log.Info("icecanesql::txn_executor::BeginTxnExecutor.Execute; start;")
	res := &BeginTxnResult{}

	txnID, err := ex.rpc.beginTxn(ex.readOnly)
	if err != nil {
		res.Err = err
		return res
	}

	res.TxnID = txnID
	return res
}

type FinishTxnExecutor struct {
	rpc      *rpcRepository
	isCommit bool
}

var _ Executor = (*FinishTxnExecutor)(nil)

func (ex *FinishTxnExecutor) Execute(txnID uint64) Result {
	log.Info("icecanesql::txn_executor::FinishTxnExecutor.Execute; start;")
	res := &FinishTxnResult{}

	var err error
	if ex.isCommit {
		err = ex.rpc.commitTxn(txnID)
	} else {
		err = ex.rpc.rollbackTxn(txnID)
	}

	res.Err = err
	return res
}
