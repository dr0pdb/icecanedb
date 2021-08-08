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

import (
	log "github.com/sirupsen/logrus"
)

// InsertExecutor is the executor for the insert query
type InsertExecutor struct {
	rpc     *rpcRepository
	plan    *InsertPlanNode
	catalog *catalog
}

var _ Executor = (*InsertExecutor)(nil)

// Execute executes the insert statement
func (ex *InsertExecutor) Execute(txnID uint64) Result {
	log.Info("icecanesql::dml_executor::InsertExecutor.Execute; start;")

	res := &InsertResult{}

	spec, err := ex.catalog.getTableInfo(ex.plan.TableName, txnID)
	if err != nil {
		return nil
	}

	// validate uniqueness of columns which are supposed to be unique

	var rowKey []byte
	if spec.IsPrimaryKeyInteger() {
		rowKey = encodeTableRowKeyWithU64(spec, uint64(ex.plan.Values[spec.PrimaryKeyColumnIdx].Val.GetAsInt()))
	} else {
		rowKey = encodeTableRowKeyWithString(spec, ex.plan.Values[spec.PrimaryKeyColumnIdx].Val.GetAsString())
	}
	rowValue := encodeTableRowValues(spec, ex.plan.Values)

	_, res.Err = ex.rpc.set(rowKey, rowValue, txnID)

	// TODO: update indices?

	return res
}
