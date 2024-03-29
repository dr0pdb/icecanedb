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
	"github.com/dr0pdb/icecanedb/pkg/frontend"
	log "github.com/sirupsen/logrus"
)

/*
	We store table schema in the kv-store.

	The format:
	Key: table name
	Value: id (64 bits) | number of columns (64 bits) | col_1 | col_2 | ...

	Apart from creating the table, we also create a set of indices.

	The primary index is implicit because we store table rows with key = tablePrefix_tableID_rowPrefix_rowID

	For other indices, the index id = id of the column on which it is based.
	If the values in the index are unique:
		index key = indexPrefix_indexID_valuePrefix_valueID
	Else:
		index key = TODO
*/

// CreateTableExecutor is the executor for the create table query
type CreateTableExecutor struct {
	rpc   *rpcRepository
	Table *frontend.TableSpec
}

var _ Executor = (*CreateTableExecutor)(nil)

// Execute executes the create table request
func (ex *CreateTableExecutor) Execute(txnID uint64) Result {
	log.Info("icecanesql::ddl_executor::CreateTableExecutor.Execute; start;")
	res := &CreateTableResult{}

	tableID, err := getNextTableID(ex.rpc, txnID)
	if err != nil {
		res.Err = err
		return res
	}
	k, v, err := encodeTableSchema(ex.Table, tableID)
	if err != nil {
		res.Err = err
		return res
	}

	_, res.Err = ex.rpc.set(k, v, txnID)

	// TODO: handle foreign keys, referential integrity, indices

	return res
}

type DropTableExecutor struct {
	rpc       *rpcRepository
	TableName string
}

var _ Executor = (*DropTableExecutor)(nil)

// Execute executes the drop table request
func (ex *DropTableExecutor) Execute(txnID uint64) Result {
	log.Info("icecanesql::ddl_executor::DropTableExecutor.Execute; start;")
	res := &DropTableResult{}

	tableID, err := getTableID(ex.rpc, ex.TableName, txnID)
	if err != nil {
		res.Err = err
		return res
	}

	tSpec := &frontend.TableSpec{TableID: tableID, Columns: []*frontend.ColumnSpec{}}
	k, _, err := encodeTableSchema(tSpec, tableID)
	if err != nil {
		res.Err = err
		return res
	}

	_, res.Err = ex.rpc.delete(k, txnID)

	// todo: delete the rows

	return res
}

type TruncateTableExecutor struct {
	rpc       *rpcRepository
	TableName string
}

var _ Executor = (*TruncateTableExecutor)(nil)

// Execute executes the drop table request
func (ex *TruncateTableExecutor) Execute(txnID uint64) Result {
	log.Info("icecanesql::ddl_executor::TruncateTableExecutor.Execute; start;")
	res := &TruncateTableResult{}

	// todo: after implementing inserts

	return res
}

// getTableID gets the id of the table with the given name
func getTableID(rpc *rpcRepository, name string, txnID uint64) (uint64, error) {
	panic("")
}

// getNextTableID returns the next unique table id from kv store
func getNextTableID(rpc *rpcRepository, txnID uint64) (uint64, error) {
	return incrementKeyAtomic(rpc, NextTableIDKey)
}
