package icecanesql

import (
	"github.com/dr0pdb/icecanedb/pkg/frontend"
	log "github.com/sirupsen/logrus"
)

const (
	NoTxn uint64 = 0
)

// CreateTableExecutor is the executor for the create table query
type CreateTableExecutor struct {
	rpc   *rpcRepository
	Table *frontend.TableSpec
}

var _ Executor = (*CreateTableExecutor)(nil)

// Execute executes the create table request
func (ex *CreateTableExecutor) Execute() Result {
	log.Info("icecanesql::ddl_executor::CreateTableExecutor.Execute; start;")
	res := &CreateTableResult{}

	// begin a txn

	// todo: get a unique table id for the table

	k, v, err := encodeTableSchema(ex.Table, 1)
	if err != nil {
		res.err = err
		return res
	}

	res.success, res.err = ex.rpc.set(k, v, NoTxn)

	// todo: create index for primary key and others if specified

	return res
}

type DropTableExecutor struct {
	rpc       *rpcRepository
	TableName string
}

var _ Executor = (*DropTableExecutor)(nil)

// Execute executes the drop table request
func (ex *DropTableExecutor) Execute() Result {
	log.Info("icecanesql::ddl_executor::DropTableExecutor.Execute; start;")
	res := &DropTableResult{}

	return res
}

type TruncateTableExecutor struct {
	rpc       *rpcRepository
	TableName string
}

var _ Executor = (*TruncateTableExecutor)(nil)

// Execute executes the drop table request
func (ex *TruncateTableExecutor) Execute() Result {
	log.Info("icecanesql::ddl_executor::TruncateTableExecutor.Execute; start;")
	res := &TruncateTableResult{}

	return res
}

// CreateTableResult is the result of the create table operation
type CreateTableResult struct {
	success bool
	err     error
}

func (ctr *CreateTableResult) getError() error {
	return ctr.err
}

var _ Result = (*CreateTableResult)(nil)

// DropTableResult is the result of the drop table operation
type DropTableResult struct {
	success bool
	err     error
}

func (ctr *DropTableResult) getError() error {
	return ctr.err
}

var _ Result = (*DropTableResult)(nil)

// TruncateTableResult is the result of the truncate table operation
type TruncateTableResult struct {
	success bool
	err     error
}

func (ctr *TruncateTableResult) getError() error {
	return ctr.err
}

var _ Result = (*TruncateTableResult)(nil)
