package icecanesql

import (
	"github.com/dr0pdb/icecanedb/pkg/frontend"
	log "github.com/sirupsen/logrus"
)

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
	inlineTxn := false

	if txnID == NoTxn {
		startedTxnID, err := ex.rpc.beginTxn(false)
		if err != nil {
			res.err = err
			return res
		}

		txnID = startedTxnID
		inlineTxn = true
	}

	tableID := getNextTableID(txnID)
	k, v, err := encodeTableSchema(ex.Table, tableID)
	if err != nil {
		res.err = err
		return res
	}

	res.success, res.err = ex.rpc.set(k, v, txnID)

	// todo: create index for primary key and others if specified

	// commit the inline txn
	if inlineTxn {
		err = ex.rpc.commitTxn(txnID)
		if err != nil {
			res.err = err
			ex.rpc.rollbackTxn(txnID) // todo: what if this also fails? retry?
		}
	}

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

	return res
}

// getNextTableID returns the next unique table id from kv store
func getNextTableID(txnID uint64) uint64 {
	panic("todo")
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
