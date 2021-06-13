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
			res.Err = err
			return res
		}

		txnID = startedTxnID
		inlineTxn = true
	}

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

	res.Success, res.Err = ex.rpc.set(k, v, txnID)

	// todo: create index for others columns if specified

	// commit the inline txn
	if inlineTxn {
		err = ex.rpc.commitTxn(txnID)
		if err != nil {
			res.Err = err
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
	inlineTxn := false

	if txnID == NoTxn {
		startedTxnID, err := ex.rpc.beginTxn(false)
		if err != nil {
			res.Err = err
			return res
		}

		txnID = startedTxnID
		inlineTxn = true
	}

	tableID, err := getTableID(ex.rpc, ex.TableName, txnID)
	if err != nil {
		res.Err = err
		return res
	}

	tSpec := &frontend.TableSpec{TableId: tableID, Columns: []*frontend.ColumnSpec{}}
	k, _, err := encodeTableSchema(tSpec, tableID)
	if err != nil {
		res.Err = err
		return res
	}

	res.Success, res.Err = ex.rpc.delete(k, txnID)

	// todo: delete the rows

	// commit the inline txn
	if inlineTxn {
		err = ex.rpc.commitTxn(txnID)
		if err != nil {
			res.Err = err
			ex.rpc.rollbackTxn(txnID) // todo: what if this also fails? retry?
		}
	}

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

// CreateTableResult is the result of the create table operation
type CreateTableResult struct {
	Success bool
	Err     error
}

func (ctr *CreateTableResult) GetError() error {
	return ctr.Err
}

var _ Result = (*CreateTableResult)(nil)

// DropTableResult is the result of the drop table operation
type DropTableResult struct {
	Success bool
	Err     error
}

func (ctr *DropTableResult) GetError() error {
	return ctr.Err
}

var _ Result = (*DropTableResult)(nil)

// TruncateTableResult is the result of the truncate table operation
type TruncateTableResult struct {
	Success bool
	Err     error
}

func (ctr *TruncateTableResult) GetError() error {
	return ctr.Err
}

var _ Result = (*TruncateTableResult)(nil)
