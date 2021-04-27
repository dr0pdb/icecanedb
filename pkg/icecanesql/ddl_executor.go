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
func (ex *CreateTableExecutor) Execute() Result {
	log.Info("icecanesql::ddl_executor::Execute; start;")
	res := &CreateTableResult{}

	k, v, err := encodeTableSchema(ex.Table, 1)
	if err != nil {
		res.err = err
		return res
	}

	res.success, res.err = ex.rpc.set(k, v)
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
