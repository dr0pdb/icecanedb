package icecanesql

import (
	"github.com/dr0pdb/icecanedb/pkg/frontend"
	log "github.com/sirupsen/logrus"
)

type CreateTableExecutor struct {
	Table *frontend.TableSpec
}

var _ Executor = (*CreateTableExecutor)(nil)

func (ex *CreateTableExecutor) Execute(p PlanNode) Result {
	log.Info("icecanesql::ddl_executor::Execute; start;")
	return nil
}

type CreateTableResult struct{}

var _ Result = (*CreateTableResult)(nil)
