/**
 * Copyright 2020 The IcecaneDB Authors. All rights reserved.
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
	"fmt"

	"github.com/dr0pdb/icecanedb/pkg/common"
	"github.com/dr0pdb/icecanedb/pkg/frontend"
	log "github.com/sirupsen/logrus"
)

const (
	NoTxn uint64 = 0
)

// Client handles query parsing, planning and execution.
// TODO: Does it need a mutex?
type Client struct {
	name    string
	rpc     *rpcRepository
	conf    *common.ClientConfig
	catalog *catalog
}

// Execute executes a sql command obtained from the grpc service.
// TODO: decide return types and also take txn id as argument
func (c *Client) Execute(cmd string, txnID uint64) error {
	log.WithFields(log.Fields{"name": c.name, "cmd": cmd}).Info("icecanesql::icecanesql::Execute; starting execution of command;")
	p := frontend.NewParser(c.name, cmd)
	stmt, err := p.Parse()
	if err != nil {
		return err
	}

	// validate the parsed statement
	err = newValidator(stmt, c.catalog).validate(txnID)
	if err != nil {
		return err
	}

	// derive the query plan
	pn, err := newPlanner(stmt, c.catalog, txnID).plan().optimize().get()
	if err != nil {
		return err
	}

	// execute the plan node
	_ = c.getExecutor(pn).Execute(txnID)
	return nil
}

func (c *Client) getExecutor(pn PlanNode) Executor {
	log.Info("icecanesql::icecanesql::getExecutor; start;")

	switch n := pn.(type) {
	case *CreateTablePlanNode:
		ex := &CreateTableExecutor{rpc: c.rpc, Table: n.Schema}
		return ex

	case *DropTablePlanNode:
		ex := &DropTableExecutor{rpc: c.rpc, TableName: n.TableName}
		return ex

	case *TruncateTablePlanNode:
		ex := &TruncateTableExecutor{rpc: c.rpc, TableName: n.TableName}
		return ex

	case *BeginTxnPlanNode:
		ex := &BeginTxnExecutor{rpc: c.rpc, readOnly: n.ReadOnly}
		return ex

	case *FinishTxnPlanNode:
		ex := &FinishTxnExecutor{rpc: c.rpc, isCommit: n.IsCommit}
		return ex

	case *InsertPlanNode:
		ex := &InsertExecutor{rpc: c.rpc, plan: n, catalog: c.catalog}
		return ex
	}

	panic(fmt.Sprintf("programming error: No executor found for the plan node: %V", pn))
}

// NewClient creates a new client for running sql queries.
func NewClient(name string, conf *common.ClientConfig) *Client {
	rpc := newRpcRepository(conf)

	return &Client{
		name:    name,
		rpc:     rpc,
		conf:    conf,
		catalog: newCatalog(rpc),
	}
}
