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
	"github.com/dr0pdb/icecanedb/pkg/common"
	"github.com/dr0pdb/icecanedb/pkg/frontend"
	log "github.com/sirupsen/logrus"
)

type Client struct {
	name string
	rpc  *rpcRepository
	conf *common.ClientConfig
}

// Execute executes a sql command obtained from the REPL.
// TODO: decide return types
func (c *Client) Execute(cmd string) error {
	log.WithFields(log.Fields{"name": c.name, "cmd": cmd}).Info("icecanesql::icecanesql::Execute; starting execution of command;")
	p := frontend.NewParser(c.name, cmd)
	stmt, err := p.Parse()
	if err != nil {
		return err
	}

	// validate the parsed statement
	err = newValidator(stmt).validate()
	if err != nil {
		return err
	}

	// derive the query plan
	pn, err := newPlanner(stmt).plan().optimize().get()
	if err != nil {
		return err
	}

	// execute the plan node
	_ = c.getExecutor(pn).Execute()
	return nil
}

func (c *Client) getExecutor(pn PlanNode) Executor {
	log.Info("icecanesql::icecanesql::getExecutor; start;")

	switch n := pn.(type) {
	case *CreateTablePlanNode:
		ex := &CreateTableExecutor{rpc: c.rpc, Table: n.Schema}
		return ex
	}

	return nil
}

// NewClient creates a new client for running sql queries.
func NewClient(name string, conf *common.ClientConfig) *Client {
	return &Client{
		name: name,
		rpc:  newRpcRepository(conf),
		conf: conf,
	}
}
