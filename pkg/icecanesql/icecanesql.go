package icecanesql

import (
	"github.com/dr0pdb/icecanedb/pkg/frontend"
	log "github.com/sirupsen/logrus"
)

type Client struct {
	name     string
	leaderId uint64
}

// Execute executes a sql command obtained from the REPL.
// TODO: decide return types
func (c *Client) Execute(cmd string) error {
	log.WithFields(log.Fields{"name": c.name, "leaderId": c.leaderId, "cmd": cmd}).Info("icecanesql::icecanesql::Execute; starting execution of command;")
	p := frontend.NewParser(c.name, cmd)
	stmt, err := p.Parse()
	if err != nil {
		return err
	}

	// derive the query plan
	pn, err := newPlanner(stmt).plan().optimize().get()
	if err != nil {
		return err
	}

	// execute the plan node
	ex := c.getExecutor(pn)
	_ = ex.Execute(pn)

	return nil
}

func (c *Client) getExecutor(pn PlanNode) Executor {
	log.Info("icecanesql::icecanesql::getExecutor; start;")

	switch n := pn.(type) {
	case *CreateTablePlanNode:
		ex := &CreateTableExecutor{Table: n.Schema}
		return ex
	}

	return nil
}

// NewClient creates a new client for running sql queries.
func NewClient(name string) *Client {
	return &Client{
		name:     name,
		leaderId: 1,
	}
}
