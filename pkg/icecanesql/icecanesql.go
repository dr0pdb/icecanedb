package icecanesql

import "github.com/dr0pdb/icecanedb/pkg/frontend"

type Client struct {
	name     string
	leaderId uint64
}

// Execute executes a sql command obtained from the REPL.
// TODO: decide return types
func (c *Client) Execute(cmd string) error {
	p := frontend.NewParser(c.name, cmd)
	stmt, err := p.Parse()
	if err != nil {
		return err
	}

	// derive the query plan
	_, err = newPlanner(stmt).plan().optimize().get()
	if err != nil {
		return err
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
