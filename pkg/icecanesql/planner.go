package icecanesql

import "github.com/dr0pdb/icecanedb/pkg/frontend"

// planner derives the execution plan of a sql query
type planner struct {
	stmt frontend.Statement

	err error // errors encountered during the process
}

// plan the execution
func (p *planner) plan() *planner {

	return p
}

// optimize optimizes the plan
func (p *planner) optimize() *planner {
	return p
}

// get returns the final plan
func (p *planner) get() error {
	return p.err
}

// newPlanner creates a new planner for the given statement
func newPlanner(stmt frontend.Statement) *planner {
	return &planner{
		stmt: stmt,
	}
}
