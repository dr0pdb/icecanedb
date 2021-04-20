package icecanesql

// Executor executes a query plan
type Executor interface {
	Execute(PlanNode) Result
}

// Result denotes the result of the execution of a query plan
type Result interface{}
