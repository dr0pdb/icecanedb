package icecanesql

import "github.com/dr0pdb/icecanedb/pkg/frontend"

// planner derives the execution plan of a sql query
type planner struct {
	stmt frontend.Statement

	res PlanNode
	err error // errors encountered during the process
}

// plan the execution
func (p *planner) plan() *planner {
	switch st := p.stmt.(type) {
	case *frontend.CreateTableStatement:
		p.res = &CreateTablePlanNode{
			Schema: st.Spec,
		}

	case *frontend.DropTableStatement:
		p.res = &DropTablePlanNode{
			TableName: st.TableName,
		}

	case *frontend.TruncateTableStatement:
		p.res = &TruncateTablePlanNode{
			TableName: st.TableName,
		}

	case *frontend.BeginTxnStatement:
		p.res = &BeginTxnPlanNode{ReadOnly: st.ReadOnly}

	case *frontend.FinishTxnStatement:
		p.res = &FinishTxnPlanNode{IsCommit: st.IsCommit}

	case *frontend.InsertStatement:
		p.res = &InsertPlanNode{
			TableName: st.Table.Name,
			Columns:   st.Columns,
			Values:    st.Values,
		}
	}

	return p
}

// optimize optimizes the plan
func (p *planner) optimize() *planner {
	return p
}

// get returns the final plan
func (p *planner) get() (PlanNode, error) {
	return p.res, p.err
}

// newPlanner creates a new planner for the given statement
func newPlanner(stmt frontend.Statement) *planner {
	return &planner{
		stmt: stmt,
	}
}
