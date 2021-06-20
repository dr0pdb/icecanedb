package frontend

type BeginTxnStatement struct {
	ReadOnly bool
}

func (bts *BeginTxnStatement) Accept(v Visitor) (node Node, ok bool) {
	panic("")
}

func (bts *BeginTxnStatement) statement() {}

// FinishTxnStatement denotes a COMMIT/ROLLBACK statement
type FinishTxnStatement struct {
	IsCommit bool
}

func (fts *FinishTxnStatement) Accept(v Visitor) (node Node, ok bool) {
	panic("")
}

func (fts *FinishTxnStatement) statement() {}
