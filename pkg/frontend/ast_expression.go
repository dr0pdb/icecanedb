package frontend

var (
	_ Expression = (*BetweenExpression)(nil)
	_ Expression = (*BinaryOpExpression)(nil)
	_ Expression = (*GroupingExpression)(nil)
	_ Expression = (*ValueExpression)(nil)
)

// ExpressionNode implements the Expression interface
type ExpressionNode struct {
	// Typ is the type of the final result of the expression
	Typ FieldType
}

func (e *ExpressionNode) expression() {}

type BetweenExpression struct {
	ExpressionNode

	Expr Expression

	Min, Max Expression
}

func (be *BetweenExpression) Accept(v Visitor) (node Node, ok bool) {
	panic("")
}

// BinaryOpExpression represents a binary operation
type BinaryOpExpression struct {
	ExpressionNode

	Op Operator

	L, R Expression
}

func (boe *BinaryOpExpression) Accept(v Visitor) (node Node, ok bool) {
	panic("")
}

// GroupingExpression represents a parenthesized expression
type GroupingExpression struct {
	ExpressionNode

	InExp Expression
}

func (ge *GroupingExpression) Accept(v Visitor) (node Node, ok bool) {
	panic("")
}

type UnaryOpExpression struct {
	ExpressionNode

	Op Operator

	Exp Expression
}

func (uoe *UnaryOpExpression) Accept(v Visitor) (node Node, ok bool) {
	panic("")
}

// ValueExpression represents a simple value
type ValueExpression struct {
	ExpressionNode

	Val *Value
}

func (ve *ValueExpression) Accept(v Visitor) (node Node, ok bool) {
	panic("")
}
