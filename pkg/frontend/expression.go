package frontend

var (
	_ Expression = (*ExpressionNode)(nil)
)

// ExpressionNode implements the Expression interface
type ExpressionNode struct {
	Type FieldType
}

func (e *ExpressionNode) Accept(v Visitor) (node Node, ok bool) {
	panic("")
}

func (e *ExpressionNode) expression() {}

// ValueExpression represents a simple value
type ValueExpression struct {
	ExpressionNode

	Val *Value
}

// BinaryOpExpression represents a binary operation
type BinaryOpExpression struct {
	ExpressionNode

	L, R ExpressionNode
}
