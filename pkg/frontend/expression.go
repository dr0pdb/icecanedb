package frontend

var (
	_ Expression = (*expressionNode)(nil)
)

// expressionNode implements the Expression interface
type expressionNode struct {
	typ FieldType
}

func (e *expressionNode) Accept(v Visitor) (node Node, ok bool) {
	panic("")
}

func (e *expressionNode) expression() {}

type BetweenExpression struct {
	expressionNode

	Expr expressionNode

	Min, Max expressionNode
}

// BinaryOpExpression represents a binary operation
type BinaryOpExpression struct {
	expressionNode

	Op Operator

	L, R expressionNode
}

type UnaryOpExpression struct {
	expressionNode

	Op Operator

	Exp expressionNode
}

// ValueExpression represents a simple value
type ValueExpression struct {
	expressionNode

	Val *Value
}
