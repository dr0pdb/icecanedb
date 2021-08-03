/**
 * Copyright 2021 The IcecaneDB Authors. All rights reserved.
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

package frontend

var (
	_ Expression = (*BetweenExpression)(nil)
	_ Expression = (*BinaryOpExpression)(nil)
	_ Expression = (*GroupingExpression)(nil)
	_ Expression = (*ValueExpression)(nil)
	_ Expression = (*UnaryOpExpression)(nil)
	_ Expression = (*IdentifierExpression)(nil)
	_ Expression = (*SelectAllExpression)(nil)
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

// UnaryOpExpression represents a unary operation
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

// IdentifierExpression represents a simple identifier
type IdentifierExpression struct {
	ExpressionNode

	Identifier string
}

func (ie *IdentifierExpression) Accept(v Visitor) (node Node, ok bool) {
	panic("")
}

// SelectAllExpression represents selection of all of the columns in the query
type SelectAllExpression struct {
	ExpressionNode
}

func (ie *SelectAllExpression) Accept(v Visitor) (node Node, ok bool) {
	panic("")
}
