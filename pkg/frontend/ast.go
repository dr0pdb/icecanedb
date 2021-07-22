/**
 * Copyright 2020 The IcecaneDB Authors. All rights reserved.
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

/*
	This file contains the common defs and utilities of the AST
*/

var (
	_ Statement = (*CreateTableStatement)(nil)
	_ Statement = (*DropTableStatement)(nil)
	_ Statement = (*TruncateTableStatement)(nil)
	_ Statement = (*BeginTxnStatement)(nil)
	_ Statement = (*FinishTxnStatement)(nil)
)

// A single node on the syntax tree
type Node interface {
	Accept(v Visitor) (node Node, ok bool)
}

// Statement denotes a parsed SQL statement
type Statement interface {
	Node
	statement()
}

// Expression denotes an expression which can be evaluated
type Expression interface {
	Node
	expression()
}

type Visitor interface {
	Visit(n Node) (node Node, skipChildren bool)
}

// FromItem is a source that can be used for SELECT statements
type FromItem interface{}

type JoinType uint64

const (
	Cross JoinType = iota
	Inner
	Left
	Right
)

var (
	_ FromItem = (*Table)(nil)
	_ FromItem = (*Join)(nil)
)

// Table denotes a single table
type Table struct {
	Name  string
	Alias string
}

// Join denotes a join table operation.
// It could recursively contain joins.
type Join struct {
	Left      FromItem
	Right     FromItem
	JoinType  JoinType
	Predicate Expression
}

// SelectionItem denotes a single column of selection in a SELECT query.
// Can optionally have an Output name.
type SelectionItem struct {
	OutputName string
	Expr       Expression
}

// ExplainStatement denotes an EXPLAIN query
type ExplainStatement struct {
	InnerStatement Statement
}

func (es *ExplainStatement) Accept(v Visitor) (node Node, ok bool) {
	panic("")
}

func (es *ExplainStatement) statement() {}
