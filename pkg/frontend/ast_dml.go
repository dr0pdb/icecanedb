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
	_ Statement = (*InsertStatement)(nil)
	_ Statement = (*UpdateStatement)(nil)
	_ Statement = (*DeleteStatement)(nil)
)

// InsertStatement is for the INSERT statement.
// TODO: support multiple set of values
type InsertStatement struct {
	Table   *Table
	Columns []string
	Values  []Expression
}

func (cts *InsertStatement) Accept(v Visitor) (node Node, ok bool) {
	panic("")
}

func (cts *InsertStatement) statement() {}

// UpdateStatement is for the UPDATE statement.
type UpdateStatement struct {
	Table     *Table
	Predicate Expression   // must evaluate to a boolean
	Values    []Expression // list of column_name = expression
}

func (cts *UpdateStatement) Accept(v Visitor) (node Node, ok bool) {
	panic("")
}

func (cts *UpdateStatement) statement() {}

// DeleteStatement is for the DELETE statement.
type DeleteStatement struct {
	Table     *Table
	Predicate Expression // must evaluate to a boolean
}

func (ds *DeleteStatement) Accept(v Visitor) (node Node, ok bool) {
	panic("")
}

func (ds *DeleteStatement) statement() {}
