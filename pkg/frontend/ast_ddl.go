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

// DDLStatement is a data definition query.
// It could be Create/Drop of tables
type CreateTableStatement struct {
	Spec *TableSpec
}

func (cts *CreateTableStatement) Accept(v Visitor) (node Node, ok bool) {
	panic("")
}

func (cts *CreateTableStatement) statement() {}

type DropTableStatement struct {
	TableName string
}

func (dts *DropTableStatement) Accept(v Visitor) (node Node, ok bool) {
	panic("")
}

func (dts *DropTableStatement) statement() {}

type TruncateTableStatement struct {
	TableName string
}

func (tts *TruncateTableStatement) Accept(v Visitor) (node Node, ok bool) {
	panic("")
}

func (tts *TruncateTableStatement) statement() {}

// TableSpec defines the specification of a table
// TODO: add methods for getting column, primary key etc.
type TableSpec struct {
	TableID   uint64 // internal id of the table. unique
	TableName string
	Columns   []*ColumnSpec

	// The primary key column - guaranteed to have a valid value after validation step
	PrimaryKeyColumnIdx int
}

func NewTableSpec(id uint64, name string, cols []*ColumnSpec) *TableSpec {
	pk := -1
	for i := range cols {
		if cols[i].PrimaryKey {
			pk = i
		}
	}

	return &TableSpec{
		TableID:   id,
		TableName: name,
		Columns:   cols,

		PrimaryKeyColumnIdx: pk,
	}
}

func (s *TableSpec) IsPrimaryKeyInteger() bool {
	if s.PrimaryKeyColumnIdx != -1 {
		return s.Columns[s.PrimaryKeyColumnIdx].Type == FieldTypeInteger
	}

	panic("programming error: TODO")
}

// ColumnSpec defines a single column of a table
type ColumnSpec struct {
	Name       string
	Type       FieldType
	Nullable   bool
	PrimaryKey bool
	Unique     bool
	Index      bool
	References string // the foreign key reference
	Default    Expression
}
