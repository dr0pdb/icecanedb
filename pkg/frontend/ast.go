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

// Statement denotes a parsed SQL statement
type Statement interface {
}

var _ Statement = (*CreateTableStatement)(nil)
var _ Statement = (*DropTableStatement)(nil)
var _ Statement = (*TruncateTableStatement)(nil)
var _ Statement = (*BeginTxnStatement)(nil)
var _ Statement = (*FinishTxnStatement)(nil)

// DDLStatement is a data definition query.
// It could be Create/Drop of tables
type CreateTableStatement struct {
	Spec *TableSpec
}

type DropTableStatement struct {
	TableName string
}

type TruncateTableStatement struct {
	TableName string
}

type BeginTxnStatement struct{}

// FinishTxnStatement denotes a COMMIT/ROLLBACK statement
type FinishTxnStatement struct {
	IsCommit bool
}

// TableSpec defines the specification of a table
// TODO: add methods for getting column, primary key etc.
type TableSpec struct {
	TableId   uint64 // internal id of the table. unique
	TableName string
	Columns   []*ColumnSpec
}

// ColumnSpec defines a single column of a table
// TODO: consider supporting defaults after supporting values
type ColumnSpec struct {
	Name       string
	Type       ColumnType
	Nullable   bool
	PrimaryKey bool
	Unique     bool
	Index      bool
	References string // the foreign key reference
}

type ColumnType uint64

const (
	ColumnTypeBoolean ColumnType = iota
	ColumnTypeInteger
	ColumnTypeString
	ColumnTypeFloat
)
