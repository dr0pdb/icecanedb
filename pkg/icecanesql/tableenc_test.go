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

package icecanesql

import (
	"testing"

	"github.com/dr0pdb/icecanedb/pkg/frontend"
	"github.com/stretchr/testify/assert"
)

func TestEncodeTableSchema(t *testing.T) {
	table := &frontend.TableSpec{
		TableName: "TestTable",
		Columns: []*frontend.ColumnSpec{
			{
				Name:       "col1",
				Type:       frontend.FieldTypeInteger,
				PrimaryKey: true,
				Nullable:   false,
				Unique:     true,
				Index:      true,
			},
			{
				Name:       "col2",
				Type:       frontend.FieldTypeBoolean,
				PrimaryKey: false,
				Nullable:   true,
				Unique:     false,
				Index:      false,
			},
			{
				Name:       "col3",
				Type:       frontend.FieldTypeFloat,
				PrimaryKey: false,
				Nullable:   true,
				Unique:     false,
				Index:      false,
			},
			{
				Name:       "col4",
				Type:       frontend.FieldTypeString,
				PrimaryKey: false,
				Nullable:   true,
				Unique:     false,
				Index:      false,
			},
		},
	}
	tableId := uint64(10)

	k, v, err := encodeTableSchema(table, tableId)
	assert.Nil(t, err, "unexpected error in encoding table schema")

	decodedTable, err := decodeTableSchema(k, v)
	assert.Nil(t, err, "unexpected error in decoding table schema")

	assert.Equal(t, table.TableName, decodedTable.TableName, "Unexpected value for table name")
	assert.Equal(t, tableId, decodedTable.TableId, "Unexpected value for table id")
	assert.Equal(t, len(table.Columns), len(decodedTable.Columns), "Unexpected length for columns list")

	for i := 0; i < len(table.Columns); i++ {
		assert.Equal(t, table.Columns[i], decodedTable.Columns[i], "column spec doesn't  match")
	}
}
