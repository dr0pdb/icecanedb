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
	"github.com/dr0pdb/icecanedb/pkg/common"
	"github.com/dr0pdb/icecanedb/pkg/frontend"
)

// encodeTableSchema returns the key-value pair to store in the kv service
// to store a table schema.
//
// The format:
// Key: table name
// Value: id (64 bits) | number of columns (64 bits) | col_1 | col_2 | ...
func encodeTableSchema(table *frontend.TableSpec, id uint64) (key, value []byte, err error) {
	key = []byte(table.TableName)

	value = common.U64ToByte(id)
	value = append(value, common.U64ToByte(uint64(len(table.Columns)))...)

	for i := 0; i < len(table.Columns); i++ {
		value = append(value, encodeColumnSpec(table.Columns[i])...)
	}

	return key, value, err
}

// encodeString encodes the string to bytes
func encodeString(s string) []byte {
	res := make([]byte, 0)

	res = append(res, common.U64ToByte(uint64(len(s)))...)
	res = append(res, []byte(s)...)

	return res
}

type columnSpecFieldMarker uint64

const (
	columnSpecFieldName columnSpecFieldMarker = iota
	columnSpecFieldType
	columnSpecFieldNullable
	columnSpecFieldPrimaryKey
	columnSpecFieldUnique
	columnSpecFieldIndex
	columnSpecFieldReferences
)

// encodeColumnSpec encodes a column spec to byte slice
func encodeColumnSpec(cs *frontend.ColumnSpec) []byte {
	res := make([]byte, 0)

	// encode name
	res = append(res, common.U64ToByte(uint64(columnSpecFieldName))...)
	res = append(res, encodeString(cs.Name)...)

	// encode type
	res = append(res, common.U64ToByte(uint64(columnSpecFieldType))...)
	res = append(res, common.U64ToByte(uint64(cs.Type))...)

	// encode nullable
	res = append(res, common.U64ToByte(uint64(columnSpecFieldNullable))...)
	res = append(res, common.BoolToByte(cs.Nullable))

	// encode primary key
	res = append(res, common.U64ToByte(uint64(columnSpecFieldPrimaryKey))...)
	res = append(res, common.BoolToByte(cs.PrimaryKey))

	// encode unique
	res = append(res, common.U64ToByte(uint64(columnSpecFieldUnique))...)
	res = append(res, common.BoolToByte(cs.Unique))

	// encode index
	res = append(res, common.U64ToByte(uint64(columnSpecFieldIndex))...)
	res = append(res, common.BoolToByte(cs.Index))

	// encode references
	res = append(res, common.U64ToByte(uint64(columnSpecFieldReferences))...)
	res = append(res, encodeString(cs.References)...)

	return res
}
