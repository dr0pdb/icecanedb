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
	"fmt"

	"github.com/dr0pdb/icecanedb/pkg/common"
	"github.com/dr0pdb/icecanedb/pkg/frontend"
)

var (
	tablePrefix = []byte("tp")
	rowPrefix   = []byte("rp")
)

type valueMarker uint64

const (
	valueMarkerBoolean valueMarker = iota
	valueMarkerInteger
	valueMarkerString
	valueMarkerFloat
	valueMarkerNull
)

// return the key that can be used to store a row of data for the given table
//
// The format:
// Key: tablePrefix_tableID_rowPrefix_rowID
func encodeTableRowKeyWithU64(table *frontend.TableSpec, rowID uint64) []byte {
	res := tablePrefix
	res = append(res, common.U64ToByteSlice(table.TableID)...)
	res = append(res, rowPrefix...)
	res = append(res, common.U64ToByteSlice(uint64(valueMarkerInteger))...)
	res = append(res, common.U64ToByteSlice(rowID)...)
	return res
}

// return the key that can be used to store a row of data for the given table
//
// The format:
// Key: tablePrefix_tableID_rowPrefix_rowID
func encodeTableRowKeyWithString(table *frontend.TableSpec, rowID string) []byte {
	res := tablePrefix
	res = append(res, common.U64ToByteSlice(table.TableID)...)
	res = append(res, rowPrefix...)
	res = append(res, common.U64ToByteSlice(uint64(valueMarkerString))...)
	res = append(res, encodeString(rowID)...)
	return res
}

// returns the serialized values that can be stored for a single row of the given table
func encodeTableRowValues(table *frontend.TableSpec, values map[int]*frontend.ValueExpression) []byte {
	var res []byte

	for i := range table.Columns {
		res = append(res, encodeColumnValue(table.Columns[i], values[i])...)
	}

	return res
}

func decodeTableRowValues(table *frontend.TableSpec, encodedValues []byte) (map[int]*frontend.ValueExpression, error) {
	res := make(map[int]*frontend.ValueExpression)
	idx := int(0)

	for i := range table.Columns {
		val, nxtIdx := decodeColumnValue(table.Columns[i], encodedValues, idx)
		res[i] = val
		idx = nxtIdx
	}

	return res, nil
}

// returns the key that can be used to query the kv for info about the table spec
func getTableKeyForQuery(tableName string) []byte {
	return encodeString(tableName)
}

// encodeTableSchema returns the key-value pair to store in the kv service
// to store a table schema.
//
// The format:
// Key: table name
// Value: id (64 bits) | number of columns (64 bits) | col_1 | col_2 | ...
func encodeTableSchema(table *frontend.TableSpec, id uint64) (key, value []byte, err error) {
	key = encodeString(table.TableName)

	value = common.U64ToByteSlice(id)
	value = append(value, common.U64ToByteSlice(uint64(len(table.Columns)))...)

	for i := 0; i < len(table.Columns); i++ {
		value = append(value, encodeColumnSpec(table.Columns[i])...)
	}

	return key, value, err
}

// decodeTableSchema decodes the table schema out of the key value pair stored in kv store
func decodeTableSchema(key, value []byte) (table *frontend.TableSpec, err error) {
	// name
	name, _, err := decodeString(key)
	if err != nil {
		return nil, err
	}

	// id
	id := common.ByteSliceToU64(value[:8])

	// columns
	len := common.ByteSliceToU64(value[8:16])
	cols := make([]*frontend.ColumnSpec, len)
	value = value[16:]
	idx := uint64(0) // to be used inside the loop

	// decode each column spec
	for i := uint64(0); i < len; i++ {
		cols[i], idx, err = decodeColumnSpec(value)
		value = value[idx:]
		if err != nil {
			return nil, err
		}
	}

	table = frontend.NewTableSpec(id, name, cols)
	return table, err
}

// encodeString encodes the string to bytes
func encodeString(s string) []byte {
	res := make([]byte, 0)

	res = append(res, common.U64ToByteSlice(uint64(len(s)))...)
	res = append(res, []byte(s)...)

	return res
}

// decodeString decodes a string from a byte slice at the start
func decodeString(b []byte) (s string, nxtIdx uint64, err error) {
	if len(b) < 8 {
		return s, nxtIdx, fmt.Errorf("invalid byte slice for a string")
	}
	len := common.ByteSliceToU64(b[:8])
	s = string(b[8 : 8+len])
	return s, 8 + len, nil
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
	res = append(res, common.U64ToByteSlice(uint64(columnSpecFieldName))...)
	res = append(res, encodeString(cs.Name)...)

	// encode type
	res = append(res, common.U64ToByteSlice(uint64(columnSpecFieldType))...)
	res = append(res, common.U64ToByteSlice(uint64(cs.Type))...)

	// encode nullable
	res = append(res, common.U64ToByteSlice(uint64(columnSpecFieldNullable))...)
	res = append(res, common.BoolToByte(cs.Nullable))

	// encode primary key
	res = append(res, common.U64ToByteSlice(uint64(columnSpecFieldPrimaryKey))...)
	res = append(res, common.BoolToByte(cs.PrimaryKey))

	// encode unique
	res = append(res, common.U64ToByteSlice(uint64(columnSpecFieldUnique))...)
	res = append(res, common.BoolToByte(cs.Unique))

	// encode index
	res = append(res, common.U64ToByteSlice(uint64(columnSpecFieldIndex))...)
	res = append(res, common.BoolToByte(cs.Index))

	// encode references
	res = append(res, common.U64ToByteSlice(uint64(columnSpecFieldReferences))...)
	res = append(res, encodeString(cs.References)...)

	// TODO: encode default value

	return res
}

// decodeColumnSpec decodes a column spec from a byte slice
// todo: check for appropriate length
func decodeColumnSpec(b []byte) (cs *frontend.ColumnSpec, idx uint64, err error) {
	cs = &frontend.ColumnSpec{}
	idx = uint64(0)
	delta := uint64(0)

	// name
	_ = common.ByteSliceToU64(b[:8])
	cs.Name, delta, err = decodeString(b[8:])
	if err != nil {
		return nil, 0, fmt.Errorf("")
	}
	idx += 8 + delta

	// type
	_ = common.ByteSliceToU64(b[idx : idx+8])
	cs.Type = frontend.FieldType(common.ByteSliceToU64(b[idx+8 : idx+16]))
	idx += 16

	// nullable
	_ = common.ByteSliceToU64(b[idx : idx+8])
	cs.Nullable = common.ByteToBool(b[idx+8])
	idx += 9

	// primary key
	_ = common.ByteSliceToU64(b[idx : idx+8])
	cs.PrimaryKey = common.ByteToBool(b[idx+8])
	idx += 9

	// unique
	_ = common.ByteSliceToU64(b[idx : idx+8])
	cs.Unique = common.ByteToBool(b[idx+8])
	idx += 9

	// index
	_ = common.ByteSliceToU64(b[idx : idx+8])
	cs.Index = common.ByteToBool(b[idx+8])
	idx += 9

	// references
	_ = common.ByteSliceToU64(b[idx : idx+8])
	cs.References, delta, err = decodeString(b[idx+8:])
	idx += 8 + delta

	// TODO: decode default value

	return cs, idx, err
}

func encodeColumnValue(col *frontend.ColumnSpec, value *frontend.ValueExpression) []byte {
	var res []byte
	res = append(res, common.U64ToByteSlice(uint64(encodeValueMarker(col.Type)))...)
	res = append(res, getSerializedValue(value.Val)...)
	return res
}

func decodeColumnValue(col *frontend.ColumnSpec, encodedValue []byte, idx int) (*frontend.ValueExpression, int) {
	val := &frontend.ValueExpression{Val: &frontend.Value{}}

	typ := decodeValueMarker(valueMarker(common.ByteSliceToU64(encodedValue[idx : idx+8])))
	val.Typ = typ
	val.Val.Typ = typ
	idx += 8

	switch typ {
	case frontend.FieldTypeBoolean:
		val.Val.Val = common.ByteToBool(encodedValue[idx])
		idx += 1
	case frontend.FieldTypeFloat:
		val.Val.Val = common.ByteSliceToFloat64(encodedValue[idx : idx+8])
		idx += 8
	case frontend.FieldTypeInteger:
		val.Val.Val = common.ByteSliceToI64(encodedValue[idx : idx+8])
		idx += 8
	case frontend.FieldTypeString:
		str, nxtIdx, _ := decodeString(encodedValue[idx:])
		val.Val.Val = str
		idx = int(nxtIdx)
	case frontend.FieldTypeNull:
		break
	}

	return val, idx
}

func encodeValueMarker(typ frontend.FieldType) valueMarker {
	switch typ {
	case frontend.FieldTypeBoolean:
		return valueMarkerBoolean
	case frontend.FieldTypeFloat:
		return valueMarkerFloat
	case frontend.FieldTypeInteger:
		return valueMarkerInteger
	case frontend.FieldTypeString:
		return valueMarkerString
	case frontend.FieldTypeNull:
		return valueMarkerNull
	}

	panic("Programming error: unreachable code")
}

func decodeValueMarker(marker valueMarker) frontend.FieldType {
	switch marker {
	case valueMarkerBoolean:
		return frontend.FieldTypeBoolean
	case valueMarkerFloat:
		return frontend.FieldTypeFloat
	case valueMarkerInteger:
		return frontend.FieldTypeInteger
	case valueMarkerString:
		return frontend.FieldTypeString
	case valueMarkerNull:
		return frontend.FieldTypeNull
	}

	panic("Programming error: unreachable code")
}

func getSerializedValue(value *frontend.Value) []byte {
	switch value.Typ {
	case frontend.FieldTypeBoolean:
		return []byte{common.BoolToByte(value.GetAsBoolean())}
	case frontend.FieldTypeFloat:
		return common.Float64ToByteSlice(value.GetAsFloat())
	case frontend.FieldTypeInteger:
		return common.I64ToByteSlice(value.GetAsInt())
	case frontend.FieldTypeString:
		return []byte(encodeString(value.GetAsString()))
	case frontend.FieldTypeNull:
		return []byte{}
	}

	panic("Programming error: unreachable code")
}
