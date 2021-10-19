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

type FieldType uint64

const (
	FieldTypeBoolean FieldType = iota
	FieldTypeInteger
	FieldTypeString
	FieldTypeFloat
	FieldTypeNull
)

func (f FieldType) String() string {
	switch f {
	case FieldTypeBoolean:
		return "boolean"

	case FieldTypeInteger:
		return "integer"

	case FieldTypeString:
		return "string"

	case FieldTypeFloat:
		return "float"

	case FieldTypeNull:
		return "null"
	}

	panic("programming error: unexpected field type in String() of FieldType")
}

type Value struct {
	Typ FieldType
	Val interface{}
}

func (v *Value) GetAsBoolean() bool {
	if v.Typ != FieldTypeBoolean {
		panic("programming error: expected type to be boolean")
	}

	return v.Val.(bool)
}

func (v *Value) GetAsInt() int64 {
	switch t := v.Val.(type) {
	case int64:
		return t
	case int:
		return int64(t)
	default:
		panic("programming error: expected type to be integer")
	}
}

func (v *Value) GetAsFloat() float64 {
	if v.Typ != FieldTypeFloat {
		panic("programming error: expected type to be float")
	}

	return v.Val.(float64)
}

func (v *Value) GetAsString() string {
	if v.Typ != FieldTypeString {
		panic("programming error: expected type to be string")
	}

	return v.Val.(string)
}

var (
	// Types which can be operands of the '+' operator
	OperatorPlusOperandTypes = map[FieldType]bool{FieldTypeInteger: true, FieldTypeFloat: true, FieldTypeString: true}

	// Types which can be operands of the '-' operator
	OperatorMinusOperandTypes = map[FieldType]bool{FieldTypeInteger: true, FieldTypeFloat: true}

	// Types which can be operands of the '*' operator
	OperatorAsteriskOperandTypes = map[FieldType]bool{FieldTypeInteger: true, FieldTypeFloat: true}

	// Types which can be operands of the '/' operator
	OperatorSlashOperandTypes = map[FieldType]bool{FieldTypeInteger: true, FieldTypeFloat: true}

	// Types which can be operands of the '%' operator
	OperatorPercentOperandTypes = map[FieldType]bool{FieldTypeInteger: true}

	// Types which can be operands of the '>', '>=', '<' & '<=' operators
	OperatorComparisonOperandTypes = map[FieldType]bool{FieldTypeInteger: true, FieldTypeFloat: true, FieldTypeString: true}
)
