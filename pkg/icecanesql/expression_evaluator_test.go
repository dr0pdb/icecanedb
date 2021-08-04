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

package icecanesql

import (
	"fmt"
	"testing"

	"github.com/dr0pdb/icecanedb/pkg/frontend"
	"github.com/stretchr/testify/assert"
)

func TestValidateBinaryOpExpressionsShouldSucceed(t *testing.T) {
	in := []*frontend.BinaryOpExpression{
		{
			Op: frontend.OperatorAndAnd,
			L:  &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeBoolean, Val: true}},
			R:  &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeBoolean, Val: false}},
		},
		{
			Op: frontend.OperatorOrOr,
			L:  &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeBoolean, Val: false}},
			R:  &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeBoolean, Val: true}},
		},
		{
			Op: frontend.OperatorPlus,
			L:  &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeInteger, Val: int64(1)}},
			R:  &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeInteger, Val: int64(1011)}},
		},
		{
			Op: frontend.OperatorPlus,
			L:  &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeFloat, Val: float64(1.9)}},
			R:  &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeFloat, Val: float64(1011.11)}},
		},
		{
			Op: frontend.OperatorPlus,
			L:  &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeString, Val: "Hello "}},
			R:  &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeString, Val: "World"}},
		},
		{
			Op: frontend.OperatorAsterisk,
			L:  &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeInteger, Val: int64(101)}},
			R:  &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeInteger, Val: int64(1011)}},
		},
		{
			Op: frontend.OperatorAsterisk,
			L:  &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeFloat, Val: float64(1.9)}},
			R:  &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeFloat, Val: float64(1011.11)}},
		},
		{
			Op: frontend.OperatorEqual,
			L:  &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeInteger, Val: int64(1)}},
			R:  &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeInteger, Val: int64(1)}},
		},
		{
			Op: frontend.OperatorEqual,
			L:  &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeString, Val: "Hello"}},
			R:  &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeString, Val: "Hello"}},
		},
		{
			Op: frontend.OperatorEqual,
			L:  &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeString, Val: "Hello"}},
			R:  &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeString, Val: "World"}},
		},
	}
	expectedOut := []*frontend.ValueExpression{
		{
			Val: &frontend.Value{
				Typ: frontend.FieldTypeBoolean,
				Val: false,
			},
		},
		{
			Val: &frontend.Value{
				Typ: frontend.FieldTypeBoolean,
				Val: true,
			},
		},
		{
			Val: &frontend.Value{
				Typ: frontend.FieldTypeInteger,
				Val: int64(1012),
			},
		},
		{
			Val: &frontend.Value{
				Typ: frontend.FieldTypeFloat,
				Val: float64(1013.01),
			},
		},
		{
			Val: &frontend.Value{
				Typ: frontend.FieldTypeString,
				Val: "Hello World",
			},
		},
		{
			Val: &frontend.Value{
				Typ: frontend.FieldTypeInteger,
				Val: int64(102111),
			},
		},
		{
			Val: &frontend.Value{
				Typ: frontend.FieldTypeFloat,
				Val: float64(1921.109), // todo: float comparisons aren't accurate. Compare with a threshold.
			},
		},
		{
			Val: &frontend.Value{
				Typ: frontend.FieldTypeBoolean,
				Val: true,
			},
		},
		{
			Val: &frontend.Value{
				Typ: frontend.FieldTypeBoolean,
				Val: true,
			},
		},
		{
			Val: &frontend.Value{
				Typ: frontend.FieldTypeBoolean,
				Val: false,
			},
		},
	}

	for i := range in {
		ee := newValueExpressionEvaluator(in[i])
		actualOut, err := ee.evaluate()
		assert.Nil(t, err, fmt.Sprintf("unexpected error in evaulating binary expression %d", i))
		assert.Equal(t, expectedOut[i], actualOut, fmt.Sprintf("expected and actual values don't match for expression %d", i))
	}
}
