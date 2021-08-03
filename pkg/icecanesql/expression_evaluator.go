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

	"github.com/dr0pdb/icecanedb/pkg/frontend"
)

// evaluates expressions and type checks them. This is meant to be used for expressions
// which are expected to return a value expression.
//
// Eg: VALUES in insert statement
// Non Eg: predicate in WHERE clause
type valueExpressionEvaluator struct {
	expr frontend.Expression
	err  error
}

func newValueExpressionEvaluator(expr frontend.Expression) *valueExpressionEvaluator {
	return &valueExpressionEvaluator{
		expr: expr,
	}
}

// evaluate the given expression, validating it in the process
func (ee *valueExpressionEvaluator) evaluate() (*frontend.ValueExpression, error) {
	return ee.evaluateAndValidateExpression(ee.expr)
}

//
// Internal methods
//

// evaluate the given expression and typecheck
func (ee *valueExpressionEvaluator) evaluateAndValidateExpression(expr frontend.Expression) (*frontend.ValueExpression, error) {
	if ee.err != nil {
		return nil, ee.err
	}

	switch e := expr.(type) {
	case *frontend.BetweenExpression:
		return nil, fmt.Errorf("invalid expression type: Between expression doesn't emit a value")
	case *frontend.BinaryOpExpression:
		return ee.evaluateAndValidateBinaryOpExpression(e)
	case *frontend.GroupingExpression:
		return ee.evaluateAndValidateExpression(e.InExp)
	case *frontend.UnaryOpExpression:
		return ee.evaluateAndValidateUnaryOpExpression(e)
	case *frontend.IdentifierExpression:
		return nil, fmt.Errorf("invalid expression type: identifier expression doesn't emit a value")
	case *frontend.SelectAllExpression:
		return nil, fmt.Errorf("invalid expression type: Select All expression doesn't emit a value")
	case *frontend.ValueExpression:
		return e, nil
	}

	panic("programming error: unexhaustive switch case in evaluateAndValidateExpression")
}

func (e *valueExpressionEvaluator) evaluateAndValidateUnaryOpExpression(expr *frontend.UnaryOpExpression) (*frontend.ValueExpression, error) {
	if e.err != nil {
		return nil, e.err
	}

	expr.Exp, _ = e.evaluateAndValidateExpression(expr.Exp)
	if e.err != nil {
		return nil, e.err
	}

	switch expr.Op {
	case frontend.OperatorMinus:
		// assert the inner exp is integer/float

	case frontend.OperatorExclamation:
		// assert the inner exp is boolean

	default:
		panic(fmt.Sprintf("programming error: unexpected operator %d in unary operator expression", expr.Op)) // todo: implement String() for operator
	}

	return nil, e.err
}

func (e *valueExpressionEvaluator) evaluateAndValidateBinaryOpExpression(expr *frontend.BinaryOpExpression) (*frontend.ValueExpression, error) {
	if e.err != nil {
		return nil, e.err
	}

	expr.L, _ = e.evaluateAndValidateExpression(expr.L)
	expr.R, _ = e.evaluateAndValidateExpression(expr.R)

	if e.err != nil {
		return nil, e.err
	}

	switch expr.Op {
	case frontend.OperatorEqual:

	case frontend.OperatorAndAnd:
		lv := expr.L.(*frontend.ValueExpression)
		rv := expr.R.(*frontend.ValueExpression)

		e.err = assertTypeEquality(lv, rv, frontend.FieldTypeBoolean)
		if e.err == nil {
			res := &frontend.ValueExpression{
				Val: &frontend.Value{
					Typ: frontend.FieldTypeBoolean,
					Val: lv.Val.GetBoolean() && rv.Val.GetBoolean(),
				},
			}
			return res, nil
		}

	case frontend.OperatorOrOr:
		lv := expr.L.(*frontend.ValueExpression)
		rv := expr.R.(*frontend.ValueExpression)

		e.err = assertTypeEquality(lv, rv, frontend.FieldTypeBoolean)
		if e.err == nil {
			res := &frontend.ValueExpression{
				Val: &frontend.Value{
					Typ: frontend.FieldTypeBoolean,
					Val: lv.Val.GetBoolean() || rv.Val.GetBoolean(),
				},
			}
			return res, nil
		}

	}

	return nil, e.err
}

//
// Utilities
//

func assertTypeEquality(l, r *frontend.ValueExpression, expectedType frontend.FieldType) error {
	if l.Typ != expectedType {
		return fmt.Errorf("type mismatch: expected %s found %s", expectedType, l.Typ)
	}

	if r.Typ != expectedType {
		return fmt.Errorf("type mismatch: expected %s found %s", expectedType, r.Typ)
	}

	return nil
}
