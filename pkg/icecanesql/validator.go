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
	log "github.com/sirupsen/logrus"
)

// Validator validates the parsed AST for inconsistencies
//
// The major responsibilities are type checking and schema validations.
type validator interface {
	validate(txnID uint64) error
}

var _ validator = (*emptyValidator)(nil)
var _ validator = (*createTableValidator)(nil)
var _ validator = (*insertStatementValidator)(nil)
var _ validator = (*deleteStatementValidator)(nil)
var _ validator = (*updateStatementValidator)(nil)

// newValidator creates a new validator for the parsed statement
func newValidator(ast frontend.Statement, catalog *catalog) validator {
	switch st := ast.(type) {
	case *frontend.CreateTableStatement:
		return &createTableValidator{ast: st}
	case *frontend.InsertStatement:
		return &insertStatementValidator{ast: st, catalog: catalog}
	case *frontend.DeleteStatement:
		return &deleteStatementValidator{ast: st}
	case *frontend.UpdateStatement:
		return &updateStatementValidator{ast: st, catalog: catalog}
	case *frontend.DropTableStatement, *frontend.TruncateTableStatement:
		return &emptyValidator{ast: ast}
	}

	panic("programming error: no validator found for statement")
}

// emptyValidator is a trivial validator that doesn't validate anything
// useful for statements such as begin, commit and rollback transactions.
type emptyValidator struct {
	ast frontend.Statement
}

func (ev *emptyValidator) validate(txnID uint64) error {
	return nil
}

// createTableValidator validates a create table statement
type createTableValidator struct {
	ast *frontend.CreateTableStatement
}

// validates the create table statement
func (ctv *createTableValidator) validate(txnID uint64) error {
	log.Info("icecanesql::validator::createTableValidator.validate; start;")
	return validateTableSpec(ctv.ast.Spec)
}

// insertStatementValidator validates an insert statement
type insertStatementValidator struct {
	ast     *frontend.InsertStatement
	catalog *catalog
}

func (isv *insertStatementValidator) validate(txnID uint64) error {
	log.Info("icecanesql::validator::insertStatementValidator.validate; start;")

	spec, err := isv.catalog.getTableInfo(isv.ast.Table.Name, txnID)
	if err != nil {
		return err
	}

	if len(isv.ast.Columns) != 0 && len(isv.ast.Columns) != len(isv.ast.Values) {
		return fmt.Errorf("validation error: length of columns and values are unequal in insert statement")
	}

	for i, val := range isv.ast.Values {
		ee := newValueExpressionEvaluator(val)
		ne, err := ee.evaluate()
		if err != nil {
			return fmt.Errorf("error in column %d, err: %v", i, err)
		}

		// update the value to the evaluate value expression so that later steps can use it directly.
		isv.ast.Values[i] = ne
	}

	_, err = validateColumns(spec, isv.ast.Columns, isv.ast.Values)
	if err != nil {
		return err
	}

	// TODO: validate that we have exactly one primary key column which is non-nullable.
	// Also, allow PK only on INTEGER and STRING type.

	return nil
}

// updateStatementValidator validates a update statement
type updateStatementValidator struct {
	ast     *frontend.UpdateStatement
	catalog *catalog
}

func (usv *updateStatementValidator) validate(txnID uint64) error {
	log.Info("icecanesql::validator::updateStatementValidator.validate; start;")

	spec, err := usv.catalog.getTableInfo(usv.ast.Table.Name, txnID)
	if err != nil {
		return err
	}

	// validate that the values are binary op expr with op_equals, left side is identifier and right side evaluates to value expression of correct type
	for i := range usv.ast.Values {
		val := usv.ast.Values[i]

		if v, ok := val.(*frontend.BinaryOpExpression); ok {
			if v.Op != frontend.OperatorEqual {
				return fmt.Errorf("")
			}
			if _, ok2 := v.L.(*frontend.IdentifierExpression); !ok2 {
				return fmt.Errorf("")
			}

			ee := newValueExpressionEvaluator(v.R)
			ne, err := ee.evaluate()
			if err != nil {
				return fmt.Errorf("error in column %d, err: %v", i, err)
			}

			// validate the type of the value matches the spec
			if ne.Val.Typ != spec.Columns[i].Type {
				return fmt.Errorf("type error in column %d, expected type %s, found type %s", i, spec.Columns[i].Type, ne.Val.Typ)
			}

			// update the value to the evaluate value expression so that later steps can use it directly.
			usv.ast.Values[i] = ne

		} else {
			return fmt.Errorf("")
		}
	}

	// validate the where clause

	return nil
}

// deleteStatementValidator validates a delete statement
type deleteStatementValidator struct {
	ast *frontend.DeleteStatement
}

func (isv *deleteStatementValidator) validate(txnID uint64) error {
	return nil
}

//
// Helper functions
//

func validateColumns(spec *frontend.TableSpec, cols []string, valuesExpr []frontend.Expression) (map[int]*frontend.ValueExpression, error) {
	vmap := make(map[int]*frontend.ValueExpression)
	values := make([]*frontend.ValueExpression, len(valuesExpr))

	for i := range valuesExpr {
		values[i] = valuesExpr[i].(*frontend.ValueExpression)
	}

	if len(cols) != 0 {
		nameToIdx := make(map[string]int)
		for i := range spec.Columns {
			nameToIdx[spec.Columns[i].Name] = i
		}

		for i := range values {
			if _, found := nameToIdx[cols[i]]; !found {
				return vmap, fmt.Errorf("unknown column name %s", cols[i])
			}

			vmap[nameToIdx[cols[i]]] = values[i]
		}

		// fill in default and null values for nullable fields
		for i := range spec.Columns {
			col := spec.Columns[i]
			if _, found := vmap[i]; found {
				continue
			}

			if col.Default != nil {
				d := col.Default.(*frontend.ValueExpression)

				vmap[i] = d
				continue
			}

			if col.Nullable {
				vmap[i] = &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeNull}}
				continue
			}
		}

		if len(vmap) != len(spec.Columns) {
			return vmap, fmt.Errorf("length of given values list doesn't match the number of columns in the table schema")
		}

		for i := range spec.Columns {
			if vmap[nameToIdx[spec.Columns[i].Name]].Val.Typ != spec.Columns[i].Type {
				return vmap, fmt.Errorf("type error in column %d, expected type %s, found type %s", i, spec.Columns[i].Type, values[i].Val.Typ)
			}
		}

	} else {
		if len(values) != len(spec.Columns) {
			return vmap, fmt.Errorf("length of given values list doesn't match the number of columns in the table schema")
		}

		for i := range spec.Columns {
			vmap[i] = values[i]

			if values[i].Val.Typ != spec.Columns[i].Type {
				return vmap, fmt.Errorf("type error in column %d, expected type %s, found type %s", i, spec.Columns[i].Type, values[i].Val.Typ)
			}
		}
	}

	return vmap, nil
}

// validate that exactly one column contains primary key and is unique and non nullable
func validateTableSpec(spec *frontend.TableSpec) error {
	pkIdx := -1

	for i := 0; i < len(spec.Columns); i++ {
		col := spec.Columns[i]

		if col.PrimaryKey {
			if pkIdx != -1 {
				return fmt.Errorf("validator error: invalid table spec. found multiple primary key columns %s %s", spec.Columns[pkIdx].Name, spec.Columns[i].Name)
			}
			pkIdx = i

			if col.Nullable {
				return fmt.Errorf("validator error: invalid table spec. primary key column %s cannot be nullable", spec.Columns[i].Name)
			}

			if !col.Unique {
				return fmt.Errorf("validator error: invalid table spec. primary key column %s cannot be non-unique", spec.Columns[i].Name)
			}
		}

		if col.Default != nil {
			ee := newValueExpressionEvaluator(col.Default)
			ne, err := ee.evaluate()
			if err != nil {
				return fmt.Errorf("error in column %s, err: %v", spec.Columns[i].Name, err)
			}

			if ne.Val.Typ != spec.Columns[i].Type {
				return fmt.Errorf("invalid table schema: default value type doesn't match with the column type for column %s", spec.Columns[i].Name)
			}

			col.Default = ne
		}
	}

	if pkIdx == -1 {
		return fmt.Errorf("invalid table schema: no primary key column defined")
	}

	return nil
}
