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
	"testing"

	"github.com/dr0pdb/icecanedb/pkg/frontend"
	"github.com/stretchr/testify/assert"
)

const (
	testTableName = "Students"
)

var (
	// "CREATE TABLE Students(ROLL_NO int PRIMARY KEY UNIQUE, NAME varchar INDEX, SUBJECT varchar, AGE int, WEIGHT double, RICH bool);"
	testTableSpec = &frontend.TableSpec{
		TableID:             1,
		TableName:           testTableName,
		Columns:             testColumnSpec,
		PrimaryKeyColumnIdx: 0,
	}

	testColumnSpec = []*frontend.ColumnSpec{
		{
			Name:       "ROLL_NO",
			Type:       frontend.FieldTypeInteger,
			Nullable:   true,
			PrimaryKey: true,
			Unique:     true,
			Index:      false,
			References: "",
		},
		{
			Name:       "NAME",
			Type:       frontend.FieldTypeString,
			Nullable:   true,
			PrimaryKey: false,
			Unique:     false,
			Index:      true,
			References: "",
		},
		{
			Name:       "SUBJECT",
			Type:       frontend.FieldTypeString,
			Nullable:   true,
			PrimaryKey: false,
			Unique:     false,
			Index:      false,
			References: "",
		},
		{
			Name:       "AGE",
			Type:       frontend.FieldTypeInteger,
			Nullable:   true,
			PrimaryKey: false,
			Unique:     false,
			Index:      false,
			References: "",
		},
		{
			Name:       "WEIGHT",
			Type:       frontend.FieldTypeFloat,
			Nullable:   true,
			PrimaryKey: false,
			Unique:     false,
			Index:      false,
			References: "",
		},
		{
			Name:       "RICH",
			Type:       frontend.FieldTypeBoolean,
			Nullable:   true,
			PrimaryKey: false,
			Unique:     false,
			Index:      false,
			References: "",
		},
	}
)

func setupCatalog() *catalog {
	c := newCatalog(nil)
	c.schemaCache[testTableName] = testTableSpec
	return c
}

//
// Create table statement
//

func TestCreateTableValidatorNoErrorWithCorrectAst(t *testing.T) {
	catalog := setupCatalog()

	statement := &frontend.CreateTableStatement{
		Spec: &frontend.TableSpec{
			TableName: "Students",
			Columns: []*frontend.ColumnSpec{
				{
					Name:       "ROLL_NO",
					Type:       frontend.FieldTypeInteger,
					Nullable:   false,
					PrimaryKey: true,
					Unique:     true,
					Index:      false,
					References: "",
				},
				{
					Name:       "AGE",
					Type:       frontend.FieldTypeFloat,
					Nullable:   true,
					PrimaryKey: false,
					Unique:     false,
					Index:      false,
					References: "",
					Default:    &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeFloat, Val: 10.1}},
				},
				{
					Name:       "RICH",
					Type:       frontend.FieldTypeBoolean,
					Nullable:   true,
					PrimaryKey: false,
					Unique:     false,
					Index:      false,
					References: "",
					Default:    &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeBoolean, Val: false}},
				},
			},
		},
	}

	validator := newValidator(statement, catalog)

	err := validator.validate(0)
	assert.Nil(t, err, "Unexpected error in validating create table statement")
}

func TestCreateTableValidatorErrorWithNoPrimaryKey(t *testing.T) {
	catalog := setupCatalog()

	statement := &frontend.CreateTableStatement{
		Spec: &frontend.TableSpec{
			TableName: "Students",
			Columns: []*frontend.ColumnSpec{
				{
					Name:       "ROLL_NO",
					Type:       frontend.FieldTypeInteger,
					Nullable:   false,
					Unique:     true,
					Index:      false,
					References: "",
				},
				{
					Name:       "AGE",
					Type:       frontend.FieldTypeFloat,
					Nullable:   true,
					Unique:     false,
					Index:      false,
					References: "",
					Default:    &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeFloat, Val: 10.1}},
				},
				{
					Name:       "RICH",
					Type:       frontend.FieldTypeBoolean,
					Nullable:   true,
					Unique:     false,
					Index:      false,
					References: "",
					Default:    &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeBoolean, Val: false}},
				},
			},
		},
	}

	validator := newValidator(statement, catalog)

	err := validator.validate(0)
	assert.NotNil(t, err, "Unexpected success in validating create table statement with no primary key")
}

func TestCreateTableValidatorErrorWithNullablePrimaryKey(t *testing.T) {
	catalog := setupCatalog()

	statement := &frontend.CreateTableStatement{
		Spec: &frontend.TableSpec{
			TableName: "Students",
			Columns: []*frontend.ColumnSpec{
				{
					Name:       "ROLL_NO",
					Type:       frontend.FieldTypeInteger,
					Nullable:   true, // shouldn't be nullable
					PrimaryKey: true,
					Unique:     true,
					Index:      false,
					References: "",
				},
				{
					Name:       "AGE",
					Type:       frontend.FieldTypeFloat,
					Nullable:   true,
					Unique:     false,
					Index:      false,
					References: "",
					Default:    &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeFloat, Val: 10.1}},
				},
				{
					Name:       "RICH",
					Type:       frontend.FieldTypeBoolean,
					Nullable:   true,
					Unique:     false,
					Index:      false,
					References: "",
					Default:    &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeBoolean, Val: false}},
				},
			},
		},
	}

	validator := newValidator(statement, catalog)

	err := validator.validate(0)
	assert.NotNil(t, err, "Unexpected success in validating create table statement with nullable primary key")
}

func TestCreateTableValidatorErrorWithNonUniquePrimaryKey(t *testing.T) {
	catalog := setupCatalog()

	statement := &frontend.CreateTableStatement{
		Spec: &frontend.TableSpec{
			TableName: "Students",
			Columns: []*frontend.ColumnSpec{
				{
					Name:       "ROLL_NO",
					Type:       frontend.FieldTypeInteger,
					Nullable:   false,
					PrimaryKey: true,
					Unique:     false, // should lead to error
					Index:      false,
					References: "",
				},
				{
					Name:       "AGE",
					Type:       frontend.FieldTypeFloat,
					Nullable:   true,
					Unique:     false,
					Index:      false,
					References: "",
					Default:    &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeFloat, Val: 10.1}},
				},
				{
					Name:       "RICH",
					Type:       frontend.FieldTypeBoolean,
					Nullable:   true,
					Unique:     false,
					Index:      false,
					References: "",
					Default:    &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeBoolean, Val: false}},
				},
			},
		},
	}

	validator := newValidator(statement, catalog)

	err := validator.validate(0)
	assert.NotNil(t, err, "Unexpected success in validating create table statement with non unique primary key")
}

func TestCreateTableValidatorErrorWithMultiplePrimaryKey(t *testing.T) {
	catalog := setupCatalog()

	statement := &frontend.CreateTableStatement{
		Spec: &frontend.TableSpec{
			TableName: "Students",
			Columns: []*frontend.ColumnSpec{
				{
					Name:       "ROLL_NO",
					Type:       frontend.FieldTypeInteger,
					Nullable:   false,
					PrimaryKey: true,
					Unique:     true,
					Index:      false,
					References: "",
				},
				{
					Name:       "AGE",
					Type:       frontend.FieldTypeFloat,
					Nullable:   true,
					PrimaryKey: true, // should lead to error
					Unique:     false,
					Index:      false,
					References: "",
					Default:    &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeFloat, Val: 10.1}},
				},
				{
					Name:       "RICH",
					Type:       frontend.FieldTypeBoolean,
					Nullable:   true,
					Unique:     false,
					Index:      false,
					References: "",
					Default:    &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeBoolean, Val: false}},
				},
			},
		},
	}

	validator := newValidator(statement, catalog)

	err := validator.validate(0)
	assert.NotNil(t, err, "Unexpected success in validating create table statement with multiple primary keys")
}

func TestCreateTableValidatorErrorWithDefaultValueTypeNotMatching(t *testing.T) {
	catalog := setupCatalog()

	statement := &frontend.CreateTableStatement{
		Spec: &frontend.TableSpec{
			TableName: "Students",
			Columns: []*frontend.ColumnSpec{
				{
					Name:       "ROLL_NO",
					Type:       frontend.FieldTypeInteger,
					PrimaryKey: true,
					Unique:     true,
					Index:      false,
					References: "",
				},
				{
					Name:       "AGE",
					Type:       frontend.FieldTypeFloat,
					Nullable:   true,
					Unique:     false,
					Index:      false,
					References: "",
					Default:    &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeString, Val: "Wrong type"}}, // should lead to error
				},
				{
					Name:       "RICH",
					Type:       frontend.FieldTypeBoolean,
					Nullable:   true,
					Unique:     false,
					Index:      false,
					References: "",
					Default:    &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeBoolean, Val: false}},
				},
			},
		},
	}

	validator := newValidator(statement, catalog)

	err := validator.validate(0)
	assert.NotNil(t, err, "Unexpected success in validating create table statement with default value type not matching with column type")
}

//
// Insert statement
//

func TestInsertStatementValidatorNoErrorWithCorrectWithoutColumns(t *testing.T) {
	catalog := setupCatalog()

	// "INSERT INTO Students VALUES (1, 'John Doe', 'Economics', 1 + 1011, 50.5, false);"
	statement := &frontend.InsertStatement{
		Table: &frontend.Table{Name: testTableName},
		Values: []frontend.Expression{
			&frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeInteger, Val: int64(1)}},
			&frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeString, Val: "'John Doe'"}},
			&frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeString, Val: "'Economics'"}},
			&frontend.BinaryOpExpression{
				Op: frontend.OperatorPlus,
				L:  &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeInteger, Val: int64(1)}},
				R:  &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeInteger, Val: int64(1011)}},
			},
			&frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeFloat, Val: 50.5}},
			&frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeBoolean, Val: false}},
		},
	}

	validator := &insertStatementValidator{
		ast:     statement,
		catalog: catalog,
	}

	err := validator.validate(0)
	assert.Nil(t, err, "Unexpected error in validating insert statement")
}

func TestInsertStatementValidatorNoErrorWithCorrectWithColumns(t *testing.T) {
	catalog := setupCatalog()

	// "INSERT INTO Students (NAME, SUBJECT, AGE, ROLL_NO, WEIGHT, RICH) VALUES ('John Doe', 'Economics', 1 + 1011, 1, 50.5, false);"
	statement := &frontend.InsertStatement{
		Table: &frontend.Table{Name: testTableName},
		Values: []frontend.Expression{
			&frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeString, Val: "'John Doe'"}},
			&frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeString, Val: "'Economics'"}},
			&frontend.BinaryOpExpression{
				Op: frontend.OperatorPlus,
				L:  &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeInteger, Val: int64(1)}},
				R:  &frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeInteger, Val: int64(1011)}},
			},
			&frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeInteger, Val: int64(1)}},
			&frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeFloat, Val: 50.5}},
			&frontend.ValueExpression{Val: &frontend.Value{Typ: frontend.FieldTypeBoolean, Val: false}},
		},
		Columns: []string{
			"NAME",
			"SUBJECT",
			"AGE",
			"ROLL_NO",
			"WEIGHT",
			"RICH",
		},
	}

	validator := &insertStatementValidator{
		ast:     statement,
		catalog: catalog,
	}

	err := validator.validate(0)
	assert.Nil(t, err, "Unexpected error in validating insert statement")
}
