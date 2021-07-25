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

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateTableBasic(t *testing.T) {
	cmd := "CREATE TABLE Students(ROLL_NO int PRIMARY KEY UNIQUE, NAME varchar INDEX, SUBJECT varchar, AGE double, RICH bool);"
	expectedSpecs := []*ColumnSpec{
		{
			Name:       "ROLL_NO",
			Type:       FieldTypeInteger,
			Nullable:   true,
			PrimaryKey: true,
			Unique:     true,
			Index:      false,
			References: "",
		},
		{
			Name:       "NAME",
			Type:       FieldTypeString,
			Nullable:   true,
			PrimaryKey: false,
			Unique:     false,
			Index:      true,
			References: "",
		},
		{
			Name:       "SUBJECT",
			Type:       FieldTypeString,
			Nullable:   true,
			PrimaryKey: false,
			Unique:     false,
			Index:      false,
			References: "",
		},
		{
			Name:       "AGE",
			Type:       FieldTypeFloat,
			Nullable:   true,
			PrimaryKey: false,
			Unique:     false,
			Index:      false,
			References: "",
		},
		{
			Name:       "RICH",
			Type:       FieldTypeBoolean,
			Nullable:   true,
			PrimaryKey: false,
			Unique:     false,
			Index:      false,
			References: "",
		},
	}

	p := NewParser("testParser", cmd)
	stmt, err := p.Parse()
	assert.Nil(t, err, "Unexpected error in parsing create table DDL")

	assert.IsType(t, &CreateTableStatement{}, stmt, "Unexpected type of statement. Expected a &CreateTableStatement")
	ctStmt := stmt.(*CreateTableStatement)

	assert.Equal(t, "Students", ctStmt.Spec.TableName, fmt.Sprintf("Wrong table name. Expected Students, Found %s", ctStmt.Spec.TableName))
	assert.Equal(t, 5, len(ctStmt.Spec.Columns), fmt.Sprintf("Unexpected length of columns. Expected 3, found %d", len(ctStmt.Spec.Columns)))

	for i := 0; i < 5; i++ {
		assert.Equal(t, expectedSpecs[i], ctStmt.Spec.Columns[i], "Wrong column spec")
	}
}

func TestCreateTableBasicWithValueDefaults(t *testing.T) {
	cmd := "CREATE TABLE Students(ROLL_NO int PRIMARY KEY UNIQUE, AGE double DEFAULT 10.1, RICH bool DEFAULT false);"
	expectedSpecs := []*ColumnSpec{
		{
			Name:       "ROLL_NO",
			Type:       FieldTypeInteger,
			Nullable:   true,
			PrimaryKey: true,
			Unique:     true,
			Index:      false,
			References: "",
		},
		{
			Name:       "AGE",
			Type:       FieldTypeFloat,
			Nullable:   true,
			PrimaryKey: false,
			Unique:     false,
			Index:      false,
			References: "",
			Default:    &ValueExpression{Val: &Value{Typ: FieldTypeFloat, Val: "10.1"}},
		},
		{
			Name:       "RICH",
			Type:       FieldTypeBoolean,
			Nullable:   true,
			PrimaryKey: false,
			Unique:     false,
			Index:      false,
			References: "",
			Default:    &ValueExpression{Val: &Value{Typ: FieldTypeBoolean, Val: "false"}},
		},
	}

	p := NewParser("testParser", cmd)
	stmt, err := p.Parse()
	assert.Nil(t, err, "Unexpected error in parsing create table DDL")

	assert.IsType(t, &CreateTableStatement{}, stmt, "Unexpected type of statement. Expected a &CreateTableStatement")
	ctStmt := stmt.(*CreateTableStatement)

	assert.Equal(t, "Students", ctStmt.Spec.TableName, fmt.Sprintf("Wrong table name. Expected Students, Found %s", ctStmt.Spec.TableName))
	assert.Equal(t, len(expectedSpecs), len(ctStmt.Spec.Columns), fmt.Sprintf("Unexpected length of columns. Expected 3, found %d", len(ctStmt.Spec.Columns)))

	for i := 0; i < len(expectedSpecs); i++ {
		assert.Equal(t, expectedSpecs[i], ctStmt.Spec.Columns[i], "Wrong column spec")
	}
}

// Incorrect statements
func TestCreateTableIncorrect(t *testing.T) {
	cmds := []string{
		"CREATE TABLE Students(ROLL_NO int PRIMARY Random KEY UNIQUE, NAME bool, SUBJECT varchar);",
		"CREATE TABLE Students ROLL_NO int PRIMARY Random KEY UNIQUE, NAME bool, SUBJECT varchar);",
		"CREATE TABLE Students(ROLL_NO int PRIMARY Random KEY UNIQUE, NAME bool, SUBJECT varchar)",
	}

	for i := 0; i < len(cmds); i++ {
		p := NewParser("testParser", cmds[i])
		_, err := p.Parse()
		assert.NotNil(t, err, "Unexpected success in parsing create table DDL")
	}
}

func TestDropTableCorrect(t *testing.T) {
	cmd := "DROP TABLE Students;"

	p := NewParser("testParser", cmd)
	stmt, err := p.Parse()
	assert.Nil(t, err, "Unexpected error in parsing create table DDL")

	assert.IsType(t, &DropTableStatement{}, stmt, "Unexpected type of statement. Expected a &DropTableStatement")
	dtStmt := stmt.(*DropTableStatement)

	assert.Equal(t, "Students", dtStmt.TableName, fmt.Sprintf("Wrong table name. Expected Students, Found %s", dtStmt.TableName))
}

func TestDropTableIncorrect(t *testing.T) {
	cmds := []string{
		"DROP RANDOM Students;",
		"DROP TABLE Students",
	}

	for i := 0; i < len(cmds); i++ {
		p := NewParser("testParser", cmds[i])
		_, err := p.Parse()
		assert.NotNil(t, err, "Unexpected success in parsing drop table DDL")
	}
}

func TestTruncateTableCorrect(t *testing.T) {
	cmd := "TRUNCATE TABLE Students;"

	p := NewParser("testParser", cmd)
	stmt, err := p.Parse()
	assert.Nil(t, err, "Unexpected error in parsing create table DDL")

	assert.IsType(t, &TruncateTableStatement{}, stmt, "Unexpected type of statement. Expected a &DropTableStatement")
	dtStmt := stmt.(*TruncateTableStatement)

	assert.Equal(t, "Students", dtStmt.TableName, fmt.Sprintf("Wrong table name. Expected Students, Found %s", dtStmt.TableName))
}

func TestTruncateTableIncorrect(t *testing.T) {
	cmds := []string{
		"TRUNCATE RANDOM Students;",
		"TRUNCATE TABLE Students",
	}

	for i := 0; i < len(cmds); i++ {
		p := NewParser("testParser", cmds[i])
		_, err := p.Parse()
		assert.NotNil(t, err, "Unexpected success in truncating drop table DDL")
	}
}

//
// DML tests
//

func TestInsertStatement(t *testing.T) {
	cmd := "INSERT INTO Students (ROLL_NO, NAME, SUBJECT, AGE) VALUES (1, 'John Doe', 'Economics', 1 + 1011);"
	expectedVals := []Expression{
		&ValueExpression{Val: &Value{Typ: FieldTypeInteger, Val: "1"}},
		&ValueExpression{Val: &Value{Typ: FieldTypeString, Val: "'John Doe'"}},
		&ValueExpression{Val: &Value{Typ: FieldTypeString, Val: "'Economics'"}},
		&BinaryOpExpression{
			Op: OperatorPlus,
			L:  &ValueExpression{Val: &Value{Typ: FieldTypeInteger, Val: "1"}},
			R:  &ValueExpression{Val: &Value{Typ: FieldTypeInteger, Val: "1011"}},
		},
	}
	expectedCols := []string{
		"ROLL_NO",
		"NAME",
		"SUBJECT",
		"AGE",
	}

	p := NewParser("testParser", cmd)
	stmt, err := p.Parse()
	assert.Nil(t, err, "Unexpected error in parsing create table DDL")

	assert.IsType(t, &InsertStatement{}, stmt, "Unexpected type of statement. Expected a &InsertStatement")
	iStmt := stmt.(*InsertStatement)

	assert.Equal(t, "Students", iStmt.Table.Name, "Wrong table name")
	assert.Equal(t, len(expectedVals), len(iStmt.Values), "Unexpected length of values")

	for i := 0; i < len(expectedVals); i++ {
		assert.Equal(t, expectedVals[i], iStmt.Values[i], "Wrong values")
	}
	assert.Equal(t, expectedCols, iStmt.Columns, "unexpected columns")
}

func TestUpdateStatement(t *testing.T) {
	cmd := "UPDATE Students SET NAME = \"updated_name\" WHERE ROLL_NO = 1;"
	expectedWhere := &BinaryOpExpression{
		Op: OperatorEqual,
		L: &IdentifierExpression{
			Identifier: "ROLL_NO",
		},
		R: &ValueExpression{Val: &Value{Val: "1", Typ: FieldTypeInteger}},
	}
	expectedVals := []Expression{
		&BinaryOpExpression{
			Op: OperatorEqual,
			L: &IdentifierExpression{
				Identifier: "NAME",
			},
			R: &ValueExpression{Val: &Value{Val: "\"updated_name\"", Typ: FieldTypeString}},
		},
	}

	p := NewParser("testParser", cmd)
	stmt, err := p.Parse()
	assert.Nil(t, err, "Unexpected error in parsing create table DDL")

	assert.IsType(t, &UpdateStatement{}, stmt, "Unexpected type of statement. Expected an &UpdateStatement")
	uStmt := stmt.(*UpdateStatement)

	assert.Equal(t, "Students", uStmt.Table.Name, "Wrong table name")
	assert.Equal(t, expectedWhere, uStmt.Predicate, "unexpected predicate")
	assert.Equal(t, expectedVals, uStmt.Values, "unexpected values")
}

func TestDeleteStatement(t *testing.T) {
	cmd := "DELETE FROM Students WHERE ROLL_NO = 1 AND NAME = 'John Doe';"
	expectedWhere := &BinaryOpExpression{
		Op: OperatorAndAnd,
		L: &BinaryOpExpression{
			Op: OperatorEqual,
			L: &IdentifierExpression{
				Identifier: "ROLL_NO",
			},
			R: &ValueExpression{Val: &Value{Val: "1", Typ: FieldTypeInteger}},
		},
		R: &BinaryOpExpression{
			Op: OperatorEqual,
			L: &IdentifierExpression{
				Identifier: "NAME",
			},
			R: &ValueExpression{Val: &Value{Val: "'John Doe'", Typ: FieldTypeString}},
		},
	}

	p := NewParser("testParser", cmd)
	stmt, err := p.Parse()
	assert.Nil(t, err, "Unexpected error in parsing create table DDL")

	assert.IsType(t, &DeleteStatement{}, stmt, "Unexpected type of statement. Expected an &DeleteStatement")
	dStmt := stmt.(*DeleteStatement)

	assert.Equal(t, "Students", dStmt.Table.Name, "Wrong table name")
	assert.Equal(t, expectedWhere, dStmt.Predicate, "unexpected predicate")
}

//
// DQL tests
//

func TestSelectStatement(t *testing.T) {
	cmd := "SELECT * FROM Students WHERE ROLL_NO = 1 AND NAME = 'John Doe';"
	expectedWhere := &BinaryOpExpression{
		Op: OperatorAndAnd,
		L: &BinaryOpExpression{
			Op: OperatorEqual,
			L: &IdentifierExpression{
				Identifier: "ROLL_NO",
			},
			R: &ValueExpression{Val: &Value{Val: "1", Typ: FieldTypeInteger}},
		},
		R: &BinaryOpExpression{
			Op: OperatorEqual,
			L: &IdentifierExpression{
				Identifier: "NAME",
			},
			R: &ValueExpression{Val: &Value{Val: "'John Doe'", Typ: FieldTypeString}},
		},
	}
	expectedSelections := &SelectionItem{Expr: &SelectAllExpression{}}

	p := NewParser("testParser", cmd)
	stmt, err := p.Parse()
	assert.Nil(t, err, "Unexpected error in parsing create table DDL")

	assert.IsType(t, &SelectStatement{}, stmt, "Unexpected type of statement. Expected a &SelectStatement")
	selectStmt := stmt.(*SelectStatement)
	assert.IsType(t, &Table{}, selectStmt.From, "Unexpected type of from item. Expected a &Table")
	table := selectStmt.From.(*Table)

	assert.Equal(t, "Students", table.Name, "Wrong table name")
	assert.False(t, selectStmt.Distinct, "unexpected value for distinct")
	assert.Nil(t, selectStmt.Limit, "unexpected value for limit")
	assert.Equal(t, expectedWhere, selectStmt.Where, "unexpected value for where clause")
	assert.Equal(t, expectedSelections, selectStmt.Selections[0], "unexpected value for selection item")
}
