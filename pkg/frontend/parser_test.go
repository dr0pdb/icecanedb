package frontend

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateTable1(t *testing.T) {
	cmd := "CREATE TABLE Students(ROLL_NO int PRIMARY KEY UNIQUE, NAME varchar INDEX, SUBJECT varchar, AGE double, RICH bool);"
	expectedSpecs := []*ColumnSpec{
		{
			Name:       "ROLL_NO",
			Type:       ColumnTypeInteger,
			Nullable:   true,
			PrimaryKey: true,
			Unique:     true,
			Index:      false,
			References: "",
		},
		{
			Name:       "NAME",
			Type:       ColumnTypeString,
			Nullable:   true,
			PrimaryKey: false,
			Unique:     false,
			Index:      true,
			References: "",
		},
		{
			Name:       "SUBJECT",
			Type:       ColumnTypeString,
			Nullable:   true,
			PrimaryKey: false,
			Unique:     false,
			Index:      false,
			References: "",
		},
		{
			Name:       "AGE",
			Type:       ColumnTypeFloat,
			Nullable:   true,
			PrimaryKey: false,
			Unique:     false,
			Index:      false,
			References: "",
		},
		{
			Name:       "RICH",
			Type:       ColumnTypeBoolean,
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

// Incorrect statements
func TestCreateTable2(t *testing.T) {
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
