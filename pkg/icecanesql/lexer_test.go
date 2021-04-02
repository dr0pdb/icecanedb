package icecanesql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var testName = "testLexer"

/*
	Example SQL statements to support

	DDL - Data Definition Language
	a. CREATE DATABASE my_awesome_database;
	b. CREATE TABLE Students(ROLL_NO int(3), NAME varchar(20), SUBJECT varchar(20));
	c. DROP DATABASE my_awesome_database;
	d. DROP TABLE Students;
    f. TRUNCATE TABLE Students;

	DML - Data Manipulation Language
	a.

	DQL - Data Query Language
	a.

	TCL - Transaction Control Language
	a.
*/

//
// DDL tests
//

func TestDDLLexer1(t *testing.T) {
	cmd := "CREATE DATABASE my_awesome_database;"

	expectedResult := []item{
		{typ: itemKeyword, val: "CREATE"},
		{typ: itemKeyword, val: "DATABASE"},
		{typ: itemIdentifier, val: "my_awesome_database"},
		{typ: itemSemicolon, val: ";"},
		{typ: itemEOF, val: ""},
	}

	_, items := newLexer(testName, cmd)
	idx := 0
	for it := range items {
		if it.typ == itemWhitespace {
			continue
		}

		assert.Equal(t, expectedResult[idx].typ, it.typ, "Unexpected typ")
		assert.Equal(t, expectedResult[idx].val, it.val, "Unexpected val")
		idx++
	}
}

func TestDDLLexer2(t *testing.T) {
	cmd := "CREATE TABLE Students(ROLL_NO int(3), NAME varchar(20), SUBJECT varchar(20));"

	expectedResult := []item{
		{typ: itemKeyword, val: "CREATE"},
		{typ: itemKeyword, val: "TABLE"},
		{typ: itemIdentifier, val: "Students"},
		{typ: itemLeftParen, val: "("},
		{typ: itemIdentifier, val: "ROLL_NO"},
		{typ: itemKeyword, val: "int"},
		{typ: itemLeftParen, val: "("},
		{typ: itemNumber, val: "3"},
		{typ: itemRightParen, val: ")"},
		{typ: itemComma, val: ","},
		{typ: itemIdentifier, val: "NAME"},
		{typ: itemKeyword, val: "varchar"},
		{typ: itemLeftParen, val: "("},
		{typ: itemNumber, val: "20"},
		{typ: itemRightParen, val: ")"},
		{typ: itemComma, val: ","},
		{typ: itemIdentifier, val: "SUBJECT"},
		{typ: itemKeyword, val: "varchar"},
		{typ: itemLeftParen, val: "("},
		{typ: itemNumber, val: "20"},
		{typ: itemRightParen, val: ")"},
		{typ: itemRightParen, val: ")"},
		{typ: itemSemicolon, val: ";"},
		{typ: itemEOF, val: ""},
	}

	_, items := newLexer(testName, cmd)
	idx := 0
	for it := range items {
		if it.typ == itemWhitespace {
			continue
		}

		assert.Equal(t, expectedResult[idx].typ, it.typ, "Unexpected typ")
		assert.Equal(t, expectedResult[idx].val, it.val, "Unexpected val")
		idx++
	}
}

func TestDDLLexer3(t *testing.T) {
	cmd := "DROP DATABASE my_awesome_database;"

	expectedResult := []item{
		{typ: itemKeyword, val: "DROP"},
		{typ: itemKeyword, val: "DATABASE"},
		{typ: itemIdentifier, val: "my_awesome_database"},
		{typ: itemSemicolon, val: ";"},
		{typ: itemEOF, val: ""},
	}

	_, items := newLexer(testName, cmd)
	idx := 0
	for it := range items {
		if it.typ == itemWhitespace {
			continue
		}

		assert.Equal(t, expectedResult[idx].typ, it.typ, "Unexpected typ")
		assert.Equal(t, expectedResult[idx].val, it.val, "Unexpected val")
		idx++
	}
}

func TestDDLLexer4(t *testing.T) {
	cmd := "DROP TABLE Students;"

	expectedResult := []item{
		{typ: itemKeyword, val: "DROP"},
		{typ: itemKeyword, val: "TABLE"},
		{typ: itemIdentifier, val: "Students"},
		{typ: itemSemicolon, val: ";"},
		{typ: itemEOF, val: ""},
	}

	_, items := newLexer(testName, cmd)
	idx := 0
	for it := range items {
		if it.typ == itemWhitespace {
			continue
		}

		assert.Equal(t, expectedResult[idx].typ, it.typ, "Unexpected typ")
		assert.Equal(t, expectedResult[idx].val, it.val, "Unexpected val")
		idx++
	}
}

func TestDDLLexer5(t *testing.T) {
	cmd := "TRUNCATE TABLE Students;"

	expectedResult := []item{
		{typ: itemKeyword, val: "TRUNCATE"},
		{typ: itemKeyword, val: "TABLE"},
		{typ: itemIdentifier, val: "Students"},
		{typ: itemSemicolon, val: ";"},
		{typ: itemEOF, val: ""},
	}

	_, items := newLexer(testName, cmd)
	idx := 0
	for it := range items {
		if it.typ == itemWhitespace {
			continue
		}

		assert.Equal(t, expectedResult[idx].typ, it.typ, "Unexpected typ")
		assert.Equal(t, expectedResult[idx].val, it.val, "Unexpected val")
		idx++
	}
}

//
// DQL tests
//

func TestDQLLexer1(t *testing.T) {
	cmd := "SELECT * FROM tablename WHERE x = 2;"

	expectedResult := []item{
		{typ: itemKeyword, val: "SELECT"},
		{typ: itemAsterisk, val: "*"},
		{typ: itemKeyword, val: "FROM"},
		{typ: itemIdentifier, val: "tablename"},
		{typ: itemKeyword, val: "WHERE"},
		{typ: itemIdentifier, val: "x"},
		{typ: itemEqual, val: "="},
		{typ: itemNumber, val: "2"},
		{typ: itemSemicolon, val: ";"},
		{typ: itemEOF, val: ""},
	}

	_, items := newLexer(testName, cmd)
	idx := 0
	for it := range items {
		if it.typ == itemWhitespace {
			continue
		}

		assert.Equal(t, expectedResult[idx].typ, it.typ, "Unexpected typ")
		assert.Equal(t, expectedResult[idx].val, it.val, "Unexpected val")
		idx++
	}
}
