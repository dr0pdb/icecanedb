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
	"testing"

	"github.com/stretchr/testify/assert"
)

var testName = "testLexer"

/*
	Example SQL statements to support

	DDL - Data Definition Language
	a. CREATE TABLE Students(ROLL_NO int, NAME varchar, SUBJECT varchar);
	b. DROP TABLE Students;
    c. TRUNCATE TABLE Students;

	DML - Data Manipulation Language
	a.

	DQL - Data Query Language
	a.

	TCL - Transaction Control Language
	a. BEGIN
	b. BEGIN READ ONLY
	c. BEGIN READ WRITE
	d. COMMIT
	e. ROLLBACK
*/

//
// DDL tests
//

func TestDDLLexer1(t *testing.T) {
	cmd := "CREATE TABLE Students(ROLL_NO int, NAME varchar, SUBJECT varchar);"

	expectedResult := []item{
		{typ: itemKeyword, val: "CREATE"},
		{typ: itemKeyword, val: "TABLE"},
		{typ: itemIdentifier, val: "Students"},
		{typ: itemLeftParen, val: "("},
		{typ: itemIdentifier, val: "ROLL_NO"},
		{typ: itemKeyword, val: "int"},
		{typ: itemComma, val: ","},
		{typ: itemIdentifier, val: "NAME"},
		{typ: itemKeyword, val: "varchar"},
		{typ: itemComma, val: ","},
		{typ: itemIdentifier, val: "SUBJECT"},
		{typ: itemKeyword, val: "varchar"},
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

func TestDDLLexer2(t *testing.T) {
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

func TestDDLLexer3(t *testing.T) {
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
// TQL tests
//

func TestTQLBeginValid(t *testing.T) {
	cmds := []string{
		"BEGIN;",
		"BEGIN READ ONLY;",
		"BEGIN READ WRITE;",
	}

	expectedResult := [][]item{
		{
			{typ: itemKeyword, val: "BEGIN"},
			{typ: itemSemicolon, val: ";"},
			{typ: itemEOF, val: ""},
		},
		{
			{typ: itemKeyword, val: "BEGIN"},
			{typ: itemKeyword, val: "READ"},
			{typ: itemKeyword, val: "ONLY"},
			{typ: itemSemicolon, val: ";"},
			{typ: itemEOF, val: ""},
		},
		{
			{typ: itemKeyword, val: "BEGIN"},
			{typ: itemKeyword, val: "READ"},
			{typ: itemKeyword, val: "WRITE"},
			{typ: itemSemicolon, val: ";"},
			{typ: itemEOF, val: ""},
		},
	}

	for i := range cmds {
		_, items := newLexer(testName, cmds[i])
		idx := 0
		for it := range items {
			if it.typ == itemWhitespace {
				continue
			}

			assert.Equal(t, expectedResult[i][idx].typ, it.typ, "Unexpected typ")
			assert.Equal(t, expectedResult[i][idx].val, it.val, "Unexpected val")
			idx++
		}
	}
}

func TestTQLFinishValid(t *testing.T) {
	cmds := []string{
		"COMMIT;",
		"ROLLBACK;",
	}

	expectedResult := [][]item{
		{
			{typ: itemKeyword, val: "COMMIT"},
			{typ: itemSemicolon, val: ";"},
			{typ: itemEOF, val: ""},
		},
		{
			{typ: itemKeyword, val: "ROLLBACK"},
			{typ: itemSemicolon, val: ";"},
			{typ: itemEOF, val: ""},
		},
	}

	for i := range cmds {
		_, items := newLexer(testName, cmds[i])
		idx := 0
		for it := range items {
			if it.typ == itemWhitespace {
				continue
			}

			assert.Equal(t, expectedResult[i][idx].typ, it.typ, "Unexpected typ")
			assert.Equal(t, expectedResult[i][idx].val, it.val, "Unexpected val")
			idx++
		}
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
