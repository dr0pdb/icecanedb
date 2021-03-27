package icecanesql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var testName = "testLexer"

func TestLexer1(t *testing.T) {
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
