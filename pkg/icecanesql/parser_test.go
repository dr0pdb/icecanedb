package icecanesql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDDLParser1(t *testing.T) {
	cmd := "CREATE TABLE Students(ROLL_NO int, NAME varchar, SUBJECT varchar);"

	p := NewParser("testParser", cmd)
	_, err := p.Parse()
	assert.Nil(t, err, "Unexpected error in parsing create table DDL")
}
