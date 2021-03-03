package raft

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetLogSerializationDeserialization(t *testing.T) {
	rl := newSetRaftLog(1, []byte("name"), []byte("saurav"))

	b := rl.toBytes()

	drl, err := deserializeRaftLog(b)
	assert.Nil(t, err, "Unexpected error in deserializing")
	assert.Equal(t, rl.term, drl.term, fmt.Sprintf("Unexpected value for term. Expected %v, found %v", rl.term, drl.term))
	assert.Equal(t, rl.command, drl.command, fmt.Sprintf("Unexpected value for command. Expected %v, found %v", rl.command, drl.command))
	assert.Equal(t, rl.ct, drl.ct, fmt.Sprintf("Unexpected ct for command. Expected %v, found %v", rl.ct, drl.ct))

	b2 := []byte{}
	drl, err = deserializeRaftLog(b2)
	assert.NotNil(t, err, "Expected an err when serializing invalid raft log bytes")
}

func TestDeleteLogSerializationDeserialization(t *testing.T) {
	rl := newDeleteRaftLog(1, []byte("name"))

	b := rl.toBytes()

	drl, err := deserializeRaftLog(b)
	assert.Nil(t, err, "Unexpected error in deserializing")
	assert.Equal(t, rl.term, drl.term, fmt.Sprintf("Unexpected value for term. Expected %v, found %v", rl.term, drl.term))
	assert.Equal(t, rl.command, drl.command, fmt.Sprintf("Unexpected value for command. Expected %v, found %v", rl.command, drl.command))
	assert.Equal(t, rl.ct, drl.ct, fmt.Sprintf("Unexpected ct for command. Expected %v, found %v", rl.ct, drl.ct))

	b2 := []byte{}
	drl, err = deserializeRaftLog(b2)
	assert.NotNil(t, err, "Expected an err when serializing invalid raft log bytes")
}
