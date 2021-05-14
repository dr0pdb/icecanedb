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
	assert.Equal(t, rl.Term, drl.Term, fmt.Sprintf("Unexpected value for term. Expected %v, found %v", rl.Term, drl.Term))
	assert.Equal(t, rl.Key, drl.Key, fmt.Sprintf("Unexpected value for key. Expected %v, found %v", rl.Key, drl.Key))
	assert.Equal(t, rl.Value, drl.Value, fmt.Sprintf("Unexpected value for value. Expected %v, found %v", rl.Value, drl.Value))
	assert.Equal(t, rl.Ct, drl.Ct, fmt.Sprintf("Unexpected ct for command. Expected %v, found %v", rl.Ct, drl.Ct))

	b2 := []byte{}
	_, err = deserializeRaftLog(b2)
	assert.NotNil(t, err, "Expected an err when serializing invalid raft log bytes")
}

func TestDeleteLogSerializationDeserialization(t *testing.T) {
	rl := newDeleteRaftLog(1, []byte("name"))

	b := rl.toBytes()

	drl, err := deserializeRaftLog(b)
	assert.Nil(t, err, "Unexpected error in deserializing")
	assert.Equal(t, rl.Term, drl.Term, fmt.Sprintf("Unexpected value for term. Expected %v, found %v", rl.Term, drl.Term))
	assert.Equal(t, rl.Key, drl.Key, fmt.Sprintf("Unexpected value for key. Expected %v, found %v", rl.Key, drl.Key))
	assert.Equal(t, rl.Ct, drl.Ct, fmt.Sprintf("Unexpected ct for command. Expected %v, found %v", rl.Ct, drl.Ct))

	b2 := []byte{}
	_, err = deserializeRaftLog(b2)
	assert.NotNil(t, err, "Expected an err when serializing invalid raft log bytes")
}
