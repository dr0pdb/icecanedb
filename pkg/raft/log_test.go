package raft

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogSerializationDeserialization(t *testing.T) {
	rl := &raftLog{
		term:    1,
		command: "my command",
	}

	b := rl.toBytes()

	drl, err := deserializeRaftLog(b)
	assert.Nil(t, err, "Unexpected error in deserializing")
	assert.Equal(t, rl.term, drl.term, fmt.Sprintf("Unexpected value for term. Expected %v, found %v", rl.term, drl.term))
	assert.Equal(t, rl.command, drl.command, fmt.Sprintf("Unexpected value for command. Expected %v, found %v", rl.command, drl.command))

	b2 := []byte{}
	drl, err = deserializeRaftLog(b2)
	assert.NotNil(t, err, "Expected an err when serializing invalid raft log bytes")
}
