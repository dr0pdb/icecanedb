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
