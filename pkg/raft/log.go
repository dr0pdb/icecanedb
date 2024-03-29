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

package raft

import (
	"fmt"

	common "github.com/dr0pdb/icecanedb/pkg/common"
)

type RaftCommandType uint64

const (
	SetCmd RaftCommandType = iota
	DeleteCmd
	MetaSetCmd
	MetaDeleteCmd
)

// RaftLog is serialized and stored into the raft storage
//
// In the serialized form, the raft log has the following format
//
// |                    |             |                 |                       |                                     |                                                |
// | command type [0:7] | term [8:15] | len_key [16:23] | key [24:24+len_key-1] | len_value [24+len_key:32+len_key-1] | value [32+len_key: 32 + len_key+len_value - 1] |
// |                    |             |                 |                       |                                     |                                                |
type RaftLog struct {
	Ct    RaftCommandType
	Term  uint64
	Key   []byte
	Value []byte
}

func (rl *RaftLog) toBytes() []byte {
	res := common.U64ToByteSlice(uint64(rl.Ct))
	res = append(res, common.U64ToByteSlice(rl.Term)...)
	res = append(res, common.U64ToByteSlice(uint64(len(rl.Key)))...)
	res = append(res, rl.Key...)
	res = append(res, common.U64ToByteSlice(uint64(len(rl.Value)))...)
	res = append(res, rl.Value...)
	return res
}

func deserializeRaftLog(l []byte) (*RaftLog, error) {
	if len(l) < 32 {
		return nil, fmt.Errorf("invalid log bytes")
	}
	ct := RaftCommandType(common.ByteSliceToU64(l[:8]))
	term := common.ByteSliceToU64(l[8:16])

	keylen := common.ByteSliceToU64(l[16:24])
	key := l[24 : 24+keylen]

	valueLen := common.ByteSliceToU64(l[24+keylen : 32+keylen])
	var value []byte
	if valueLen > 0 {
		value = l[32+keylen:]
	}

	return &RaftLog{
		Ct:    ct,
		Term:  term,
		Key:   key,
		Value: value,
	}, nil
}

func newSetRaftLog(term uint64, key, value []byte) *RaftLog {
	return &RaftLog{
		Term:  term,
		Key:   key,
		Value: value,
		Ct:    SetCmd,
	}
}

func newDeleteRaftLog(term uint64, key []byte) *RaftLog {
	return &RaftLog{
		Term: term,
		Key:  key,
		Ct:   DeleteCmd,
	}
}

func newMetaSetRaftLog(term uint64, key, value []byte) *RaftLog {
	return &RaftLog{
		Term:  term,
		Key:   key,
		Value: value,
		Ct:    MetaSetCmd,
	}
}

func newMetaDeleteRaftLog(term uint64, key []byte) *RaftLog {
	return &RaftLog{
		Term: term,
		Key:  key,
		Ct:   MetaDeleteCmd,
	}
}
