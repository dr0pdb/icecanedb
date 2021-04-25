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

type raftCommandType uint64

const (
	setCmd raftCommandType = iota
	deleteCmd
	metaSetCmd
	metaDeleteCmd
)

// raftLog is serialized and stored into the raft storage
//
// In the serialized form, the raft log has the following format
//
// |                    |             |                 |                       |                                     |                                                |
// | command type [0:7] | term [8:15] | len_key [16:23] | key [24:24+len_key-1] | len_value [24+len_key:32+len_key-1] | value [32+len_key: 32 + len_key+len_value - 1] |
// |                    |             |                 |                       |                                     |                                                |
type raftLog struct {
	ct    raftCommandType
	term  uint64
	key   []byte
	value []byte
}

func (rl *raftLog) toBytes() []byte {
	res := common.U64ToByte(uint64(rl.ct))
	res = append(res, common.U64ToByte(rl.term)...)
	res = append(res, common.U64ToByte(uint64(len(rl.key)))...)
	res = append(res, rl.key...)
	res = append(res, common.U64ToByte(uint64(len(rl.value)))...)
	res = append(res, rl.value...)
	return res
}

func deserializeRaftLog(l []byte) (*raftLog, error) {
	if len(l) < 32 {
		return nil, fmt.Errorf("invalid log bytes")
	}
	ct := raftCommandType(common.ByteToU64(l[:8]))
	term := common.ByteToU64(l[8:16])

	keylen := common.ByteToU64(l[16:24])
	key := l[24 : 24+keylen]

	valueLen := common.ByteToU64(l[24+keylen : 32+keylen])
	var value []byte
	if valueLen > 0 {
		value = l[32+keylen:]
	}

	return &raftLog{
		ct:    ct,
		term:  term,
		key:   key,
		value: value,
	}, nil
}

func newSetRaftLog(term uint64, key, value []byte) *raftLog {
	return &raftLog{
		term:  term,
		key:   key,
		value: value,
		ct:    setCmd,
	}
}

func newDeleteRaftLog(term uint64, key []byte) *raftLog {
	return &raftLog{
		term: term,
		key:  key,
		ct:   deleteCmd,
	}
}

func newMetaSetRaftLog(term uint64, key, value []byte) *raftLog {
	return &raftLog{
		term:  term,
		key:   key,
		value: value,
		ct:    metaSetCmd,
	}
}

func newMetaDeleteRaftLog(term uint64, key []byte) *raftLog {
	return &raftLog{
		term: term,
		key:  key,
		ct:   metaDeleteCmd,
	}
}
