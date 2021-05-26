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

package mvcc

import "github.com/dr0pdb/icecanedb/pkg/common"

// txnValue contains the value of a key value pair set by a txn
// It's format is as follows
// | user value (variable size) | delete flag (1 byte) |
//
// In case of set, the flag is 0 and the user value contains the actual user value written by the txn
// Otherwise, the flag is 1 and the user value is empty
type txnValue []byte

func newSetTxnValue(value []byte) txnValue {
	var tVal txnValue = make(txnValue, len(value)+1)
	i := copy(tVal, value)
	tVal[i] = common.BoolToByte(false)
	return tVal
}

func newDeleteTxnValue() txnValue {
	var tVal txnValue = make(txnValue, 1)
	tVal[0] = common.BoolToByte(true)
	return tVal
}

func (t txnValue) getUserValue() []byte {
	if len(t) > 1 {
		return []byte(t[:len(t)-1])
	}
	return []byte{}
}

func (t txnValue) getDeleteFlag() bool {
	return common.ByteToBool(t[len(t)-1])
}
