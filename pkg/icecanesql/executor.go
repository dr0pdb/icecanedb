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

package icecanesql

import "github.com/dr0pdb/icecanedb/pkg/common"

// Executor executes a query plan
type Executor interface {
	Execute(txnID uint64) Result
}

// Result denotes the result of the execution of a query plan
type Result interface {
	GetError() error
}

//
// Utility functions for all executors
//

// incrementKeyAtomic increments the value associated with the given key
// and returns the updated value
// NOTE: Assumes that the value associated with the key is a valid serialized uint64 using U64ToByteSlice
// returns 0 if the key wasn't found
func incrementKeyAtomic(rpc *rpcRepository, key []byte) (uint64, error) {
	txnID, err := rpc.beginTxn(false)
	if err != nil {
		return 0, err
	}

	_, prev, err := rpc.get(key, txnID)
	if err != nil {
		return 0, err
	}

	prevVal := common.ByteSliceToU64(prev)

	success, err := rpc.set(key, common.U64ToByteSlice(prevVal+1), txnID)
	if err != nil || !success {
		return 0, err
	}

	return prevVal + 1, nil
}
