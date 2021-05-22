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

import (
	pb "github.com/dr0pdb/icecanedb/pkg/protogen/icecanedbpb"
)

// Transaction is the MVCC transaction.
// A single transaction is not thread safe.
// Operations on a single txn should be called sequentially.
type Transaction struct {
	// unique transaction id
	id uint64

	// concurrent txns contains the id of all the active txns at the start of the txn.
	// This txn should be invisible to these transactions.
	// The validation phase verifies that before commiting.
	concTxns map[uint64]bool

	// txnMode
	mode pb.TxnMode
}

// newTransaction creates a new transaction.
func newTransaction(id uint64, concTxns map[uint64]bool, mode pb.TxnMode) *Transaction {
	return &Transaction{
		id:       id,
		concTxns: concTxns,
		mode:     mode,
	}
}
