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
	"bytes"

	"github.com/dr0pdb/icecanedb/pkg/common"
	"github.com/dr0pdb/icecanedb/pkg/storage"
	log "github.com/sirupsen/logrus"
)

type keyType int

const (
	nxtTxnID keyType = iota
	activeTxn
	txnSnapshot
	txnWrite
)

const (
	notUsed = 0
)

// getKey returns key for the given mvcc key type
func getKey(txnID uint64, keyType keyType, key []byte) []byte {
	log.Info("mvcc::key::getKey; start")

	switch keyType {
	case nxtTxnID:
		return []byte("nxtTxnID")
	case activeTxn:
		res := []byte("activeTxn")
		res = append(res, common.U64ToByteSlice(txnID)...)
		return res
	case txnSnapshot:
		res := []byte("txnSnapshot")
		res = append(res, common.U64ToByteSlice(txnID)...)
		return res
	case txnWrite:
		res := []byte("txnWrite")
		res = append(res, common.U64ToByteSlice(txnID)...)
		res = append(res, key...)
		return res
	}

	panic("invalid key type")
}

func getTxnWrite(b []byte) (uint64, []byte) {
	idx := len([]byte("txnWrite"))
	idb := b[idx : idx+8]
	k := b[idx+8:]
	id := common.ByteSliceToU64(idb)
	return id, k
}

// TxnKey contains the key and 8 byte txnID
type TxnKey []byte

func newTxnKey(key []byte, txnID uint64) TxnKey {
	var tKey TxnKey = make(TxnKey, len(key)+8)

	i := copy(tKey, key)
	tKey[i] = uint8(txnID) // last byte of txnID
	tKey[i+1] = uint8(txnID >> 8)
	tKey[i+2] = uint8(txnID >> 16)
	tKey[i+3] = uint8(txnID >> 24)
	tKey[i+4] = uint8(txnID >> 32)
	tKey[i+5] = uint8(txnID >> 40)
	tKey[i+6] = uint8(txnID >> 48)
	tKey[i+7] = uint8(txnID >> 56)

	return tKey
}

// userKey extracts the user key from the txn key and returns a new slice.
// assumes that the txn key is valid. Will panic if it isn't.
func (tk TxnKey) userKey() []byte {
	return []byte(tk[:len(tk)-8])
}

// txnID returns the sequence number of the txn key.
// assumes that the txn key is valid. Will panic if it isn't
func (tk TxnKey) txnID() uint64 {
	var ti uint64 = 0

	i := len(tk) - 8
	ti |= uint64(tk[i])
	ti |= uint64(tk[i+1]) << 8
	ti |= uint64(tk[i+2]) << 16
	ti |= uint64(tk[i+3]) << 24
	ti |= uint64(tk[i+4]) << 32
	ti |= uint64(tk[i+5]) << 40
	ti |= uint64(tk[i+6]) << 48
	ti |= uint64(tk[i+7]) << 56

	return ti
}

// valid returns if the txn key is valid structurally.
func (tk TxnKey) valid() bool {
	return len(tk) >= 8
}

// TxnKeyComparator is the comparator which compares mvcc txn keys.
//
// keys are first compared for their user key according to the default byte comparison.
// ties are broken by comparing txn id (decreasing).
type TxnKeyComparator struct{}

// Compare compares two byte slices assuming they're TxnKey
func (d *TxnKeyComparator) Compare(a, b []byte) int {
	akey, bkey := TxnKey(a), TxnKey(b)
	if !akey.valid() {
		if bkey.valid() {
			return -1
		}
		return bytes.Compare(a, b) // both invalid, so return byte wise comparison
	}
	if !bkey.valid() {
		return 1
	}

	if ukc := bytes.Compare(akey.userKey(), bkey.userKey()); ukc != 0 {
		return ukc
	}

	if atxnID, btxnID := akey.txnID(), bkey.txnID(); atxnID < btxnID {
		return 1
	} else if atxnID > btxnID {
		return -1
	}

	return 0
}

// Name returns the name of the comparator.
func (d *TxnKeyComparator) Name() string {
	return "txnKeyComparator"
}

// NewtxnKeyComparator creates a new instance of an txnKeyComparator
// returns a pointer to the Comparator interface.
func NewtxnKeyComparator() storage.Comparator {
	return &TxnKeyComparator{}
}
