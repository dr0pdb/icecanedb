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
	"context"
	"fmt"
	"math"
	"sync"

	icommon "github.com/dr0pdb/icecanedb/internal/common"
	"github.com/dr0pdb/icecanedb/pkg/common"
	pb "github.com/dr0pdb/icecanedb/pkg/protogen/icecanedbpb"
	"github.com/dr0pdb/icecanedb/pkg/raft"
	log "github.com/sirupsen/logrus"
)

/*
	Every request is part of a txn. If the request body contains a txn id,
	then the request is considered to be part of that txn.
	Otherwise, we create a new txn for the single request.

	This is important for the ACID guarantees.

	Each Key is appended by the txn id that created/modified it.

	MVCC uses TxnKeyComparator in keys.go to order the keys written using MVCC in the kv storage.
	Rules for sorting:
	1. Keys are first sorted according to the user key in increasing order
	2. For the same user key, the keys are ordered in decreasing order of the transaction id that wrote it.

	For eg. If 3 transactions wrote values for 2 user keys, then the effective order would be:
	UserKey 1
		Txn 3
		Txn 2
		Txn 1
	UserKey 2
		Txn 3
		Txn 2
		Txn 1

	Assuming that lexicographically UserKey 2 > UserKey 1.

	We also store the txn metadata in kv-meta storage. kv-meta storage uses the default (lexicographical) comparator for keys.
	keys are stored as |"txnWrite" | txn_id (64 bits) | user_key (byte slice of varying length)

	As a result of the above format, meta storage keys are grouped by their transaction id
	which helps in efficiently retrieving all the writes done by a single txn in case of rollback.
*/

// MVCC is the Multi Version Concurrency Control layer for transactions.
// Operations on it are thread safe using a RWMutex
type MVCC struct {
	id uint64

	mu *sync.RWMutex

	// active transactions at the present moment.
	activeTxn map[uint64]*Transaction

	// the underlying raft node.
	rs raft.IcecaneRaftServer
}

//
// Public methods called from the grpc server
//

// BeginTxn begins a MVCC transaction providing ACID guarantees.
func (m *MVCC) BeginTxn(ctx context.Context, req *pb.BeginTxnRequest) (*pb.BeginTxnResponse, error) {
	log.WithFields(log.Fields{"id": m.id}).Info("mvcc::mvcc::BeginTxn; started")

	txn, isLeader, err := m.begin(req.Mode)
	resp := &pb.BeginTxnResponse{
		IsLeader: isLeader,
	}

	if err != nil {
		return resp, err
	}
	if !isLeader {
		resp.Success = false
		return resp, nil
	}

	resp.TxnId = txn.id
	resp.Success = true

	log.WithFields(log.Fields{"id": m.id, "createdTxnId": resp.TxnId}).Info("mvcc::mvcc::BeginTxn; done")
	return resp, nil
}

// Get gets the value of a key.
func (m *MVCC) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Info("mvcc::mvcc::Get; started")

	resp := &pb.GetResponse{
		IsLeader: true,
	}
	inlinedTxn := (req.TxnId == 0)

	txnID, err := m.ensureTxn(req.TxnId, pb.TxnMode_ReadOnly)
	if err != nil {
		return nil, err
	}
	req.TxnId = txnID

	txn := m.activeTxn[txnID]

	tk := newTxnKey(req.GetKey(), req.GetTxnId())
	itr, isLeader, err := m.rs.Scan(tk)

	if err != nil {
		resp.Error = err.Error()
		log.WithFields(log.Fields{"txnID": req.TxnId}).Error(fmt.Sprintf("mvcc::mvcc::Get; error in scan: %v", err.Error()))
		return resp, nil
	}

	if !isLeader {
		resp.IsLeader = false
		return resp, nil
	}

	// iterate through the kv pairs sorted (decreasing) by txnID.
	// skip the ones which were active during the txn init.
	for {
		if itr.Valid() {
			log.WithFields(log.Fields{"txnID": req.TxnId}).Info("mvcc::mvcc::Get; valid node on the iterator")
			tk := TxnKey(itr.Key())
			uk := tk.userKey()
			if !bytes.Equal(uk, req.GetKey()) {
				break
			}

			tid := tk.txnID()
			if _, ok := txn.concTxns[tid]; !ok {
				log.WithFields(log.Fields{"txnID": req.TxnId, "tid": tid}).Info("mvcc::mvcc::Get; found a value for the key")
				if len(itr.Value()) > 0 {
					resp.Value = itr.Value()
					resp.Found = true
				} else {
					resp.Found = false
				}

				break
			} else {
				log.WithFields(log.Fields{"txnID": req.TxnId, "tid": tid}).Info("mvcc::mvcc::Get; found a conflicting txn; skipping")
				itr.Next()
			}
		} else {
			break
		}
	}

	if resp.Found {
		log.WithFields(log.Fields{"txnID": req.TxnId, "value": string(resp.Value)}).Info("mvcc::mvcc::Get; found value")
	}

	if inlinedTxn {
		log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Info("mvcc::mvcc::Get; inlined txn. committing..")
		_, err = m.commitTxn(req.TxnId)
		if err != nil {
			// todo: handle
		}
	}

	if err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// Set sets the value of a key.
//
// It first checks if there is a concurrent txn (snapshot taken while creation of txn)
// that has already written for the key.
// If yes, we return an error and let the client decide whether it wants to abort the txn or not do the write at all.
// If no, then the write is done and recorded via (txnId, key) entry.
func (m *MVCC) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Info("mvcc::mvcc::Set; started")

	resp := &pb.SetResponse{
		IsLeader: true,
	}
	inlinedTxn := (req.TxnId == 0)

	txnID, err := m.ensureTxn(req.TxnId, pb.TxnMode_ReadWrite)
	if err != nil {
		log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Error("mvcc::mvcc::Set; unable to ensure txn")
		return resp, err
	}
	req.TxnId = txnID

	txn := m.activeTxn[txnID]
	log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Info("mvcc::mvcc::Set; checking for conflicting sets")

	isLeader, err := m.checkForConflictingWrites(txn, txnID, req.GetKey())
	if !isLeader {
		resp.IsLeader = false
		return resp, nil
	}
	if err != nil {
		resp.Error = "Serialization error"
		log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Warn("mvcc::mvcc::Set; found conflicting set")
		return resp, nil
	}

	log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Info("mvcc::mvcc::Set; no conflicts. writing now")
	m.mu.Lock()

	// no conflicts. do the write now
	tKey := newTxnKey(req.GetKey(), req.TxnId)
	_, err = m.rs.SetValue(tKey, req.GetValue(), false)
	if err != nil {
		// log it

		m.mu.Unlock()
		return resp, err
	}
	upKey := getKey(req.TxnId, txnWrite, req.GetKey())
	_, err = m.rs.SetValue(upKey, []byte("true"), true)
	if err != nil {
		// TODO: remove the previous write.

		m.mu.Unlock()
		return resp, err
	}

	m.mu.Unlock() // commitTxn aquires the lock
	if inlinedTxn {
		log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Info("mvcc::mvcc::Set; inlined txn. committing..")
		_, err := m.commitTxn(req.TxnId)
		if err != nil {
			// TODO: handle
		}
	}

	resp.Success = true
	log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Info("mvcc::mvcc::Set; done")
	return resp, err
}

// Delete deletes the value of a key.
// TODO: remove excessive code duplication b/w set and delete.
func (m *MVCC) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	log.WithFields(log.Fields{"peeId": m.id, "txnId": req.TxnId}).Info("mvcc::mvcc::Delete; started")

	resp := &pb.DeleteResponse{
		IsLeader: true,
	}
	inlinedTxn := (req.TxnId == 0)

	txnID, err := m.ensureTxn(req.TxnId, pb.TxnMode_ReadWrite)
	if err != nil {
		return nil, err
	}
	req.TxnId = txnID
	txn := m.activeTxn[txnID]

	log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Info("mvcc::mvcc::Delete; checking for conflicting writes")

	isLeader, err := m.checkForConflictingWrites(txn, txnID, req.GetKey())
	if !isLeader {
		resp.IsLeader = false
		return resp, nil
	}
	if err != nil {
		resp.Error = "Serialization error"
		log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Warn("mvcc::mvcc::Delete; found conflicting write")
		return resp, nil
	}

	m.mu.Lock()
	log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Info("mvcc::mvcc::Delete; no conflicts. writing now")

	// no conflicts. do the write now
	tKey := newTxnKey(req.GetKey(), req.TxnId)
	_, err = m.rs.DeleteValue(tKey, false)
	if err != nil {
		// log it
		m.mu.Unlock()
		return resp, err
	}
	upKey := getKey(req.TxnId, txnWrite, req.GetKey())
	_, err = m.rs.SetValue(upKey, []byte("true"), true)
	if err != nil {
		// TODO: remove the previous write.
		m.mu.Unlock()
		return resp, err
	}

	m.mu.Unlock() // commitTxn aquires the lock
	if inlinedTxn {
		log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Info("mvcc::mvcc::Delete; inlined txn. committing..")
		_, err := m.commitTxn(req.TxnId)
		if err != nil {
			// TODO: handle
		}
	}

	resp.Success = true
	return resp, err
}

// CommitTxn attempts to commit a MVCC txn
func (m *MVCC) CommitTxn(ctx context.Context, req *pb.CommitTxnRequest) (*pb.CommitTxnResponse, error) {
	log.WithFields(log.Fields{"peeId": m.id, "txnId": req.TxnId}).Info("mvcc::mvcc::CommitTxn; started")

	resp := &pb.CommitTxnResponse{
		IsLeader: true,
	}
	isLeader, err := m.commitTxn(req.TxnId)
	if !isLeader {
		resp.IsLeader = false
		return resp, nil
	}

	if err != nil {
		log.WithFields(log.Fields{"peeId": m.id, "txnId": req.TxnId}).Info("mvcc::mvcc::CommitTxn; error in committing txn")
		resp.Error = err.Error()
		return resp, err
	}
	resp.Success = true
	log.WithFields(log.Fields{"peeId": m.id, "txnId": req.TxnId}).Info("mvcc::mvcc::CommitTxn; ending successfully")
	return resp, nil
}

// RollbackTxn rollsback a MVCC txn
func (m *MVCC) RollbackTxn(ctx context.Context, req *pb.RollbackTxnRequest) (*pb.RollbackTxnResponse, error) {
	log.WithFields(log.Fields{"peeId": m.id, "txnId": req.TxnId}).Info("mvcc::mvcc::RollbackTxn; started")

	resp := &pb.RollbackTxnResponse{
		IsLeader: true,
	}

	isLeader, err := m.rollbackTxn(req.TxnId)
	if !isLeader {
		resp.IsLeader = false
		return resp, nil
	}

	if err != nil {
		log.WithFields(log.Fields{"peeId": m.id, "txnId": req.TxnId}).Error("mvcc::mvcc::RollbackTxn; error in rolling back txn")
		resp.Error = err.Error()
		return resp, err
	}
	resp.Success = true
	log.WithFields(log.Fields{"peeId": m.id, "txnId": req.TxnId}).Info("mvcc::mvcc::RollbackTxn; ending successfully")
	return resp, nil
}

//
// Internal methods
//

// begin begins a new Transaction
// holds a lock on the struct.
func (m *MVCC) begin(mode pb.TxnMode) (*Transaction, bool, error) {
	log.Info("mvcc::mvcc::begin; started")

	m.mu.Lock()
	defer m.mu.Unlock()

	// get next txn id
	nxtIDUint64, isLeader, err := m.getNextTxnID()
	if err != nil {
		log.Error(fmt.Sprintf("mvcc::mvcc::begin; error in getting next txnKey. err: %v", err.Error()))
		return nil, isLeader, err
	}

	// set next txn id
	txnKey := []byte(getKey(notUsed, nxtTxnID, nil))
	_, err = m.rs.SetValue(txnKey, common.U64ToByteSlice(nxtIDUint64+1), true)
	if err != nil {
		log.Error(fmt.Sprintf("mvcc::mvcc::begin; error in setting next txnKey. err: %v", err.Error()))
		return nil, isLeader, err
	}

	// snapshot of active txns at the moment of creation.
	cTxns := make(map[uint64]bool)
	var cTxnsS []uint64
	for k := range m.activeTxn {
		cTxns[k] = true
		cTxnsS = append(cTxnsS, k)
	}

	// persist snapshot and mark txn as active.
	snapkey := getKey(nxtIDUint64, txnSnapshot, nil)
	_, err = m.rs.SetValue([]byte(snapkey), common.U64SliceToByteSlice(cTxnsS), true)
	if err != nil {
		log.WithFields(log.Fields{"id": nxtIDUint64}).Error(fmt.Sprintf("mvcc::mvcc::begin; error in setting snapshot. err: %v", err.Error()))
		return nil, isLeader, err
	}
	markKey := getKey(nxtIDUint64, activeTxn, nil)
	_, err = m.rs.SetValue([]byte(markKey), []byte("true"), true)
	if err != nil {
		log.WithFields(log.Fields{"id": nxtIDUint64}).Error(fmt.Sprintf("mvcc::mvcc::begin; error in setting mark key. err: %v", err.Error()))
		return nil, isLeader, err
	}

	txn := newTransaction(nxtIDUint64, cTxns, mode)
	m.activeTxn[txn.id] = txn

	log.Info("mvcc::mvcc::begin; done")
	return txn, isLeader, nil
}

// ensureTxn ensures a txn exists.
// if id = 0, it creates a new txn with the given mode.
// if id != 0, it ensures that the txn is active.
// IMP: Don't hold locks while calling this function.
func (m *MVCC) ensureTxn(id uint64, mode pb.TxnMode) (uint64, error) {
	log.WithFields(log.Fields{"id": m.id, "txnId": id}).Info("mvcc::mvcc::ensureTxn; starting")
	if id == 0 {
		txnID, _, err := m.begin(mode)
		if err != nil {
			return 0, err
		}

		id = txnID.id
	} else {
		m.mu.RLock()
		defer m.mu.RUnlock()

		if _, ok := m.activeTxn[id]; !ok {
			return id, fmt.Errorf("error: inactive transaction")
		}
	}

	log.Info("mvcc::mvcc::ensureTxn; done")
	return id, nil
}

// checkForConflictingWrites checks if there is a concurrent write that conflicts with the write being attempted.
// IMP: Holds lock while execution. Don't hold lock while calling this function.
func (m *MVCC) checkForConflictingWrites(txn *Transaction, txnID uint64, key []byte) (isLeader bool, err error) {
	log.WithFields(log.Fields{"id": m.id, "txnID": txnID}).Info("mvcc::mvcc::checkForConflictingWrites; started")

	m.mu.Lock()
	defer m.mu.Unlock()

	// Iterate over the snapshot of concurrent txns to see if
	// one of those txns have written a value for the key which we're trying to write.
	for ctxn := range txn.concTxns {
		upkey := getKey(ctxn, txnWrite, key)

		// we don't care about the value. We need to check for existence.
		_, isLeader, err := m.rs.MetaGetValue(upkey)
		if !isLeader {
			return false, nil
		}
		if err == nil {
			err = fmt.Errorf("conflicting write")
			log.WithFields(log.Fields{"id": m.id, "txnID": txnID}).Error("mvcc::mvcc::checkForConflictingWrites; found conflicting set")
			return true, err
		}
	}

	// Iterate over any writes with the same key which were done by txn with higher txn id.
	// These could not have been included in the snapshot hence a separate check
	// since for a single user key, the keys are ordered in decreasing order to txn id, we start from max possible txn id
	itr, isLeader, _ := m.rs.MetaScan(getKey(math.MaxUint64, txnWrite, key))
	if !isLeader {
		return false, nil
	}
	if itr.Valid() {
		tid, uk := getTxnWrite(itr.Key())
		if bytes.Equal(uk, key) && tid > txnID {
			err = fmt.Errorf("conflicting write")
			log.WithFields(log.Fields{"id": m.id, "txnID": txnID}).Error(fmt.Sprintf("mvcc::mvcc::checkForConflictingWrites; found conflicting set in a higher txn number: %d", tid))
			return true, err
		}
	}

	return true, nil
}

// commitTxn commits a txn with the given id
// aquires an exclusive lock on mvcc.
func (m *MVCC) commitTxn(id uint64) (isLeader bool, err error) {
	log.WithFields(log.Fields{"id": m.id, "txnId": id}).Info("mvcc::mvcc::commitTxn; start")

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.activeTxn[id]; ok {
		// delete the key activeTxn from storage.
		markKey := getKey(id, activeTxn, nil)
		isLeader, err = m.rs.DeleteValue([]byte(markKey), true)
		if err != nil {
			log.WithFields(log.Fields{"id": m.id, "txnId": id}).Error("mvcc::mvcc::commitTxn; error in deleting mark key for txn.")
			return true, err
		}
		if !isLeader {
			return false, nil
		}

		delete(m.activeTxn, id)
	} else {
		log.WithFields(log.Fields{"id": m.id, "txnId": id}).Info("mvcc::mvcc::commitTxn; commit on inactive txn.")
		return true, fmt.Errorf("commit on inactive txn")
	}

	log.WithFields(log.Fields{"id": m.id, "txnId": id}).Info("mvcc::mvcc::commitTxn; committed successfully.")
	return true, nil
}

// rollbackTxn rolls back a txn with the given id
// acquires an exclusive lock on mvcc
func (m *MVCC) rollbackTxn(txnID uint64) (isLeader bool, err error) {
	log.WithFields(log.Fields{"id": m.id, "txnId": txnID}).Info("mvcc::mvcc::rollbackTxn; start")

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.activeTxn[txnID]; ok {
		// delete the entries written by this txn.
		// we can with key = "id" + nil. since nil is the smallest,
		// all the keys with prefix "id" can be scanned with the iterator.
		// we stop when the value of id doesn't match.
		itr, isLeader, err := m.rs.MetaScan(getKey(txnID, txnWrite, []byte(nil)))
		if !isLeader {
			return false, nil
		}

		if err != nil {
			log.WithFields(log.Fields{"id": m.id, "txnId": txnID}).Error("mvcc::mvcc::rollbackTxn; error in scan for written keys")
			return true, err
		}
		var toDelete [][]byte

		for {
			if itr.Valid() {
				tid, uk := getTxnWrite(itr.Key())
				if tid != txnID {
					break
				}

				// delete in the main storage.
				_, err := m.rs.DeleteValue(newTxnKey(uk, txnID), false)
				if err != nil {
					log.WithFields(log.Fields{"id": m.id, "txnId": txnID}).Error("mvcc::mvcc::rollbackTxn; error while deleting the written value")
					return true, err
				}

				toDelete = append(toDelete, uk)
				itr.Next()
			} else {
				break
			}
		}

		// delete the keys from meta storage
		for _, k := range toDelete {
			_, err = m.rs.DeleteValue(getKey(txnID, txnWrite, k), true)
			if err != nil {
				log.WithFields(log.Fields{"id": m.id, "txnId": txnID}).Error("mvcc::mvcc::rollbackTxn; error while deleting the written key in meta")
				return true, err
			}
		}

		// delete the key activeTxn from storage.
		markKey := getKey(txnID, activeTxn, nil)
		isLeader, err = m.rs.DeleteValue([]byte(markKey), true)
		if !isLeader {
			return false, nil
		}
		if err != nil {
			log.WithFields(log.Fields{"id": m.id, "txnId": txnID}).Info("mvcc::mvcc::rollbackTxn; error in deleting mark key for txn.")
			return true, err
		}

		delete(m.activeTxn, txnID)
	} else {
		log.WithFields(log.Fields{"id": m.id, "txnId": txnID}).Info("mvcc::mvcc::rollbackTxn; rollback on inactive txn.")
		return true, fmt.Errorf("rollback on inactive txn")
	}

	log.WithFields(log.Fields{"id": m.id, "txnId": txnID}).Info("mvcc::mvcc::rollbackTxn; rolled back successfully.")
	return true, nil
}

// getNextTxnID returns the next available txn id.
// IMP: Hold mu before calling this. Also update the nxt id after wards
func (m *MVCC) getNextTxnID() (nxtIDUint64 uint64, isLeader bool, err error) {
	log.WithFields(log.Fields{"id": m.id}).Info("mvcc::mvcc::getNextTxnID; start")

	txnKey := []byte(getKey(notUsed, nxtTxnID, nil))
	nxtID, isLeader, err := m.rs.MetaGetValue(txnKey)
	if !isLeader {
		return 0, false, nil
	}
	if err != nil {
		if _, ok := err.(icommon.NotFoundError); ok {
			log.WithFields(log.Fields{"id": m.id}).Error("mvcc::mvcc::getNextTxnID; txnKey not found. choosing default as 1")
			nxtIDUint64 = 1
		} else {
			log.WithFields(log.Fields{"id": m.id}).Error(fmt.Sprintf("mvcc::mvcc::getNextTxnID; error in getting txnKey. err: %v", err.Error()))
			return 0, true, err
		}
	} else {
		nxtIDUint64 = common.ByteSliceToU64(nxtID)
	}

	log.WithFields(log.Fields{"id": m.id}).Info("mvcc::mvcc::getNextTxnID; done")
	return nxtIDUint64, true, nil
}

func (m *MVCC) init() {
	// todo: abort active txns after crash.
}

// NewMVCC creates a new MVCC transactional layer for the storage
func NewMVCC(id uint64, rs raft.IcecaneRaftServer) *MVCC {
	m := &MVCC{
		id:        id,
		mu:        new(sync.RWMutex),
		activeTxn: make(map[uint64]*Transaction),
		rs:        rs,
	}

	m.init()

	return m
}
