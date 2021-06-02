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
	"github.com/dr0pdb/icecanedb/pkg/storage"
	log "github.com/sirupsen/logrus"
)

/*
	Every request is part of a txn. If the request body contains a txn id,
	then the request is considered to be part of that txn.
	Otherwise, we create a new txn for the single request.

	This is important for the ACID guarantees.

	Each Key is appended by the txn id that created/modified it.

	MVCC uses TxnKeyComparator defined in keys.go to order the keys written using MVCC in the kv storage.
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

/*
	TODO: In many places in this file, we try to rollback the txn in case of a failure.
	In case, the rollback also fails, then at the moment we just ignore it
	It might make sense to crash the node in that case to avoid inconsistencies
*/

// MVCC is the Multi Version Concurrency Control layer for transactions.
// Operations on it are thread safe using a RWMutex
type MVCC struct {
	id uint64

	mu *sync.RWMutex

	// cache of active transactions
	activeTxnsCache map[uint64]*Transaction

	// the underlying raft node
	rs raft.IcecaneRaftServer
}

//
// Public methods called from the grpc server
//

// BeginTxn begins a MVCC transaction providing ACID guarantees.
func (m *MVCC) BeginTxn(ctx context.Context, req *pb.BeginTxnRequest) (*pb.BeginTxnResponse, error) {
	log.WithFields(log.Fields{"id": m.id}).Info("mvcc::mvcc::BeginTxn; started")

	txn, err := m.begin(req.Mode)
	resp := &pb.BeginTxnResponse{}

	if err != nil {
		return resp, err
	}

	resp.TxnId = txn.id
	resp.Success = true

	log.WithFields(log.Fields{"id": m.id, "createdTxnId": resp.TxnId}).Info("mvcc::mvcc::BeginTxn; done")
	return resp, nil
}

// Get gets the value of a key.
func (m *MVCC) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Info("mvcc::mvcc::Get; started")

	resp := &pb.GetResponse{}
	inlinedTxn := (req.TxnId == 0)

	txnID, err := m.ensureTxn(req.TxnId, pb.TxnMode_ReadOnly)
	if err != nil {
		return nil, err
	}
	req.TxnId = txnID

	tk := newTxnKey(req.GetKey(), txnID)
	itr, err := m.rs.ScanValues(tk)
	if err != nil {
		log.WithFields(log.Fields{"txnID": txnID}).Error(fmt.Sprintf("mvcc::mvcc::Get; error in scan: %v", err.Error()))
		return nil, err
	}

	k, v, found, err := m.getValueForTxn(itr, req.GetKey(), txnID)
	if err != nil {
		log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId, "err": err.Error()}).Error("mvcc::mvcc::Get; error while getting value")
		return nil, err
	}

	resp.Found = found
	if resp.Found {
		resp.Kv = &pb.KeyValuePair{
			Key:   k,
			Value: v,
		}

		log.WithFields(log.Fields{"txnID": req.TxnId, "value": string(resp.Kv.Value)}).Info("mvcc::mvcc::Get; found value")
	}

	if inlinedTxn {
		log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Info("mvcc::mvcc::Get; inlined txn. committing..")
		err = m.commitTxn(req.TxnId)
		if err != nil {
			resp.Error = &pb.IcecaneError{
				Retryable: false,
			}
		}
	}

	return resp, nil
}

// Scan returns the list of kv pairs >= startKey for the given transaction
func (m *MVCC) Scan(ctx context.Context, req *pb.ScanRequest) (*pb.ScanResponse, error) {
	log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Info("mvcc::mvcc::Scan; started")

	resp := &pb.ScanResponse{}
	inlinedTxn := (req.TxnId == 0)

	txnID, err := m.ensureTxn(req.TxnId, pb.TxnMode_ReadOnly)
	if err != nil {
		return nil, err
	}
	req.TxnId = txnID
	count := int32(0)

	tk := newTxnKey(req.StartKey, txnID)
	itr, err := m.rs.ScanValues(tk)
	if err != nil {
		log.WithFields(log.Fields{"id": m.id, "txnID": txnID}).Error(fmt.Sprintf("mvcc::mvcc::Scan; error in scan: %v", err.Error()))
		return nil, err
	}

	prevUk := []byte("")
	results := make([]*pb.KeyValuePair, 0)

	for {
		if count >= req.MaxReadings || !itr.Valid() {
			break
		}

		tk := TxnKey(itr.Key())
		k, v, found, err := m.getValueForTxn(itr, tk.userKey(), txnID)
		if err != nil {
			return nil, err
		}

		if found {
			results = append(results, &pb.KeyValuePair{Key: k, Value: v})
			prevUk = k
			count++
		}

		for {
			if !itr.Valid() || !bytes.Equal(TxnKey(itr.Key()).userKey(), prevUk) {
				break
			}

			itr.Next()
		}
	}

	if inlinedTxn {
		log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Info("mvcc::mvcc::Get; inlined txn. committing..")
		m.commitTxn(req.TxnId)
	}

	resp.Entries = results
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

	resp := &pb.SetResponse{}
	inlinedTxn := (req.TxnId == 0)

	txnID, err := m.ensureTxn(req.TxnId, pb.TxnMode_ReadWrite)
	if err != nil {
		log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Error("mvcc::mvcc::Set; unable to ensure txn")
		return resp, err
	}
	req.TxnId = txnID

	txn := m.activeTxnsCache[txnID]
	log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Info("mvcc::mvcc::Set; checking for conflicting sets")

	err = m.checkForConflictingWrites(txn, txnID, req.GetKey())
	if err != nil {
		resp.Error = &pb.IcecaneError{
			Retryable: true,
			Message:   "serialization error",
		}
		log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Warn("mvcc::mvcc::Set; found conflicting set")
		return resp, nil
	}

	log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Info("mvcc::mvcc::Set; no conflicts. writing now")
	m.mu.Lock()

	// no conflicts. do the write now
	tKey := newTxnKey(req.GetKey(), req.TxnId)
	tVal := newSetTxnValue(req.GetValue())
	err = m.rs.SetValue(tKey, tVal, false)
	if err != nil {
		log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId, "err": err.Error()}).Error("mvcc::mvcc::Set; error while setting value")
		m.mu.Unlock()
		return resp, err
	}
	upKey := getKey(req.TxnId, txnWrite, req.GetKey())
	err = m.rs.SetValue(upKey, []byte("true"), true)

	m.mu.Unlock()

	if inlinedTxn && err == nil {
		log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Info("mvcc::mvcc::Set; inlined txn. committing..")
		err = m.commitTxn(req.TxnId)
	}
	// in case of a failure rollback the inline txn
	if inlinedTxn && err != nil {
		log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Info("mvcc::mvcc::Set; error in inline txn, rolling it back")
		m.rollbackTxn(req.TxnId)
		return resp, err
	}

	resp.Success = true
	log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Info("mvcc::mvcc::Set; done")
	return resp, err
}

// Delete deletes the value of a key.
// TODO: remove excessive code duplication b/w set and delete.
func (m *MVCC) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	log.WithFields(log.Fields{"peeId": m.id, "txnId": req.TxnId}).Info("mvcc::mvcc::Delete; started")

	resp := &pb.DeleteResponse{}
	inlinedTxn := (req.TxnId == 0)

	txnID, err := m.ensureTxn(req.TxnId, pb.TxnMode_ReadWrite)
	if err != nil {
		return nil, err
	}
	req.TxnId = txnID
	txn := m.activeTxnsCache[txnID]

	log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Info("mvcc::mvcc::Delete; checking for conflicting writes")

	err = m.checkForConflictingWrites(txn, txnID, req.GetKey())
	if err != nil {
		resp.Error = &pb.IcecaneError{
			Retryable: true,
			Message:   "serialization error",
		}
		log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Warn("mvcc::mvcc::Delete; found conflicting write")
		return resp, nil
	}

	log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Info("mvcc::mvcc::Delete; no conflicts. writing now")
	m.mu.Lock()

	// no conflicts. do the write now
	tKey := newTxnKey(req.GetKey(), req.TxnId)
	tVal := newDeleteTxnValue()
	err = m.rs.SetValue(tKey, tVal, false)
	if err != nil {
		log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId, "err": err.Error()}).Error("mvcc::mvcc::Delete; error while setting delete value")
		m.mu.Unlock()
		return resp, err
	}
	upKey := getKey(req.TxnId, txnWrite, req.GetKey())
	err = m.rs.SetValue(upKey, []byte("true"), true)

	m.mu.Unlock()

	if inlinedTxn && err == nil {
		log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Info("mvcc::mvcc::Delete; inlined txn. committing..")
		err = m.commitTxn(req.TxnId)
	}
	// in case of a failure rollback the inline txn
	if inlinedTxn && err != nil {
		log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Info("mvcc::mvcc::Delete; error in inline txn, rolling it back")
		m.rollbackTxn(req.TxnId)
		return resp, err
	}

	resp.Success = true
	return resp, err
}

// CommitTxn attempts to commit a MVCC txn
func (m *MVCC) CommitTxn(ctx context.Context, req *pb.CommitTxnRequest) (*pb.CommitTxnResponse, error) {
	log.WithFields(log.Fields{"peeId": m.id, "txnId": req.TxnId}).Info("mvcc::mvcc::CommitTxn; started")

	resp := &pb.CommitTxnResponse{}
	err := m.commitTxn(req.TxnId)

	if err != nil {
		log.WithFields(log.Fields{"peeId": m.id, "txnId": req.TxnId}).Info("mvcc::mvcc::CommitTxn; error in committing txn")
		resp.Error = &pb.IcecaneError{
			Retryable: false,
			Message:   err.Error(),
		}
		return resp, err
	}
	resp.Success = true
	log.WithFields(log.Fields{"peeId": m.id, "txnId": req.TxnId}).Info("mvcc::mvcc::CommitTxn; ending successfully")
	return resp, nil
}

// RollbackTxn rollsback a MVCC txn
func (m *MVCC) RollbackTxn(ctx context.Context, req *pb.RollbackTxnRequest) (*pb.RollbackTxnResponse, error) {
	log.WithFields(log.Fields{"peeId": m.id, "txnId": req.TxnId}).Info("mvcc::mvcc::RollbackTxn; started")

	resp := &pb.RollbackTxnResponse{}

	err := m.rollbackTxn(req.TxnId)
	if err != nil {
		log.WithFields(log.Fields{"peeId": m.id, "txnId": req.TxnId}).Error("mvcc::mvcc::RollbackTxn; error in rolling back txn")
		resp.Error = &pb.IcecaneError{
			Retryable: false,
			Message:   err.Error(),
		}
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
func (m *MVCC) begin(mode pb.TxnMode) (*Transaction, error) {
	log.Info("mvcc::mvcc::begin; started")

	m.mu.Lock()
	defer m.mu.Unlock()

	// get next txn id
	nxtIDUint64, err := m.getNextTxnID()
	if err != nil {
		log.Error(fmt.Sprintf("mvcc::mvcc::begin; error in getting next txnKey. err: %v", err.Error()))
		return nil, err
	}

	// snapshot of active txns at the moment of creation.
	cTxns := make(map[uint64]bool)
	var cTxnsS []uint64
	for k := range m.activeTxnsCache {
		cTxns[k] = true
		cTxnsS = append(cTxnsS, k)
	}

	// persist snapshot and mark txn as active.
	snapkey := getKey(nxtIDUint64, txnSnapshot, nil)
	err = m.rs.SetValue([]byte(snapkey), common.U64SliceToByteSlice(cTxnsS), true)
	if err != nil {
		log.WithFields(log.Fields{"id": nxtIDUint64}).Error(fmt.Sprintf("mvcc::mvcc::begin; error in setting snapshot. err: %v", err.Error()))
		return nil, err
	}
	markKey := getKey(nxtIDUint64, activeTxn, nil)
	err = m.rs.SetValue([]byte(markKey), []byte("true"), true)
	if err != nil {
		log.WithFields(log.Fields{"id": nxtIDUint64}).Error(fmt.Sprintf("mvcc::mvcc::begin; error in setting mark key. err: %v", err.Error()))
		return nil, err
	}

	txn := newTransaction(nxtIDUint64, cTxns, mode)
	m.activeTxnsCache[txn.id] = txn

	log.Info("mvcc::mvcc::begin; done")
	return txn, nil
}

func (m *MVCC) getValueForTxn(itr storage.Iterator, key []byte, txnID uint64) (k, v []byte, found bool, err error) {
	txn := m.activeTxnsCache[txnID]

	// iterate through the kv pairs sorted (decreasing) by txnID.
	// skip the ones which were active during the txn init.
	for {
		if itr.Valid() {
			log.WithFields(log.Fields{"txnID": txnID}).Info("mvcc::mvcc::getValueForTxn; valid node on the iterator")
			tk := TxnKey(itr.Key())
			uk := tk.userKey()
			if !bytes.Equal(uk, key) {
				break
			}

			tid := tk.txnID()
			if _, ok := txn.concTxns[tid]; !ok {
				log.WithFields(log.Fields{"txnID": txnID, "tid": tid}).Info("mvcc::mvcc::getValueForTxn; found a value for the key")

				if len(itr.Value()) > 0 {
					tVal := txnValue(itr.Value())
					if !tVal.getDeleteFlag() {
						return uk, tVal.getUserValue(), true, nil
					}
				}

				return uk, nil, false, nil
			} else {
				log.WithFields(log.Fields{"txnID": txnID, "tid": tid}).Info("mvcc::mvcc::getValueForTxn; found a conflicting txn; skipping")
				itr.Next()
			}
		} else {
			break
		}
	}

	return nil, nil, false, nil
}

// ensureTxn ensures a txn exists.
// if id = 0, it creates a new txn with the given mode.
// if id != 0, it ensures that the txn is active.
// IMP: Don't hold locks while calling this function.
func (m *MVCC) ensureTxn(id uint64, mode pb.TxnMode) (uint64, error) {
	log.WithFields(log.Fields{"id": m.id, "txnId": id}).Info("mvcc::mvcc::ensureTxn; starting")
	if id == 0 {
		txnID, err := m.begin(mode)
		if err != nil {
			return 0, err
		}

		id = txnID.id
	} else {
		txn, err := m.getTxn(id)
		if err != nil {
			return 0, err
		}

		id = txn.id
	}

	log.Info("mvcc::mvcc::ensureTxn; done")
	return id, nil
}

// checkForConflictingWrites checks if there is a concurrent write that conflicts with the write being attempted.
// IMP: Holds lock while execution. Don't hold lock while calling this function.
func (m *MVCC) checkForConflictingWrites(txn *Transaction, txnID uint64, key []byte) error {
	log.WithFields(log.Fields{"id": m.id, "txnID": txnID}).Info("mvcc::mvcc::checkForConflictingWrites; started")

	m.mu.Lock()
	defer m.mu.Unlock()

	// Iterate over the snapshot of concurrent txns to see if
	// one of those txns have written a value for the key which we're trying to write.
	for ctxn := range txn.concTxns {
		upkey := getKey(ctxn, txnWrite, key)

		// we don't care about the value. We need to check for existence.
		_, err := m.rs.MetaGetValue(upkey)
		if err == nil {
			err = fmt.Errorf("conflicting write")
			log.WithFields(log.Fields{"id": m.id, "txnID": txnID}).Error("mvcc::mvcc::checkForConflictingWrites; found conflicting set")
			return err
		}
	}

	// Iterate over any writes with the same key which were done by txn with higher txn id.
	// These could not have been included in the snapshot hence a separate check
	// since for a single user key, the keys are ordered in decreasing order to txn id, we start from max possible txn id
	itr, _ := m.rs.MetaScan(getKey(math.MaxUint64, txnWrite, key))
	if itr.Valid() {
		tid, uk := getTxnWrite(itr.Key())
		if bytes.Equal(uk, key) && tid > txnID {
			err := fmt.Errorf("conflicting write")
			log.WithFields(log.Fields{"id": m.id, "txnID": txnID}).Error(fmt.Sprintf("mvcc::mvcc::checkForConflictingWrites; found conflicting set in a higher txn number: %d", tid))
			return err
		}
	}

	return nil
}

// commitTxn commits a txn with the given id
// aquires an exclusive lock on mvcc.
func (m *MVCC) commitTxn(id uint64) (err error) {
	log.WithFields(log.Fields{"id": m.id, "txnId": id}).Info("mvcc::mvcc::commitTxn; start")

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.activeTxnsCache[id]; ok {
		// delete the key activeTxn from storage.
		markKey := getKey(id, activeTxn, nil)
		err = m.rs.DeleteValue([]byte(markKey), true)
		if err != nil {
			log.WithFields(log.Fields{"id": m.id, "txnId": id}).Error("mvcc::mvcc::commitTxn; error in deleting mark key for txn.")
			return err
		}

		delete(m.activeTxnsCache, id)
	} else {
		log.WithFields(log.Fields{"id": m.id, "txnId": id}).Info("mvcc::mvcc::commitTxn; commit on inactive txn.")
		return fmt.Errorf("commit on inactive txn")
	}

	log.WithFields(log.Fields{"id": m.id, "txnId": id}).Info("mvcc::mvcc::commitTxn; committed successfully.")
	return nil
}

// rollbackTxn rolls back a txn with the given id
// NOTE: acquires an exclusive lock on mvcc
func (m *MVCC) rollbackTxn(txnID uint64) (err error) {
	log.WithFields(log.Fields{"id": m.id, "txnId": txnID}).Info("mvcc::mvcc::rollbackTxn; start")

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.activeTxnsCache[txnID]; ok {
		// delete the entries written by this txn.
		// we can with key = "id" + nil. since nil is the smallest,
		// all the keys with prefix "id" can be scanned with the iterator.
		// we stop when the value of id doesn't match.
		itr, err := m.rs.MetaScan(getKey(txnID, txnWrite, []byte(nil)))

		if err != nil {
			log.WithFields(log.Fields{"id": m.id, "txnId": txnID}).Error("mvcc::mvcc::rollbackTxn; error in scan for written keys")
			return err
		}
		var toDelete [][]byte

		for {
			if itr.Valid() {
				tid, uk := getTxnWrite(itr.Key())
				if tid != txnID {
					break
				}

				// delete in the main storage.
				err := m.rs.DeleteValue(newTxnKey(uk, txnID), false)
				if err != nil {
					log.WithFields(log.Fields{"id": m.id, "txnId": txnID}).Error("mvcc::mvcc::rollbackTxn; error while deleting the written value")
					return err
				}

				toDelete = append(toDelete, uk)
				itr.Next()
			} else {
				break
			}
		}

		// delete the keys from meta storage
		for _, k := range toDelete {
			err = m.rs.DeleteValue(getKey(txnID, txnWrite, k), true)
			if err != nil {
				log.WithFields(log.Fields{"id": m.id, "txnId": txnID}).Error("mvcc::mvcc::rollbackTxn; error while deleting the written key in meta")
				return err
			}
		}

		// delete the key activeTxn from storage.
		markKey := getKey(txnID, activeTxn, nil)
		err = m.rs.DeleteValue([]byte(markKey), true)
		if err != nil {
			log.WithFields(log.Fields{"id": m.id, "txnId": txnID}).Info("mvcc::mvcc::rollbackTxn; error in deleting mark key for txn.")
			return err
		}

		delete(m.activeTxnsCache, txnID)
	} else {
		log.WithFields(log.Fields{"id": m.id, "txnId": txnID}).Info("mvcc::mvcc::rollbackTxn; rollback on inactive txn.")
		return fmt.Errorf("rollback on inactive txn")
	}

	log.WithFields(log.Fields{"id": m.id, "txnId": txnID}).Info("mvcc::mvcc::rollbackTxn; rolled back successfully.")
	return nil
}

// getTxn returns the txn with the given id if it's active
// Note: Don't hold lock while calling this
func (m *MVCC) getTxn(txnID uint64) (*Transaction, error) {
	log.WithFields(log.Fields{"id": m.id}).Info("mvcc::mvcc::getTxn; start")
	m.mu.RLock()
	if txn, ok := m.activeTxnsCache[txnID]; ok {
		m.mu.RUnlock()
		return txn, nil
	}
	m.mu.RUnlock()

	// todo: fetch from disk

	return nil, nil
}

// getNextTxnID returns the next available txn id. It also updates the nxt id in storage
// IMP: Hold mu before calling this.
func (m *MVCC) getNextTxnID() (nxtIDUint64 uint64, err error) {
	log.WithFields(log.Fields{"id": m.id}).Info("mvcc::mvcc::getNextTxnID; start")

	txnKey := []byte(getKey(notUsed, nxtTxnID, nil))
	nxtID, err := m.rs.MetaGetValue(txnKey)
	if err != nil {
		if _, ok := err.(icommon.NotFoundError); ok {
			log.WithFields(log.Fields{"id": m.id}).Error("mvcc::mvcc::getNextTxnID; txnKey not found. choosing default as 1")
			nxtIDUint64 = 1
		} else {
			log.WithFields(log.Fields{"id": m.id}).Error(fmt.Sprintf("mvcc::mvcc::getNextTxnID; error in getting txnKey. err: %v", err.Error()))
			return 0, err
		}
	} else {
		nxtIDUint64 = common.ByteSliceToU64(nxtID)
	}

	// set next txn id
	txnKey = []byte(getKey(notUsed, nxtTxnID, nil))
	err = m.rs.SetValue(txnKey, common.U64ToByteSlice(nxtIDUint64+1), true)
	if err != nil {
		log.Error(fmt.Sprintf("mvcc::mvcc::getNextTxnID; error in setting next txnKey. err: %v", err.Error()))
		return 0, err
	}

	log.WithFields(log.Fields{"id": m.id}).Info("mvcc::mvcc::getNextTxnID; done")
	return nxtIDUint64, nil
}

func (m *MVCC) init() {
	// todo: abort active txns created by this node after crash.
	// this also means that we need to store which node created a txn
}

// NewMVCC creates a new MVCC transactional layer for the storage
func NewMVCC(id uint64, rs raft.IcecaneRaftServer) *MVCC {
	m := &MVCC{
		id:              id,
		mu:              new(sync.RWMutex),
		activeTxnsCache: make(map[uint64]*Transaction),
		rs:              rs,
	}

	m.init()

	return m
}
