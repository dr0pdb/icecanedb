package mvcc

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	icommon "github.com/dr0pdb/icecanedb/internal/common"
	"github.com/dr0pdb/icecanedb/pkg/common"
	pb "github.com/dr0pdb/icecanedb/pkg/protogen"
	"github.com/dr0pdb/icecanedb/pkg/raft"
	log "github.com/sirupsen/logrus"
)

/*
	Every request is part of a txn. If the request body contains a txn id,
	then the request is considered to be part of that txn.
	Otherwise, we create a new txn for the single request.

	This is important for the ACID guarantees.

	Each Key is appended by the txn id that created/modified it.
*/

// MVCC is the Multi Version Concurrency Control layer for transactions.
// Operations on it are thread safe using a RWMutex
type MVCC struct {
	id uint64

	mu *sync.RWMutex

	// active transactions at the present moment.
	activeTxn map[uint64]*Transaction

	// the underlying raft node.
	rs *raft.Server
}

//
// Public methods called from the grpc server
//

// BeginTxn begins a MVCC transaction providing ACID guarantees.
func (m *MVCC) BeginTxn(ctx context.Context, req *pb.BeginTxnRequest) (*pb.BeginTxnResponse, error) {
	log.Info("mvcc::mvcc::BeginTxn; started")

	txn, leaderID, err := m.begin(req.Mode)
	resp := &pb.BeginTxnResponse{
		LeaderId: leaderID,
	}

	if err != nil {
		return resp, err
	}

	resp.TxnId = txn.id
	resp.Success = true

	log.Info("mvcc::mvcc::BeginTxn; done")
	return resp, nil
}

// Get gets the value of a key.
func (m *MVCC) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	log.Info("mvcc::mvcc::Get; started")

	resp := &pb.GetResponse{}
	inlinedTxn := (req.TxnId == 0)

	txnID, err := m.ensureTxn(req.TxnId, pb.TxnMode_ReadOnly)
	if err != nil {
		return nil, err
	}
	req.TxnId = txnID

	txn := m.activeTxn[txnID]

	tk := newTxnKey(req.GetKey(), req.GetTxnId())
	itr, leaderID, err := m.rs.Scan(tk)

	resp.LeaderId = leaderID
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}

	// iterate through the kv pairs sorted (decreasing) by txnID.
	// skip the ones which were active during the txn init.
	for {
		if itr.Valid() {
			tk := TxnKey(itr.Key())
			uk := tk.userKey()
			if bytes.Compare(uk, req.GetKey()) != 0 {
				break
			}

			tid := tk.txnID()
			if _, ok := txn.concTxns[tid]; !ok {
				log.WithFields(log.Fields{"txnID": req.TxnId, "tid": tid}).Info("mvcc::mvcc::Get; found a value for the key")
				resp.Value = itr.Value()
				resp.Found = true
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
		_, err = m.commitTxn(req.TxnId)
	}

	return resp, err
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

	txnID, err := m.ensureTxn(req.TxnId, pb.TxnMode_ReadWrite)
	if err != nil {
		log.WithFields(log.Fields{"id": m.id, "txnID": req.TxnId}).Error("mvcc::mvcc::Set; unable to ensure txn")
		return resp, err
	}
	req.TxnId = txnID

	m.mu.Lock()
	defer m.mu.Unlock()

	txn := m.activeTxn[txnID]

	// Iterate over the snapshot of concurrent txns to see if
	// one of those txns have written a value for the key which we're trying to write.
	for ctxn := range txn.concTxns {
		upkey := getKey(ctxn, txnWrite, req.GetKey())

		// we don't care about the value. We need to check for existence.
		_, leaderID, err := m.rs.MetaGetValue(upkey)
		if leaderID != m.id {
			resp.LeaderId = leaderID
			return resp, nil
		}
		if err != nil {
			_, ok := err.(icommon.NotFoundError)

			if ok {
				continue
			} else {
				resp.Error = "Serialization Error"
				return resp, err
			}
		}

	}

	// no conflicts. do the write now
	tKey := newTxnKey(req.GetKey(), req.TxnId)
	_, err = m.rs.SetValue(tKey, req.GetValue())
	if err != nil {
		// log it
		return resp, err
	}
	upKey := getKey(req.TxnId, txnWrite, req.GetKey())
	_, err = m.rs.MetaSetValue(upKey, []byte("true"))
	if err != nil {
		// TODO: remove the previous write.
		return resp, err
	}

	resp.Success = true
	return resp, err
}

// Delete deletes the value of a key.
// TODO: remove excessive code duplication b/w set and delete.
func (m *MVCC) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	log.WithFields(log.Fields{"peeId": m.id, "txnId": req.TxnId}).Info("mvcc::mvcc::Delete; started")

	resp := &pb.DeleteResponse{}

	txnID, err := m.ensureTxn(req.TxnId, pb.TxnMode_ReadWrite)
	if err != nil {
		return nil, err
	}
	req.TxnId = txnID

	m.mu.Lock()
	defer m.mu.Unlock()

	txn := m.activeTxn[txnID]

	// Iterate over the snapshot of concurrent txns to see if
	// one of those txns have written a value for the key which we're trying to write.
	for ctxn := range txn.concTxns {
		upkey := getKey(ctxn, txnWrite, req.GetKey())

		// we don't care about the value. We need to check for existence.
		_, leaderID, err := m.rs.MetaGetValue(upkey)
		if leaderID != m.id {
			resp.LeaderId = leaderID
			return resp, nil
		}
		if err != nil {
			_, ok := err.(icommon.NotFoundError)

			if ok {
				continue
			} else {
				resp.Error = "Serialization Error"
				return resp, err
			}
		}

	}

	// no conflicts. do the write now
	tKey := newTxnKey(req.GetKey(), req.TxnId)
	_, err = m.rs.DeleteValue(tKey)
	if err != nil {
		// log it
		return resp, err
	}
	upKey := getKey(req.TxnId, txnWrite, req.GetKey())
	_, err = m.rs.MetaSetValue(upKey, []byte("true"))
	if err != nil {
		// TODO: remove the previous write.
		return resp, err
	}

	resp.Success = true
	return resp, err
}

// CommitTxn attempts to commit a MVCC txn
func (m *MVCC) CommitTxn(ctx context.Context, req *pb.CommitTxnRequest) (*pb.CommitTxnResponse, error) {
	log.WithFields(log.Fields{"peeId": m.id, "txnId": req.TxnId}).Info("mvcc::mvcc::CommitTxn; started")

	resp := &pb.CommitTxnResponse{}
	leaderID, err := m.commitTxn(req.TxnId)
	if err != nil {
		log.WithFields(log.Fields{"peeId": m.id, "txnId": req.TxnId}).Info("mvcc::mvcc::CommitTxn; error in committing txn")
		resp.Error = err.Error()
		return resp, err
	}
	resp.LeaderId = leaderID
	resp.Success = true
	log.WithFields(log.Fields{"peeId": m.id, "txnId": req.TxnId}).Info("mvcc::mvcc::CommitTxn; ending successfully")
	return resp, nil
}

// RollbackTxn rollsback a MVCC txn
func (m *MVCC) RollbackTxn(ctx context.Context, req *pb.RollbackTxnRequest) (*pb.RollbackTxnResponse, error) {
	log.WithFields(log.Fields{"peeId": m.id, "txnId": req.TxnId}).Info("mvcc::mvcc::RollbackTxn; started")

	resp := &pb.RollbackTxnResponse{}
	leaderID, err := m.rollbackTxn(req.TxnId)
	if err != nil {
		log.WithFields(log.Fields{"peeId": m.id, "txnId": req.TxnId}).Error("mvcc::mvcc::RollbackTxn; error in rolling back txn")
		resp.Error = err.Error()
		return resp, err
	}
	resp.LeaderId = leaderID
	resp.Success = true
	log.WithFields(log.Fields{"peeId": m.id, "txnId": req.TxnId}).Info("mvcc::mvcc::RollbackTxn; ending successfully")
	return resp, nil
}

//
// Internal methods
//

// begin begins a new Transaction
func (m *MVCC) begin(mode pb.TxnMode) (*Transaction, uint64, error) {
	log.Info("mvcc::mvcc::begin; started")

	m.mu.Lock()
	defer m.mu.Unlock()

	// get next txn id
	nxtIDUint64, leaderID, err := m.getNextTxnID()
	if err != nil {
		log.Error(fmt.Sprintf("mvcc::mvcc::begin; error in getting next txnKey. err: %v", err.Error()))
		return nil, leaderID, err
	}

	// set next txn id
	txnKey := []byte(getKey(notUsed, nxtTxnID, nil))
	_, err = m.rs.SetValue(txnKey, common.U64ToByte(nxtIDUint64+1))
	if err != nil {
		log.Error(fmt.Sprintf("mvcc::mvcc::begin; error in setting next txnKey. err: %v", err.Error()))
		return nil, leaderID, err
	}

	// snapshot of active txns at the moment of creation.
	var cTxns map[uint64]bool
	var cTxnsS []uint64
	for k := range m.activeTxn {
		cTxns[k] = true
		cTxnsS = append(cTxnsS, k)
	}

	// persist snapshot and mark txn as active.
	snapkey := getKey(nxtIDUint64, txnSnapshot, nil)
	_, err = m.rs.MetaSetValue([]byte(snapkey), common.U64SliceToByteSlice(cTxnsS))
	if err != nil {
		log.WithFields(log.Fields{"id": nxtIDUint64}).Error(fmt.Sprintf("mvcc::mvcc::begin; error in setting snapshot. err: %v", err.Error()))
		return nil, leaderID, err
	}
	markKey := getKey(nxtIDUint64, activeTxn, nil)
	_, err = m.rs.MetaSetValue([]byte(markKey), []byte("true"))
	if err != nil {
		log.WithFields(log.Fields{"id": nxtIDUint64}).Error(fmt.Sprintf("mvcc::mvcc::begin; error in setting mark key. err: %v", err.Error()))
		return nil, leaderID, err
	}

	txn := newTransaction(nxtIDUint64, cTxns, mode)
	m.activeTxn[txn.id] = txn

	log.Info("mvcc::mvcc::begin; done")
	return txn, leaderID, nil
}

// ensureTxn ensures a txn exists.
// if id = 0, it creates a new txn with the given mode.
// if id != 0, it ensures that the txn is active.
// IMP: Don't hold locks while calling this function.
func (m *MVCC) ensureTxn(id uint64, mode pb.TxnMode) (uint64, error) {
	log.WithFields(log.Fields{"id": id}).Info("mvcc::mvcc::ensureTxn; starting")
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

// commitTxn commits a txn with the given id
// aquires an exclusive lock on mvcc.
func (m *MVCC) commitTxn(id uint64) (leaderID uint64, err error) {
	log.WithFields(log.Fields{"id": id}).Info("mvcc::mvcc::commitTxn; start")

	m.mu.Lock()
	defer m.mu.Unlock()

	// default to this node
	leaderID = m.id

	if _, ok := m.activeTxn[id]; ok {
		// delete the key activeTxn from storage.
		markKey := getKey(id, activeTxn, nil)
		leaderID, err = m.rs.MetaDeleteValue([]byte(markKey))
		if err != nil {
			log.WithFields(log.Fields{"id": id}).Error("mvcc::mvcc::commitTxn; error in deleting mark key for txn.")
			return leaderID, err
		}

		delete(m.activeTxn, id)
	} else {
		log.WithFields(log.Fields{"id": id}).Info("mvcc::mvcc::commitTxn; commit on inactive txn.")
		return leaderID, fmt.Errorf("commit on inactive txn")
	}

	log.WithFields(log.Fields{"id": id}).Info("mvcc::mvcc::commitTxn; committed successfully.")
	return leaderID, nil
}

// rollbackTxn rolls back a txn with the given id
// acquires an exclusive lock on mvcc
func (m *MVCC) rollbackTxn(id uint64) (leaderID uint64, err error) {
	log.WithFields(log.Fields{"id": id}).Info("mvcc::mvcc::rollbackTxn; start")

	m.mu.Lock()
	defer m.mu.Unlock()

	// default to this node
	leaderID = m.id

	if _, ok := m.activeTxn[id]; ok {
		// delete the entries written by this txn.
		// we can with key = "id" + nil. since nil is the smallest,
		// all the keys with prefix "id" can be scanned with the iterator.
		// we stop when the value of id doesn't match.
		itr, lid, err := m.rs.MetaScan(common.U64ToByte(id))
		leaderID = lid
		if err != nil {
			log.WithFields(log.Fields{"id": id}).Error("mvcc::mvcc::rollbackTxn; error in scan for written keys")
			return leaderID, err
		}
		var toDelete [][]byte

		for {
			if itr.Valid() {
				tid, uk := getTxnWrite(itr.Key())
				if tid != id {
					break
				}

				// delete in the main storage.
				_, err := m.rs.DeleteValue(newTxnKey(uk, id))
				if err != nil {
					log.WithFields(log.Fields{"id": id}).Error("mvcc::mvcc::rollbackTxn; error while deleting the written value")
					return leaderID, err
				}

				toDelete = append(toDelete, uk)
			} else {
				break
			}
		}

		// delete the keys from meta storage
		for _, k := range toDelete {
			_, err = m.rs.MetaDeleteValue(getKey(id, txnWrite, k))
			if err != nil {
				log.WithFields(log.Fields{"id": id}).Error("mvcc::mvcc::rollbackTxn; error while deleting the written key in meta")
				return leaderID, err
			}
		}

		// delete the key activeTxn from storage.
		markKey := getKey(id, activeTxn, nil)
		leaderID, err = m.rs.MetaDeleteValue([]byte(markKey))
		if err != nil {
			log.WithFields(log.Fields{"id": id}).Info("mvcc::mvcc::rollbackTxn; error in deleting mark key for txn.")
			return leaderID, err
		}

		delete(m.activeTxn, id)
	} else {
		log.WithFields(log.Fields{"id": id}).Info("mvcc::mvcc::rollbackTxn; rollback on inactive txn.")
		return leaderID, fmt.Errorf("rollback on inactive txn")
	}

	log.WithFields(log.Fields{"id": id}).Info("mvcc::mvcc::rollbackTxn; rolled back successfully.")
	return leaderID, nil
}

// getNextTxnID returns the next available txn id.
// IMP: Hold mu before calling this. Also update the nxt id after wards
func (m *MVCC) getNextTxnID() (nxtIDUint64, leaderID uint64, err error) {
	log.Info("mvcc::mvcc::getNextTxnID; start")

	txnKey := []byte(getKey(notUsed, nxtTxnID, nil))
	nxtID, leaderID, err := m.rs.MetaGetValue(txnKey)
	if err != nil {
		if _, ok := err.(icommon.NotFoundError); ok {
			log.Error(fmt.Sprintf("mvcc::mvcc::getNextTxnID; txnKey not found. choosing default as 1"))
			nxtIDUint64 = 1
		} else {
			log.Error(fmt.Sprintf("mvcc::mvcc::getNextTxnID; error in getting txnKey. err: %v", err.Error()))
			return 0, leaderID, err
		}
	} else {
		nxtIDUint64 = common.ByteToU64(nxtID)
	}

	log.Info("mvcc::mvcc::getNextTxnID; done")
	return nxtIDUint64, leaderID, nil
}

func (m *MVCC) init() {
	// todo: abort active txns after crash.
}

// NewMVCC creates a new MVCC transactional layer for the storage
func NewMVCC(id uint64, rs *raft.Server) *MVCC {
	m := &MVCC{
		id:        id,
		mu:        new(sync.RWMutex),
		activeTxn: make(map[uint64]*Transaction),
		rs:        rs,
	}

	m.init()

	return m
}
