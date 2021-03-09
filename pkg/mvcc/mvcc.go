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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
		err = m.commitTxn(req.TxnId)
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
	log.Info("mvcc::mvcc::Set; started")

	resp := &pb.SetResponse{}

	txnID, err := m.ensureTxn(req.TxnId, pb.TxnMode_ReadOnly)
	if err != nil {
		return nil, err
	}
	req.TxnId = txnID

	m.mu.Lock()
	defer m.mu.Unlock()

	txn := m.activeTxn[txnID]

	for id := range txn.concTxns {
		tkey := newTxnKey(req.Key, id)

		_, leaderID, err := m.rs.MetaGetValue(tkey)
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
	_, err = m.rs.SetValue(req.GetKey(), req.GetValue())
	if err != nil {

	}

	return resp, err
}

// Delete deletes the value of a key.
func (m *MVCC) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RawGet not implemented")
}

// CommitTxn attempts to commit a MVCC txn
func (m *MVCC) CommitTxn(ctx context.Context, req *pb.CommitTxnRequest) (*pb.CommitTxnResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RawGet not implemented")
}

// RollbackTxn rollsback a MVCC txn
func (m *MVCC) RollbackTxn(ctx context.Context, req *pb.RollbackTxnRequest) (*pb.RollbackTxnResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RawGet not implemented")
}

//
// Internal methods
//

// begin begins a new Transaction
func (m *MVCC) begin(mode pb.TxnMode) (*Transaction, uint64, error) {
	log.Info("mvcc::mvcc::Begin; started")

	m.mu.Lock()
	defer m.mu.Unlock()

	txnKey := []byte(getKey(notUsed, nxtTxnID, nil))
	nxtID, leaderID, err := m.rs.MetaGetValue(txnKey)
	nxtIDUint64 := common.ByteToU64(nxtID)
	if err != nil {
		return nil, leaderID, err
	}
	_, err = m.rs.SetValue(txnKey, common.U64ToByte(nxtIDUint64+1))
	if err != nil {
		return nil, leaderID, err
	}

	// snapshot of active txns at the moment of creation.
	var cTxns map[uint64]bool
	for k := range m.activeTxn {
		cTxns[k] = true
	}

	// TODO: store cTxns and mark as active in storage

	txn := newTransaction(nxtIDUint64, cTxns, mode)
	m.activeTxn[txn.id] = txn

	log.Info("mvcc::mvcc::Begin; done")
	return txn, leaderID, nil
}

// ensureTxn ensures a txn exists.
// if id = 0, it creates a new txn with the given mode.
// if id != 0, it ensures that the txn is active.
func (m *MVCC) ensureTxn(id uint64, mode pb.TxnMode) (uint64, error) {
	if id == 0 {
		txnID, _, err := m.begin(mode)
		if err != nil {
			return 0, err
		}

		id = txnID.id
	} else {
		if _, ok := m.activeTxn[id]; !ok {
			return id, fmt.Errorf("error: inactive transaction")
		}
	}

	return id, nil
}

// commitTxn commits a txn with the given id
func (m *MVCC) commitTxn(id uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if txn, ok := m.activeTxn[id]; ok {
		if txn.mode == pb.TxnMode_ReadOnly {
			// delete the key activeTxn from storage as well.

			delete(m.activeTxn, id)
		}
	} else {
		return fmt.Errorf("commit on inactive txn")
	}

	return nil
}

// NewMVCC creates a new MVCC transactional layer for the storage
func NewMVCC(id uint64, rs *raft.Server) *MVCC {
	// todo: set txnNext to 1 at the first startup

	// todo: abort active txns after crash.

	return &MVCC{
		id:        id,
		mu:        new(sync.RWMutex),
		activeTxn: make(map[uint64]*Transaction),
		rs:        rs,
	}
}
