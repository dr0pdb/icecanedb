package mvcc

import (
	"context"
	"fmt"
	"sync"

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

	txnID, err := m.ensureTxn(req.TxnId, pb.TxnMode_ReadOnly)
	if err != nil {
		return nil, err
	}
	req.TxnId = txnID

	return nil, status.Errorf(codes.Unimplemented, "method RawGet not implemented")
}

// Set sets the value of a key.
func (m *MVCC) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RawGet not implemented")
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

	txnKey := []byte(getKey(notUsed, nxtTxnID))
	nxtID, leaderID, err := m.rs.GetValue(txnKey)
	nxtIDUint64 := common.ByteToU64(nxtID)
	if err != nil {
		return nil, leaderID, err
	}
	_, err = m.rs.SetValue(txnKey, common.U64ToByte(nxtIDUint64+1))
	if err != nil {
		return nil, leaderID, err
	}

	var cTxns []uint64
	for k := range m.activeTxn {
		cTxns = append(cTxns, k)
	}

	txn := newTransaction(nxtIDUint64, cTxns, mode, m.rs)
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

// NewMVCC creates a new MVCC transactional layer for the storage
func NewMVCC(rs *raft.Server) *MVCC {
	// todo: set txnNext to 1 at the first startup

	return &MVCC{
		mu:        new(sync.RWMutex),
		activeTxn: make(map[uint64]*Transaction),
		rs:        rs,
	}
}
