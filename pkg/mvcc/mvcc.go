package mvcc

import (
	"context"
	"sync"

	"github.com/dr0pdb/icecanedb/pkg/common"
	pb "github.com/dr0pdb/icecanedb/pkg/protogen"
	"github.com/dr0pdb/icecanedb/pkg/raft"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MVCC is the Multi Version Concurrency Control layer for transactions.
// Operations on it are thread safe using a RWMutex
type MVCC struct {
	mu *sync.RWMutex

	// active transactions at the present moment.
	activeTxn map[uint64]*Transaction

	// the underlying raft node.
	rs *raft.Server
}

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

	log.Info("mvcc::mvcc::Begin; done")
	return txn, leaderID, nil
}

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

// NewMVCC creates a new MVCC transactional layer for the storage
func NewMVCC(rs *raft.Server) *MVCC {
	return &MVCC{
		mu:        new(sync.RWMutex),
		activeTxn: make(map[uint64]*Transaction),
		rs:        rs,
	}
}
