package mvcc

import (
	"os"

	"github.com/dr0pdb/icecanedb/pkg/raft"
)

type mvccTestHarness struct {
	mvcc       *MVCC
	raftServer *raft.TestRaftServer

	testDirectory string
}

func newMvccTestHarness(testDirectory string, isLeader bool) *mvccTestHarness {
	txnComp := NewtxnKeyComparator()
	rs := raft.NewTestRaftServer(testDirectory, isLeader, txnComp)
	mvcc := NewMVCC(1, rs)
	return &mvccTestHarness{
		raftServer:    rs,
		mvcc:          mvcc,
		testDirectory: testDirectory,
	}
}

func (m *mvccTestHarness) init() error {
	err := os.MkdirAll(m.testDirectory, os.ModePerm)
	if err != nil {
		return err
	}

	return m.raftServer.Init()
}

func (m *mvccTestHarness) setLeader(isLeader bool) {
	m.raftServer.SetLeadership(isLeader)
}

func (m *mvccTestHarness) cleanup() {
	m.raftServer.Close()
}
