package mvcc

import (
	"github.com/dr0pdb/icecanedb/pkg/raft"
)

type mvccTestHarness struct {
	mvcc       *MVCC
	raftServer *raft.TestRaftServer
}

func newMvccTestHarness(testDirectory string, isLeader bool) *mvccTestHarness {
	rs := raft.NewTestRaftServer(testDirectory, isLeader)
	mvcc := NewMVCC(1, rs)
	return &mvccTestHarness{
		raftServer: rs,
		mvcc:       mvcc,
	}
}

func (m *mvccTestHarness) setLeader(isLeader bool) {
	m.raftServer.SetLeadership(isLeader)
}

func (m *mvccTestHarness) close() {
	m.raftServer.Close()
}
