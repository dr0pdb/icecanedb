package raft

import (
	"math/rand"
	"time"
)

// raftServerApplyMsg is the message sent by raft.Raft to raft.Server.
// This is used to apply modifications to the storage layer after a log has been committed.
type raftServerApplyMsg struct{}

// raftServerApplyMsg is the message sent by raft.Raft to raft.Server.
// This is used to communicate with other peers.
type raftServerCommunicationMsg struct{}

func isMajiority(cnt, allcnt int) bool {
	return cnt*2 > allcnt
}

// TODO: may be seed it?
func getElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(int(MaxElectionTimeout)-int(MinElectionTimeout)) + int(MinElectionTimeout))
}
