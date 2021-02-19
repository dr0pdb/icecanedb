package raft

import (
	"fmt"

	common "github.com/dr0pdb/icecanedb/pkg/common"
)

// raftLog is serialized and stored into the raft storage
type raftLog struct {
	term    uint64
	command string
}

func (rl *raftLog) toBytes() []byte {
	res := common.U64ToByte(rl.term)

	b := []byte(rl.command)
	for _, sb := range b {
		res = append(res, sb)
	}

	return res
}

func deserializeRaftLog(l []byte) (*raftLog, error) {
	if len(l) < 8 {
		return nil, fmt.Errorf("invalid log bytes")
	}
	var term uint64 = 0

	term |= uint64(l[0])
	term |= uint64(l[1]) << 8
	term |= uint64(l[2]) << 16
	term |= uint64(l[3]) << 24
	term |= uint64(l[4]) << 32
	term |= uint64(l[5]) << 40
	term |= uint64(l[6]) << 48
	term |= uint64(l[7]) << 56

	cmd := string(l[8:])
	return &raftLog{
		term:    term,
		command: cmd,
	}, nil
}
