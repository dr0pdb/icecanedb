package raft

import (
	"fmt"
	"strings"

	common "github.com/dr0pdb/icecanedb/pkg/common"
)

type raftCommandType uint64

const (
	setCmd raftCommandType = iota
	deleteCmd
	metaSetCmd
	metaDeleteCmd
)

type raftCommand string

// raftLog is serialized and stored into the raft storage
type raftLog struct {
	term    uint64
	ct      raftCommandType
	command raftCommand
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

	cmd := raftCommand(l[8:])
	ct := setCmd
	if strings.HasPrefix(string(cmd), "DELETE") {
		ct = deleteCmd
	} else if strings.HasPrefix(string(cmd), "METASET") {
		ct = metaSetCmd
	} else if strings.HasPrefix(string(cmd), "METADELETE") {
		ct = metaDeleteCmd
	}

	return &raftLog{
		term:    term,
		command: cmd,
		ct:      ct,
	}, nil
}

func newSetRaftLog(term uint64, key, value []byte) *raftLog {
	return &raftLog{
		term:    term,
		command: raftCommand(fmt.Sprintf("SET %s %s", string(key), string(value))),
		ct:      setCmd,
	}
}

func newDeleteRaftLog(term uint64, key []byte) *raftLog {
	return &raftLog{
		term:    term,
		command: raftCommand(fmt.Sprintf("DELETE %s", string(key))),
		ct:      deleteCmd,
	}
}

func newMetaSetRaftLog(term uint64, key, value []byte) *raftLog {
	return &raftLog{
		term:    term,
		command: raftCommand(fmt.Sprintf("METASET %s %s", string(key), string(value))),
		ct:      metaSetCmd,
	}
}

func newMetaDeleteRaftLog(term uint64, key []byte) *raftLog {
	return &raftLog{
		term:    term,
		command: raftCommand(fmt.Sprintf("METADELETE %s", string(key))),
		ct:      metaDeleteCmd,
	}
}
