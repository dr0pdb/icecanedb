package raft

import "fmt"

// raftLog is serialized and stored into the raft storage
type raftLog struct {
	term    uint64
	command string
}

func (rl *raftLog) toBytes() []byte {
	res := make([]byte, 8)

	// encode term in first 8 bytes
	res[0] = uint8(rl.term) // last 1 byte of term
	res[1] = uint8(rl.term >> 8)
	res[2] = uint8(rl.term >> 16)
	res[3] = uint8(rl.term >> 24)
	res[4] = uint8(rl.term >> 32)
	res[5] = uint8(rl.term >> 40)
	res[6] = uint8(rl.term >> 48)
	res[7] = uint8(rl.term >> 56)

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
