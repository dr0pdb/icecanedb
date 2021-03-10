package raft

import (
	"math/rand"
	"time"
)

type setRequest struct {
	key, value []byte
	meta       bool
}

type deleteRequest struct {
	key  []byte
	meta bool
}

func isMajiority(cnt, allcnt int) bool {
	return cnt*2 > allcnt
}

// TODO: may be seed it?
func getElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(int(MaxElectionTimeout)-int(MinElectionTimeout)) + int(MinElectionTimeout))
}
