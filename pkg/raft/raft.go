package raft

import (
	"context"
	"fmt"
	"time"

	pb "github.com/dr0pdb/icecanedb/pkg/protogen"
	"github.com/dr0pdb/icecanedb/pkg/storage"
	log "github.com/sirupsen/logrus"
	codes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PeerRole defines the role of a raft peer.
type PeerRole uint64

const (
	leader PeerRole = iota
	follower
	candidate
)

const (
	noVote uint64 = 0
)

// Progress denotes the progress of a peer.
// It indicates where the peer is in it's log.
// This is only used in the leader.
// Refer Fig 2, State (Volatile state on leaders) section of the Raft extended paper
type Progress struct {
	// Match is the index of the next log entry to send to that server
	// init to leader last log index + 1
	Match uint64

	// Next is the index of the highest log entry known to be replicated on server
	// init to 0, increase monotonically.
	Next uint64
}

// Raft is the replicated state machine.
// See Fig 2 of the Raft Extended paper.
// TODO: think about the need of a mutex.
type Raft struct {
	// id of the raft node.
	// IMPORTANT: starts from 1 so that votedFor = 0 indicates no vote.
	id uint64

	// CurrentTerm denotes the latest term node has seen.
	// VotedFor denotes the candidate id that got the vote from this node. 0 is no one.
	currentTerm, votedFor uint64

	// stores the raft logs
	// key: index and value: serialized form of the command.
	// Once storage implements persistence then it will be truly committed.
	raftStorage *storage.Storage

	// commitIndex is the index of highest log entry known to be committed
	// init to 0 and increase monotonically
	commitIndex uint64

	// lastApplied is the index of highest log entry applied to state machine
	// init to 0 and increases monotonically.
	lastApplied uint64

	// each peers progress
	// volatile state on leaders.
	// Reinitialized after election.
	allProgress map[uint64]*Progress

	// this peer's role
	role PeerRole

	// applyCh is used to communicate with the wrapper server
	applyCh chan raftServerApplyMsg
	commCh  chan raftServerCommunicationMsg

	istate *internalState
}

type internalState struct {
	fch, lch, cch chan bool

	// variable indicating whether an append was received in the timeout window
	appendReceived bool
}

//
// grpc server calls
//

// RequestVote is used by the raft candidate to request for votes.
func (r *Raft) requestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RequestVote not implemented")
}

//
// internal functions
//

func (r *Raft) sendRequestVote(id uint64) (*pb.RequestVoteResponse, error) {
	log.WithFields(log.Fields{"id": r.id}).Info(fmt.Sprintf("raft::raft::sendRequestVote; sending vote to %d peer", id))
	panic("")
}

func (r *Raft) initRoutines() {
	// follower
	go func(r *Raft) {
		for {
			if r.role == follower {
				time.Sleep(ElectionTimeout) // todo: add randomness

				// either current leader is alive or I've voted for some other candidate.
				if r.istate.appendReceived || r.votedFor != noVote {
					r.istate.appendReceived = false
					r.votedFor = noVote // reset vote
				} else {
					log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::followerroutine; no append/vote request received.")
					r.role = candidate
					r.istate.cch <- true
				}
			} else {
				log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::followerroutine; sleeping on the follower chan")
				<-r.istate.fch // wait to become the follower
			}
		}
	}(r)

	// candidate
	go func(r *Raft) {
		for {
			if r.role == candidate {
				log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::candidateroutine; became candidate; requesting votes")
				t := time.NewTicker(ElectionTimeout)
				cnt := 1 // counting self votes
				allcnt := 1
				voteRec := make(chan *pb.RequestVoteResponse, len(r.allProgress))

				for i := range r.allProgress {
					if i != r.id {
						go func(i uint64) {
							resp, err := r.sendRequestVote(i)
							if err != nil {
								// todo: handle error
							}
							voteRec <- resp
						}(i)
					}
				}

				for {
					tobreak := false
					select {
					case resp := <-voteRec:
						allcnt++
						if resp.VoteGranted {
							cnt++
							log.WithFields(log.Fields{"id": r.id}).Debug(fmt.Sprintf("raft::raft::candidateroutine; received vote from %d", resp.VoterId))
						}
						if allcnt == len(r.allProgress) {
							tobreak = true
						}
					case <-t.C:
						log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::candidateroutine; leader election timed out. restarting..")
						tobreak = true
					}

					if tobreak {
						break
					}
				}

				if isMajiority(cnt, allcnt) {
					log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::candidateroutine; received majiority of votes. becoming leader")
					r.role = leader
					r.istate.lch <- true
				}

			} else {
				log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::candidateroutine; sleeping on the candidate chan")
				<-r.istate.cch // wait to become the candidate
			}
		}
	}(r)

	// leader
	go func(r *Raft) {
		for {
			if r.role == leader {

			} else {
				<-r.istate.lch // wait to become the leader
			}
		}
	}(r)
}

// NewRaft initializes a new raft state machine.
func NewRaft(id uint64, raftStorage *storage.Storage, applyCh chan raftServerApplyMsg, commCh chan raftServerCommunicationMsg) *Raft {
	r := &Raft{
		id:          id,
		currentTerm: 0, // redundant but still good for clarity to explicitly set to 0
		votedFor:    noVote,
		raftStorage: raftStorage,
		commitIndex: 0,
		lastApplied: 0,
		allProgress: make(map[uint64]*Progress),
		role:        follower, // starts as a follower
		applyCh:     applyCh,
		commCh:      commCh,
		istate: &internalState{
			lch: make(chan bool, 3),
			cch: make(chan bool, 3),
			fch: make(chan bool, 3),
		},
	}

	r.initRoutines()
	return r
}
