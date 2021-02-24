package raft

import (
	"context"
	"fmt"
	"time"

	common "github.com/dr0pdb/icecanedb/pkg/common"
	pb "github.com/dr0pdb/icecanedb/pkg/protogen"
	"github.com/dr0pdb/icecanedb/pkg/storage"
	log "github.com/sirupsen/logrus"
)

// TODO: There are a lot of inconsistencies in the file. Mixed use of boolean flags and goto.
// make it consistent.

const (
	// MinElectionTimeout is the min duration for which a follower waits before becoming a candidate
	MinElectionTimeout = 5000 * time.Millisecond
	// MaxElectionTimeout is the max duration for which a follower waits before becoming a candidate
	MaxElectionTimeout = 2 * MinElectionTimeout
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

	// kvConfig is the complete key value config.
	kvConfig *common.KVConfig

	s *Server

	istate *internalState
}

type internalState struct {
	// grpc requests from the client
	clientRequests chan interface{}

	// append grpc requests from the leader
	appendRequests chan interface{}

	// grpc requests for requesting votes and responses
	requestVoteRequests  chan *pb.RequestVoteRequest
	requestVoteResponses chan bool

	// running indicates if the server is running or not.
	// to stop the server, set it to false
	running common.ProtectedBool
}

//
// grpc server calls
//

// RequestVote is used by the raft candidate to request for votes.
// The current node has received a request to vote by another peer.
func (r *Raft) requestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	log.WithFields(log.Fields{"id": r.id, "candidateId": request.CandidateId, "candidateTerm": request.Term}).Info("raft::raft::requestVote; received grpc request to vote;")
	r.istate.requestVoteRequests <- request
	t := time.NewTicker(getElectionTimeout())
	voteGranted := false
	for {
		tobreak := false
		select {
		case granted := <-r.istate.requestVoteResponses:
			voteGranted = granted
			tobreak = true
		case <-t.C:
			return nil, fmt.Errorf("request timeout")
		}

		if tobreak {
			break
		}
	}
	return &pb.RequestVoteResponse{
		Term:        r.currentTerm,
		VoteGranted: voteGranted,
		VoterId:     r.id,
	}, nil
}

//
// internal functions
//

func (r *Raft) sendRequestVote(rl *raftLog, voterID uint64) (*pb.RequestVoteResponse, error) {
	log.WithFields(log.Fields{"id": r.id}).Info(fmt.Sprintf("raft::raft::sendRequestVote; sending vote to peer %d", voterID))
	req := &pb.RequestVoteRequest{
		Term:         r.currentTerm,
		CandidateId:  r.id,
		LastLogIndex: r.commitIndex,
		LastLogTerm:  rl.term,
	}
	return r.s.sendRequestVote(voterID, req)
}

// follower is the follower routine that does the role of raft follower.
func (r *Raft) follower() {
	log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::followerroutine; became follower;")
	t := time.NewTicker(getElectionTimeout())

	for {
		tobreak := false
		select {
		case <-t.C:
			log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::followerroutine; no append/vote request received. timeout")
			r.role = candidate
			t.Stop()
			tobreak = true

		case <-r.istate.clientRequests: // client requests are ignored if the peer is not the leader
			log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::followerroutine; server request received")

		case <-r.istate.appendRequests:
			log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::followerroutine; leader request received")
			// apply it.
			t.Reset(getElectionTimeout())

		case req := <-r.istate.requestVoteRequests:
			log.WithFields(log.Fields{"id": r.id, "candidate": req.CandidateId}).Debug("raft::raft::followerroutine; request for vote received")
			if req.Term >= r.currentTerm && r.votedFor == noVote && r.isUpToDate(req) {
				r.istate.requestVoteResponses <- true
				t.Reset(getElectionTimeout())
			} else {
				r.istate.requestVoteResponses <- false
			}

		}

		if tobreak {
			break
		}
	}
	log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::followerroutine; follower routine ending;")
}

// isUpToDate returns if the candidate's log is to update in comparison to this node.
// it compares the log of the candidate with this node's log
// for the last entry of the committed log,
// if it's terms are different, we favour the later term.
// in case of tie, we favour the index of the last term.
// for more info check section 5.4.1 last paragraph of the paper.
func (r *Raft) isUpToDate(req *pb.RequestVoteRequest) bool {
	// commit Index is 0 when the server just starts.
	// In this case, the candidate will always be at least up to date as us.
	if r.commitIndex > 0 {
		b, err := r.raftStorage.Get(common.U64ToByte(r.commitIndex), &storage.ReadOptions{})
		if err != nil {
			return false
		}
		rl, err := deserializeRaftLog(b)
		if err != nil {
			return false
		}

		// we decline if the candidate log is not at least up to date as us.
		if rl.term > req.LastLogTerm || r.commitIndex > req.LastLogIndex {
			return false
		}
	}

	return true
}

func (r *Raft) candidate() {
	log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::candidateroutine; became candidate;")
	for {
		log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::candidateroutine; requesting votes")
		cnt := 1 // counting self votes
		allcnt := 1
		voteRecCh := make(chan *pb.RequestVoteResponse, len(r.allProgress))
		r.currentTerm++
		r.votedFor = noVote

		// dummy raft log with term 0
		rl := &raftLog{
			term: 0,
		}

		if r.commitIndex > 0 {
			// get the latest committed entry to get it's term number
			b, err := r.raftStorage.Get(common.U64ToByte(r.commitIndex), &storage.ReadOptions{})
			if err != nil {
				// todo: handle this? this is a fatal failure. panic?
				log.Error(fmt.Sprintf("raft::raft::candidateroutine; error in fetching last committed entry: %v", err))
			}
			rl, err = deserializeRaftLog(b)
			if err != nil {
				// todo: handle this? this is a fatal failure. panic?
				log.Error(fmt.Sprintf("raft::raft::candidateroutine; error in deserializing last committed entry: %v", err))
			}
		}

		for i := range r.allProgress {
			if i != r.id {
				go func(i uint64) {
					resp, err := r.sendRequestVote(rl, i)
					if err != nil {
						log.Error(fmt.Sprintf("raft::raft::candidateroutine; error in sending vote request: %v", err))
					}
					voteRecCh <- resp
				}(i)
			}
		}

		t := time.After(getElectionTimeout())

		for {
			select {
			case resp := <-voteRecCh:
				if resp != nil {
					log.WithFields(log.Fields{"id": r.id, "voterId": resp.VoterId, "granted": resp.VoteGranted}).Info(fmt.Sprintf("raft::raft::candidateroutine; received vote from %d", resp.VoterId))
					if resp.VoteGranted {
						cnt++
					}
				}
				allcnt++
				if allcnt == len(r.allProgress) {
					goto majiorityCheck
				}
			case <-r.istate.appendRequests:
				log.WithFields(log.Fields{"id": r.id}).Info(fmt.Sprintf("raft::raft::candidateroutine; received append request from ..; becoming follower again"))
				r.role = follower
				goto end
			case <-t:
				log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::candidateroutine; leader election timed out.")
				goto majiorityCheck
			}
		}
	majiorityCheck:
		if isMajiority(cnt, allcnt) {
			log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::candidateroutine; received majiority of votes. becoming leader")
			r.role = leader
			goto end
		} else {
			log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::candidateroutine; didn't receive majiority votes; restarting...")
		}
	}

end:
	log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::candidateroutine; ending;")
}

func (r *Raft) leader() {
	log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::leaderroutine; became leader;")

	log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::leaderroutine; ending;")
}

func (r *Raft) init() {
	log.Info("raft::raft::init; started")
	go func() {
		time.Sleep(time.Second) // allow starting grpc server
		log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::initroutine; starting;")
		r.istate.running.Set(true)
		for {
			if r.role == follower {
				r.follower()
			} else if r.role == candidate {
				r.candidate()
			} else if r.role == leader {
				r.leader()
			} else if !r.istate.running.Get() {
				log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::initroutine; stopping;")
				break
			}
		}
	}()
	log.Info("raft::raft::init; done")
}

// NewRaft initializes a new raft state machine.
func NewRaft(kvConfig *common.KVConfig, raftStorage *storage.Storage, s *Server) *Raft {
	log.Info("raft::raft::NewRaft; started")
	r := &Raft{
		id:          kvConfig.ID,
		currentTerm: 0, // redundant but still good for clarity to explicitly set to 0
		votedFor:    noVote,
		raftStorage: raftStorage,
		commitIndex: 0,
		lastApplied: 0,
		allProgress: make(map[uint64]*Progress),
		role:        follower, // starts as a follower
		istate: &internalState{
			clientRequests:      make(chan interface{}), // todo: consider some buffer?
			appendRequests:      make(chan interface{}),
			requestVoteRequests: make(chan *pb.RequestVoteRequest),
		},
		kvConfig: kvConfig,
		s:        s,
	}
	r.init()
	log.Info("raft::raft::NewRaft; done")
	return r
}
