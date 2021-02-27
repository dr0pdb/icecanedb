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

const (
	// MinElectionTimeout is the min duration for which a follower waits before becoming a candidate
	MinElectionTimeout = 300 * time.Millisecond
	// MaxElectionTimeout is the max duration for which a follower waits before becoming a candidate
	MaxElectionTimeout = 400 * time.Millisecond
)

const (
	leader uint64 = iota
	follower
	candidate
)

const (
	noVote   uint64 = 0
	noLeader uint64 = 0
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
	// places where this is set:
	// 1. follower
	// 2. candidate
	// 3. leader
	// 4. init
	// 4 is at the start and only one role out of 1,2,3 are possible. So a lock is fine.
	currentTerm, votedFor common.ProtectedUint64

	// stores the raft logs
	// key: index and value: serialized form of the command.
	// Once storage implements persistence then it will be truly committed.
	raftStorage *storage.Storage

	// commitIndex is the index of highest log entry known to be committed
	// init to 0 and increase monotonically
	commitIndex common.ProtectedUint64

	// lastApplied is the index of highest log entry applied to state machine
	// init to 0 and increases monotonically.
	lastApplied common.ProtectedUint64

	// each peers progress
	// volatile state on leaders.
	// Reinitialized after election.
	// key: id
	// Important - Should also include the dummy progress for the current node.
	// len(allProgress) is used as the count of nodes in the cluster.
	allProgress map[uint64]*Progress

	// this peer's role
	// TODO: protect with mutex
	role common.ProtectedUint64

	// kvConfig is the complete key value config.
	kvConfig *common.KVConfig

	// s is the server
	s *Server

	// istate is the internal state of raft consisting of channels for communication.
	istate *internalState
}

type internalState struct {
	// grpc requests from the client
	clientRequests chan interface{}

	// append grpc requests from the leader
	appendEntriesRequests chan *pb.AppendEntriesRequest
	appendEntriesReponses chan *pb.AppendEntriesResponse

	// grpc requests for requesting votes and responses
	requestVoteRequests  chan *pb.RequestVoteRequest
	requestVoteResponses chan bool

	// running indicates if the server is running or not.
	// to stop the server, set it to false
	running common.ProtectedBool

	// currentLeader is the current leader of raft according to this node.
	// could be inaccurate but doesn't affect correctness.
	currentLeader common.ProtectedUint64

	// lastAppendOrVoteTime is the latest time at which we received an append request from the leader
	// or we casted a vote to a candidate.
	lastAppendOrVoteTime time.Time

	// endFollower is the channel on which the follower routine ends
	endFollower chan bool

	followerRunning, candRunning, leaderRunning common.ProtectedBool
}

//
// grpc server calls
//

// RequestVote is used by the raft candidate to request for votes.
// The current node has received a request to vote by another peer.
func (r *Raft) requestVote(ctx context.Context, request *pb.RequestVoteRequest) (resp *pb.RequestVoteResponse, err error) {
	log.WithFields(log.Fields{"id": r.id, "candidateId": request.CandidateId, "candidateTerm": request.Term}).Info("raft::raft::requestVote; received grpc request to vote;")
	r.istate.requestVoteRequests <- request
	t := time.After(getElectionTimeout())
	voteGranted := false

	select {
	case granted := <-r.istate.requestVoteResponses:
		voteGranted = granted
	case <-t:
		err = fmt.Errorf("request timeout")
	}

	return &pb.RequestVoteResponse{
		Term:        r.currentTerm.Get(),
		VoteGranted: voteGranted,
		VoterId:     r.id,
	}, err
}

// appendEntries is invoked by leader to replicate log entries; also used as heartbeat
func (r *Raft) appendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (resp *pb.AppendEntriesResponse, err error) {
	log.WithFields(log.Fields{"id": r.id, "leaderID": req.LeaderId, "leaderTerm": req.Term}).Info("raft::raft::appendEntries; received grpc request to append entries;")
	r.istate.appendEntriesRequests <- req
	t := time.After(getElectionTimeout())

	select {
	case resp = <-r.istate.appendEntriesReponses:
		break
	case <-t:
		err = fmt.Errorf("request timeout")
	}

	return resp, err
}

//
// internal functions
//

func (r *Raft) sendRequestVote(rl *raftLog, voterID uint64) (*pb.RequestVoteResponse, error) {
	log.WithFields(log.Fields{"id": r.id}).Info(fmt.Sprintf("raft::raft::sendRequestVote; sending vote to peer %d", voterID))
	req := &pb.RequestVoteRequest{
		Term:         r.currentTerm.Get(),
		CandidateId:  r.id,
		LastLogIndex: r.commitIndex.Get(),
		LastLogTerm:  rl.term,
	}
	return r.s.sendRequestVote(voterID, req)
}

func (r *Raft) sendAppendEntries(receiverID, prevLogIndex, prevLogTerm uint64) (*pb.AppendEntriesResponse, error) {
	log.WithFields(log.Fields{"id": r.id}).Info(fmt.Sprintf("raft::raft::sendAppendEntries; sending append entries to peer %d", receiverID))
	// todo: add entries.
	req := &pb.AppendEntriesRequest{
		Term:         r.currentTerm.Get(),
		LeaderId:     r.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: r.commitIndex.Get(),
	}
	return r.s.sendAppendEntries(receiverID, req)
}

// follower does the role of a raft follower.
func (r *Raft) follower() {
	log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::followerroutine; became follower;")
	r.istate.followerRunning.Set(true)
	for {
		select {
		case <-r.istate.endFollower:
			log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::followerroutine; received on endFollower chan")
			goto end

		case req := <-r.istate.requestVoteRequests:
			log.WithFields(log.Fields{"id": r.id, "candidate": req.CandidateId}).Info("raft::raft::followerroutine; request for vote received")
			if req.Term >= r.currentTerm.Get() && r.votedFor.Get() == noVote && r.isUpToDate(req) {
				log.WithFields(log.Fields{"id": r.id, "candidate": req.CandidateId}).Info("raft::raft::followerroutine; vote yes")
				r.istate.lastAppendOrVoteTime = time.Now()
				r.istate.requestVoteResponses <- true
				r.currentTerm.Set(req.Term)
			} else {
				log.WithFields(log.Fields{"id": r.id, "candidate": req.CandidateId}).Info("raft::raft::followerroutine; vote no")
				r.istate.requestVoteResponses <- false
			}

		case req := <-r.istate.appendEntriesRequests:
			log.WithFields(log.Fields{"id": r.id, "leader": req.LeaderId, "leaderTerm": req.Term}).Info("raft::raft::followerroutine; leader append request received")
			r.istate.lastAppendOrVoteTime = time.Now()

			ct := r.currentTerm.Get()
			success := true

			if len(req.Entries) != 0 {
				// TODO: check for conflicts and save to raft logs if ok
			}

			if success {
				r.istate.currentLeader.Set(req.LeaderId)
			}

			resp := &pb.AppendEntriesResponse{
				Term:    ct,
				Success: success,
			}
			r.istate.appendEntriesReponses <- resp
			if r.currentTerm.Get() < req.Term {
				r.currentTerm.Set(req.Term)
			}

		case <-r.istate.clientRequests: // client requests are ignored if the peer is not the leader
			log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::followerroutine; server request received")
			// TODO: redirect to the leader
		}
	}
end:
	r.istate.followerRunning.Set(false)
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
	if r.commitIndex.Get() > 0 {
		b, err := r.raftStorage.Get(common.U64ToByte(r.commitIndex.Get()), &storage.ReadOptions{})
		if err != nil {
			return false
		}
		rl, err := deserializeRaftLog(b)
		if err != nil {
			return false
		}

		// we decline if the candidate log is not at least up to date as us.
		if rl.term > req.LastLogTerm || r.commitIndex.Get() > req.LastLogIndex {
			return false
		}
	}

	return true
}

func (r *Raft) candidate() {
	log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::candidateroutine; became candidate;")
	r.istate.candRunning.Set(true)
	for {
		log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::candidateroutine; requesting votes")
		cnt := 1 // counting self votes
		allcnt := 1
		voteRecCh := make(chan *pb.RequestVoteResponse, len(r.allProgress))

		// this is fine, since there is only one routine that is doing the write on the current term.
		// If we had multiple writers, we would need an atomic int and do compare and swap
		r.currentTerm.Set(r.currentTerm.Get() + 1)

		// voting for myself
		r.votedFor.Set(r.id)

		// dummy raft log with term 0.
		rl := &raftLog{
			term: 0,
		}

		if r.commitIndex.Get() > 0 {
			// get the latest committed entry to get it's term number
			b, err := r.raftStorage.Get(common.U64ToByte(r.commitIndex.Get()), &storage.ReadOptions{})
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
				go func(id uint64) {
					resp, err := r.sendRequestVote(rl, id)
					if err != nil {
						log.Error(fmt.Sprintf("raft::raft::candidateroutine; error response to the vote request: %v", err))
						return
					}
					voteRecCh <- resp
				}(i)
			}
		}

		t := time.After(getElectionTimeout())

		for {
			select {
			case <-r.istate.appendEntriesRequests: // todo: should we process the request as well?
				log.WithFields(log.Fields{"id": r.id}).Info(fmt.Sprintf("raft::raft::candidateroutine; received append request from ..; becoming follower again"))
				r.istate.lastAppendOrVoteTime = time.Now()
				r.role.Set(follower)
				goto end

			case resp := <-voteRecCh:
				log.WithFields(log.Fields{"id": r.id, "voterId": resp.VoterId, "granted": resp.VoteGranted}).Info(fmt.Sprintf("raft::raft::candidateroutine; received vote from %d", resp.VoterId))
				if resp.VoteGranted {
					cnt++
				}

				// TODO: update progress using the resp.Term

				allcnt++
				if allcnt == len(r.allProgress) {
					goto majiorityCheck
				}
				break

			case <-t:
				log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::candidateroutine; leader election timed out.")
				close(voteRecCh) // this might lead to rpc go routines crashing but that isn't an issue.
				goto majiorityCheck
			}
		}

	majiorityCheck:
		if isMajiority(cnt, len(r.allProgress)) {
			log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::candidateroutine; received majiority of votes. becoming leader")
			r.role.Set(leader)
			goto end
		} else {
			log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::candidateroutine; didn't receive majiority votes; restarting...")
		}
	}

end:
	r.istate.candRunning.Set(false)
	log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::candidateroutine; ending;")
}

func (r *Raft) leader() {
	log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::leaderroutine; became leader;")
	r.istate.leaderRunning.Set(true)

	r.istate.leaderRunning.Set(false)
	log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::leaderroutine; ending;")
}

// becomeFollower updates the role to follower and blocks untill the cand and leader routines stop.
func (r *Raft) becomeFollower() {
	log.Info("raft::raft::becomeFollower; started")

	r.role.Set(follower)
	for {
		if !r.istate.candRunning.Get() && !r.istate.leaderRunning.Get() {
			break
		}
	}

	log.Info("raft::raft::becomeFollower; done")
}

// becomeCandidate updates the role to follower and blocks untill the follower and leader routines stop.
func (r *Raft) becomeCandidate() {
	log.Info("raft::raft::becomeCandidate; started")

	r.istate.endFollower <- true
	r.role.Set(candidate)
	for {
		if !r.istate.followerRunning.Get() && !r.istate.leaderRunning.Get() {
			break
		}
	}

	log.Info("raft::raft::becomeCandidate; done")
}

// becomeLeader updates the role to follower and blocks untill the cand and follower routines stop.
func (r *Raft) becomeLeader() {
	log.Info("raft::raft::becomeLeader; started")

	r.istate.endFollower <- true
	r.role.Set(leader)
	for {
		if !r.istate.candRunning.Get() && !r.istate.followerRunning.Get() {
			break
		}
	}

	log.Info("raft::raft::becomeLeader; done")
}

func (r *Raft) electionTimerRoutine() {
	log.Info("raft::raft::electionTimerRoutine; started")
	go func() {
		time.Sleep(2 * time.Second) // wait for other components to init
		for {
			time.Sleep(50 * time.Millisecond)

			if r.role.Get() == follower {
				ts := getElectionTimeout()
				if time.Now().Sub(r.istate.lastAppendOrVoteTime) > ts {
					log.Info("raft::raft::electionTimerRoutine; election timeout triggered; sending end signal to follower")
					r.role.Set(candidate)
					r.istate.endFollower <- true
				}
			}
		}
	}()
	log.Info("raft::raft::electionTimerRoutine; done")
}

func (r *Raft) heartBeatRoutine() {
	log.Info("raft::raft::heartBeatRoutine; started")
	go func() {
		time.Sleep(MinElectionTimeout / 5)

		if r.role.Get() == leader {
			ch := make(chan *pb.AppendEntriesResponse, len(r.allProgress))

			for i := range r.allProgress {
				if i != r.id {
					go func(id uint64) {
						resp, err := r.sendAppendEntries(id, 0, 0)
						if err != nil {
							// handle error
						}

						ch <- resp
					}(i)
				}
			}

			t := time.After(getElectionTimeout())

			for {
				select {
				case _ = <-ch:
					// update term and move to follower if required.
				case <-t:
					// handle timeout
				}
			}
		}
	}()

	log.Info("raft::raft::heartBeatRoutine; done")
}

func (r *Raft) applyRoutine() {
	log.Info("raft::raft::applyRoutine; started")

	go func() {
		time.Sleep(1 * time.Second) // todo: adjust?

		la := r.lastApplied.Get()
		ci := r.commitIndex.Get()

		if la < ci {
			log.Info(fmt.Sprintf("raft::raft::applyRoutine; applying logs to the storage layer from index %d to %d", la+1, ci))

			for idx := la + 1; idx <= ci; idx++ {
				// todo: call server to apply the log
			}
		}
	}()

	log.Info("raft::raft::applyRoutine; done")
}

func (r *Raft) init() {
	log.Info("raft::raft::init; started")

	r.electionTimerRoutine()
	r.heartBeatRoutine()
	r.applyRoutine()
	go func() {
		time.Sleep(time.Second) // allow starting grpc server
		log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::initroutine; starting;")
		r.istate.running.Set(true)
		for {
			role := r.role.Get()
			if role == follower {
				r.follower()
			} else if role == candidate {
				r.candidate()
			} else if role == leader {
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
		raftStorage: raftStorage,
		allProgress: initProgress(kvConfig.ID, kvConfig.Peers),
		istate: &internalState{
			clientRequests:        make(chan interface{}), // todo: consider some buffer?
			appendEntriesRequests: make(chan *pb.AppendEntriesRequest),
			appendEntriesReponses: make(chan *pb.AppendEntriesResponse),
			requestVoteRequests:   make(chan *pb.RequestVoteRequest),
			requestVoteResponses:  make(chan bool),
			endFollower:           make(chan bool),
			lastAppendOrVoteTime:  time.Now(), // todo: any issue with this?
		},
		kvConfig: kvConfig,
		s:        s,
	}

	// TODO: update after persistence
	r.commitIndex.Set(0)
	r.lastApplied.Set(0)
	r.currentTerm.Set(0)
	r.istate.currentLeader.Set(noLeader)
	r.role.Set(follower)

	r.votedFor.Set(noVote)

	r.init()
	log.Info("raft::raft::NewRaft; done")
	return r
}

func initProgress(id uint64, peers []common.Peer) map[uint64]*Progress {
	log.Info("raft::raft::initProgress; started")
	m := make(map[uint64]*Progress)

	m[id] = &Progress{
		Match: 1,
		Next:  0,
	}

	for _, p := range peers {
		m[p.ID] = &Progress{
			Match: 1, // todo: handle persistence later.
			Next:  0,
		}
	}

	log.Info("raft::raft::initProgress; started")
	return m
}
