package raft

import (
	"context"
	"fmt"
	"sync"
	"time"

	common "github.com/dr0pdb/icecanedb/pkg/common"
	pb "github.com/dr0pdb/icecanedb/pkg/protogen/icecanedbpb"
	"github.com/dr0pdb/icecanedb/pkg/storage"
	log "github.com/sirupsen/logrus"
)

const (
	minTimeoutSeconds = 1
	maxTimeoutSeconds = 2

	// MinElectionTimeout is the min duration for which a follower waits before becoming a candidate
	MinElectionTimeout = minTimeoutSeconds * time.Second
	// MaxElectionTimeout is the max duration for which a follower waits before becoming a candidate
	MaxElectionTimeout = maxTimeoutSeconds * time.Second
)

// persistent state of the node
var (
	termKey         = []byte("termKey")
	votedForKey     = []byte("votedFor")
	lastLogIndexKey = []byte("lastLogIndex")
)

type state uint64

const (
	leader state = iota
	follower
	candidate
	dead
)

func (s state) String() string {
	switch s {
	case leader:
		return "leader"

	case follower:
		return "follower"

	case candidate:
		return "candidate"

	case dead:
		return "dead"

	default:
		panic("unreachable")
	}
}

const (
	noVote uint64 = iota
)

// Progress denotes the progress of a peer.
// It indicates where the peer is in it's log.
// This is only used in the leader.
// Refer Fig 2, State (Volatile state on leaders) section of the Raft extended paper
type Progress struct {
	// Next is the index of the next log entry to send to that server
	// init to leader last log index + 1
	Next uint64

	// Match is the index of the highest log entry known to be replicated on server
	// init to 0, increase monotonically.
	Match uint64
}

// Raft is the replicated state machine.
// See Fig 2 of the Raft Extended paper.
type Raft struct {
	// mu protects the state
	mu *sync.RWMutex

	// id of the raft node.
	// IMPORTANT: starts from 1 so that votedFor = 0 indicates no vote.
	id uint64

	/*
		Persisted values
	*/

	// CurrentTerm denotes the latest term node has seen.
	// VotedFor denotes the candidate id that got the vote from this node. 0 is no one.
	currentTerm, votedFor uint64

	// stores the raft logs
	// key: index and value: serialized form of the command.
	// IMP: index starts from 1.
	raftStorage *storage.Storage

	// lastLogIndex contains the index of the highest log entry stored in raftStorage.
	// commitIndex <= lastLogIndex
	lastLogIndex uint64

	/*
		Volatile values
	*/

	// commitIndex is the index of highest log entry known to be committed
	// init to 0 and increase monotonically
	commitIndex uint64

	// lastApplied is the index of highest log entry applied to state machine
	// init to 0 and increases monotonically.
	lastApplied uint64

	// each peers progress
	// volatile state on leaders.
	// Reinitialized after election.
	// key: id
	// Important - Should also include the dummy progress for the current node.
	// len(allProgress) is used as the count of nodes in the cluster.
	allProgress map[uint64]*Progress

	// this peer's role
	role state

	// kvConfig is the complete key value config.
	kvConfig *common.KVConfig

	// s is the server
	s *Server

	// istate is the internal state of raft consisting of channels for communication.
	istate *internalState

	// snapshot if log size exceeds it. -1 indicates no snapshotting
	maxRaftState int64
}

type internalState struct {
	// grpc requests from the client
	clientRequests  chan interface{}
	clientResponses chan bool

	// lastAppendOrVoteTime is the latest time at which we received an append request from the leader
	// or we casted a vote to a candidate.
	lastAppendOrVoteTime time.Time

	// applyCommittedEntriesCh is the signal to apply the committed entries to the state machine
	applyCommittedEntriesCh chan interface{}
}

//
// grpc server calls: server to raft
//

// getNodeState returns the role, term of the node
// NOTE: Holds the internal read lock of raft.
func (r *Raft) getNodeState() (term uint64, role state) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.currentTerm, r.role
}

// getLastCommittedIndex returns the last committed log index
// NOTE: Holds the internal read lock of raft.
func (r *Raft) getLastCommittedIndex() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.commitIndex
}

// RequestVote is used by the raft candidate to request for votes.
// The current node has received a request to vote by another peer.
// NOTE: Holds the internal lock of raft.
func (r *Raft) handleRequestVote(ctx context.Context, req *pb.RequestVoteRequest) (resp *pb.RequestVoteResponse, err error) {
	log.WithFields(log.Fields{"id": r.id, "candidateId": req.CandidateId, "candidateTerm": req.Term}).Info("raft::raft::requestVote; received grpc request to vote;")

	// todo: check role

	r.mu.RLock()
	ct := r.currentTerm
	votedFor := r.votedFor
	r.mu.RUnlock()

	granted := false

	if (req.Term > ct || (req.Term == ct && votedFor == noVote)) && r.isUpToDate(req) {
		log.WithFields(log.Fields{"id": r.id, "candidate": req.CandidateId}).Info("raft::raft::followerroutine; vote yes")

		r.mu.Lock()
		r.istate.lastAppendOrVoteTime = time.Now()
		r.setTerm(req.Term)
		r.setVotedFor(req.CandidateId)
		r.mu.Unlock()

		granted = true
	} else {
		log.WithFields(log.Fields{"id": r.id, "candidate": req.CandidateId}).Info("raft::raft::followerroutine; vote no")
	}

	return &pb.RequestVoteResponse{
		Term:        ct,
		VoteGranted: granted,
		VoterId:     r.id,
	}, err
}

// appendEntries is invoked by leader to replicate log entries; also used as heartbeat
// NOTE: Holds the internal lock of raft.
func (r *Raft) handleAppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (resp *pb.AppendEntriesResponse, err error) {
	log.WithFields(log.Fields{"id": r.id, "leaderID": req.LeaderId, "leaderTerm": req.Term}).Info("raft::raft::appendEntries; received grpc request to append entries;")

	r.mu.RLock()
	ct := r.currentTerm
	role := r.role
	r.mu.RUnlock()

	success := req.Term >= ct

	if success {
		r.mu.Lock()
		r.istate.lastAppendOrVoteTime = time.Now()
		r.mu.Unlock()

		if role == candidate || (role == leader && req.Term > ct) {
			r.becomeFollower(req.Term)
		} else if role == follower {
			lrl := r.getLogEntryOrDefault(req.PrevLogIndex)
			if lrl.term == req.PrevLogTerm {
				for idx, rlb := range req.Entries {
					// insert at prevLogIndex + idx (0 based indexing) + 1
					err := r.raftStorage.Set(common.U64ToByte(uint64(idx+1)+req.PrevLogIndex), rlb.Entry, nil)
					if err != nil {
						success = false // ?
						break
					}
					r.setLastLogIndex(uint64(idx+1) + req.PrevLogIndex)
				}
			} else {
				success = false
			}

			r.mu.Lock()
			r.setTerm(req.Term)
			r.mu.Unlock()
		}
	}

	resp = &pb.AppendEntriesResponse{
		Term:        ct,
		Success:     success,
		ResponderId: r.id,
	}

	// update commit index
	r.mu.Lock()
	defer r.mu.Unlock()
	if req.LeaderCommit > r.commitIndex {
		r.commitIndex = common.MinU64(req.LeaderCommit, r.lastLogIndex)
	}

	// trigger application to storage layer
	r.istate.applyCommittedEntriesCh <- struct{}{}

	return resp, err
}

// handleClientSetRequest handles the set request
// NOTE: holds exclusive lock to the raft struct
// returns false if the node is not the leader
func (r *Raft) handleClientSetRequest(key, value []byte, meta bool) (uint64, bool, error) {
	log.WithFields(log.Fields{"id": r.id, "key": string(key), "value": string(value)}).Info("raft::raft::handleClientSetRequest; received set request")

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.role != leader {
		return 0, false, nil
	}

	var rl *raftLog

	if meta {
		log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::handleClientSetRequest; metaset request")
		rl = newMetaSetRaftLog(r.currentTerm, key, value)
	} else {
		log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::handleClientSetRequest; set request")
		rl = newSetRaftLog(r.currentTerm, key, value)
	}

	idx, err := r.submitRaftLog(rl)
	if err != nil {
		return 0, true, err
	}

	log.WithFields(log.Fields{"id": r.id}).Info(fmt.Sprintf("raft::raft::handleClientSetRequest; submitted set log at idx: %d", idx))
	return idx, true, nil
}

// handleClientDeleteRequest handles the delete request
// NOTE: holds exclusive lock to the raft struct
// returns false if the node is not the leader
func (r *Raft) handleClientDeleteRequest(key []byte, meta bool) (uint64, bool, error) {
	log.WithFields(log.Fields{"id": r.id, "key": string(key)}).Info("raft::raft::handleClientDeleteRequest; received delete request")

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.role != leader {
		return 0, false, nil
	}

	var rl *raftLog

	if meta {
		log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::handleClientDeleteRequest; metadelete request")
		rl = newMetaDeleteRaftLog(r.currentTerm, key)
	} else {
		log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::handleClientDeleteRequest; delete request")
		rl = newDeleteRaftLog(r.currentTerm, key)
	}

	idx, err := r.submitRaftLog(rl)
	if err != nil {
		return 0, true, err
	}

	log.WithFields(log.Fields{"id": r.id}).Info(fmt.Sprintf("raft::raft::handleClientDeleteRequest; submitted delete log at idx: %d", idx))
	return idx, true, nil
}

//
// internal functions
//

// submitRaftLog submits the Raft log
// NOTE: Expects exclusive lock to be held on the struct
// returns the index of the log entry / error
func (r *Raft) submitRaftLog(rl *raftLog) (uint64, error) {
	err := r.raftStorage.Set(common.U64ToByte(r.lastLogIndex+1), rl.toBytes(), nil)
	if err != nil {
		log.WithFields(log.Fields{"id": r.id}).Info(fmt.Sprintf("raft::raft::submitRaftLog; error in submitting request to the raft storage at idx: %d. err: %v", r.lastLogIndex+1, err))
		return 0, err
	}
	r.lastLogIndex += 1
	return r.lastLogIndex, nil
}

// isUpToDate returns if the candidate's log is to update in comparison to this node.
// it compares the log of the candidate with this node's log
// for the last entry of the committed log,
// if it's terms are different, we favour the later term.
// in case of tie, we favour the index of the last term.
// for more info check section 5.4.1 last paragraph of the paper.
// NOTE: Don't hold lock while calling this function
func (r *Raft) isUpToDate(req *pb.RequestVoteRequest) bool {
	log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::isUpToDate; starting")

	r.mu.RLock()
	commitIndex := r.commitIndex
	r.mu.RUnlock()

	// commit Index is 0 when the server just starts.
	// In this case, the candidate will always be at least up to date as us.
	if commitIndex > 0 {
		b, err := r.raftStorage.Get(common.U64ToByte(commitIndex), &storage.ReadOptions{})
		if err != nil {
			log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::isUpToDate; return false due to error in getting raft log from storage")
			return false
		}
		rl, err := deserializeRaftLog(b)
		if err != nil {
			log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::isUpToDate; returning false due to error in deserializing raft log")
			return false
		}

		// we decline if the candidate log is not at least up to date as us.
		if rl.term > req.LastLogTerm || commitIndex > req.LastLogIndex {
			log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::isUpToDate; returning false since candidate log is not up to date.")
			return false
		}
	}

	log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::isUpToDate; done. returning true")
	return true
}

// becomeFollower updates the role to follower
// NOTE: Expects lock to be held by the caller
func (r *Raft) becomeFollower(term uint64) {
	log.WithFields(log.Fields{"newTerm": term}).Info("raft::raft::becomeFollower; started")

	r.role = follower
	r.setTerm(term)
	r.setVotedFor(noVote)
	r.istate.lastAppendOrVoteTime = time.Now()

	go r.electionTimerChecker()

	log.Info("raft::raft::becomeFollower; done")
}

// commitEntryRoutine commits the log entries
// Spins a routine so doesn't block.
func (r *Raft) commitEntryRoutine() {
	log.Info("raft::raft::commitEntryRoutine; started")

	go func() {
		for {
			r.mu.RLock()
			role := r.role
			ci := r.commitIndex
			lastLogIndex := r.lastLogIndex
			ct := r.currentTerm
			r.mu.RUnlock()

			if role != leader {
				log.Info(fmt.Sprintf("raft::raft::commitEntryRoutine; no longer a leader. current state: %s", role))
				return
			}

			for idx := ci + 1; idx <= lastLogIndex; idx++ {
				rl := r.getLogEntryOrDefault(idx)
				log.Info(fmt.Sprintf("raft::raft::commitEntryRoutine; trying to commit idx: %d", idx))
				if rl.term == ct {
					cnt := 1

					for id, p := range r.allProgress {
						if r.id != id && p.Match >= idx {
							cnt++
						}
					}

					if isMajiority(cnt, len(r.allProgress)) {
						r.mu.Lock()
						r.commitIndex = idx
						r.mu.Unlock()
						log.Info(fmt.Sprintf("raft::raft::commitEntryRoutine; committed idx: %d", idx))
					}
				}
			}

			time.Sleep(10 * time.Millisecond)
		}
	}()
}

// applyCommittedEntries applies the already committed entries that haven't been applied yet.
// it blocks hence should be a separate routine
// it's always running in the background till the ch is closed.
func (r *Raft) applyCommittedEntriesRoutine() {
	log.Info("raft::raft::applyCommittedEntries; started")

	for range r.istate.applyCommittedEntriesCh {
		r.mu.RLock()
		la := r.lastApplied
		lli := r.commitIndex
		r.mu.RUnlock()

		// we simply reapply all the entries.
		// the paper suggests to check the log entries and only apply those
		// which have a conflicting entry (term) at the same index.
		for idx := la + 1; idx <= lli; idx++ {
			rl := r.getLogEntryOrDefault(idx)
			err := r.s.applyEntry(rl)
			if err != nil {
				log.Error(fmt.Sprintf("raft::raft::applyCommittedEntries; error in applying entry. Err: %v", err.Error()))
				// todo: move to dead state
			}

			r.mu.Lock()
			r.lastApplied = idx
			r.mu.Unlock()
		}
	}

	log.Info("raft::raft::applyCommittedEntries; done. closing...")
}

// sendAppendEntries sends append entry requests to the followers.
// If there is nothing to send, it sends heartbeats.
// NOTE: Don't hold the lock while calling this
func (r *Raft) sendAppendEntries() {
	log.Info("raft::raft::sendAppendEntries; sending append entry requests to followers")

	r.mu.RLock()
	lastLogIndex := r.lastLogIndex
	currentTerm := r.currentTerm
	leaderCommit := r.commitIndex
	r.mu.RUnlock()

	for i := range r.allProgress {
		if i != r.id {
			go func(receiverID uint64) {
				// we have to send entries from index p.Next to r.lastLogIndex
				p := r.allProgress[receiverID]
				lastRl := r.getLogEntryOrDefault(p.Next - 1)
				prevLogIndex := p.Next - 1
				prevLogTerm := lastRl.term

				var entries []*pb.LogEntry
				lim := lastLogIndex
				for i := p.Next; i <= lim; i++ {
					rl := r.getLogEntryOrDefault(i)
					entry := &pb.LogEntry{
						Entry: rl.toBytes(),
					}
					entries = append(entries, entry)
				}

				log.WithFields(log.Fields{"id": r.id, "p.Next": p.Next, "lim": lim}).Info(fmt.Sprintf("raft::raft::sendAppendEntries; Number of append entries to peer %d: %d", receiverID, len(entries)))

				req := &pb.AppendEntriesRequest{
					Term:         currentTerm,
					LeaderId:     r.id,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					LeaderCommit: leaderCommit,
					Entries:      entries,
				}
				resp, err := r.s.sendAppendEntries(receiverID, req)

				if err != nil {
					log.Error(fmt.Sprintf("raft::raft::sendAppendEntries; error in send append entries %v", err))
					return
				}

				if resp.Term > currentTerm {
					r.setTerm(resp.Term)
					r.becomeFollower(resp.Term)
					log.Info(fmt.Sprintf("raft::raft::sendAppendEntries; append entry response term is higher. becoming follower"))
				}

				if resp.Success {
					p.Next = r.lastLogIndex + 1
					p.Match = r.lastLogIndex
				} else {
					log.Info(fmt.Sprintf("raft::raft::sendAppendEntries; response from %d indicates failure. retrying with decremented next index", resp.ResponderId))
					p.Next--
				}
			}(i)
		}
	}
}

// initializeLeaderVolatileState inits the volatile state for the leader
// NOTE: Expects the lock to be held while calling this
func (r *Raft) initializeLeaderVolatileState() {
	for id, prog := range r.allProgress {
		if id != r.id {
			prog.Next = r.lastLogIndex + 1
			prog.Match = 0
		}
	}
}

// startLeader sets role to leader and initiates periodic append entry requests
// It also initiates the commit entry routine for committing log entries
// NOTE: Expects the lock to be held while calling this
func (r *Raft) startLeader() {
	r.role = leader
	r.initializeLeaderVolatileState()
	log.Info(fmt.Sprintf("raft::raft::startLeader; Became a leader with term: %d", r.currentTerm))

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			r.sendAppendEntries()
			<-ticker.C

			r.mu.Lock()

			// It could be because one of the responses to the append entry could have a higher term
			// which would mean this node would be a follower now.
			if r.role != leader {
				r.mu.Unlock()
				return
			}

			r.mu.Unlock()
		}
	}()

	go r.commitEntryRoutine()
}

// startElection starts a leader election
// it changes the role to a candidate and sends vote requests
// NOTE: Expects the lock to be held while calling this
func (r *Raft) startElection() {
	log.Info("raft::raft::startElection; started")
	r.role = candidate
	r.currentTerm += 1
	r.setVotedFor(r.id)
	rl := r.getLogEntryOrDefault(r.lastLogIndex)

	var count common.ProtectedUint64
	count.Set(1)

	term := r.currentTerm
	lastLogIndex := r.lastLogIndex

	for i := range r.allProgress {
		if i != r.id {
			go func(id uint64) {
				req := &pb.RequestVoteRequest{
					Term:         term,
					CandidateId:  r.id,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  rl.term,
				}

				resp, err := r.s.sendRequestVote(id, req)
				if err != nil {
					log.Error(fmt.Sprintf("raft::raft::startElection; error response to the vote request: %v", err))
					return
				}
				log.Info(fmt.Sprintf("raft::raft::startElection; Received vote response from %d, granted: %v", resp.VoterId, resp.VoteGranted))

				r.mu.Lock()
				defer r.mu.Unlock()

				if r.role != candidate {
					log.Info(fmt.Sprintf("raft::raft::startElection; No longer a candidate after receiving vote response. state: %s", r.role))
					return
				}

				if resp.Term > term {
					log.Info(fmt.Sprintf("raft::raft::startElection; resp.Term (%d) is higher than saved term (%d). becoming follower..", resp.Term, term))
					r.becomeFollower(resp.Term)
					return
				} else {
					if resp.VoteGranted {
						count.Increment()
						if int(count.Get())*2 > len(r.allProgress) {
							log.Info("raft::raft::startElection; became leader")
							r.startLeader()
							return
						}
					}
				}

			}(i)
		}
	}

	// run checker to run another election if this one fails/times out
	// if election was successful then it's okay since the checker returns if the role is leader/dead
	go r.electionTimerChecker()
}

// electionTimerChecker checks for election timeout and triggers it when required
func (r *Raft) electionTimerChecker() {
	ts := getElectionTimeout()
	r.mu.RLock()
	term := r.currentTerm
	r.mu.RUnlock()
	log.Info(fmt.Sprintf("raft::raft::electionTimerChecker; started. timeout = %d term = %d", ts, term))

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		<-ticker.C

		r.mu.Lock()

		// no need for election timer in leader/dead state
		if r.role != candidate && r.role != follower {
			log.Info(fmt.Sprintf("raft::raft::electionTimerChecker; no longer a candidate/follower. state = %s term = %d", r.role, term))
			r.mu.Unlock()
			return
		}

		// if the term has increased. this can happen in the following conditions:
		// a. if the follower receives an append entry message with a new term
		// b. if a candidate receives an append entry and moves back to follower state
		if r.currentTerm != term {
			log.Info(fmt.Sprintf("raft::raft::electionTimerChecker; current term doesn't match. exiting. current term = %s term = %d", r.currentTerm, term))
			r.mu.Unlock()
			return
		}

		if delta := time.Since(r.istate.lastAppendOrVoteTime); delta >= ts {
			log.Info("raft::raft::electionTimerChecker; election timeout triggered. starting election")
			r.startElection()
			r.mu.Unlock()
			return
		}

		r.mu.Unlock()
	}
}

// NewRaft initializes a new raft state machine.
func NewRaft(kvConfig *common.KVConfig, raftStorage *storage.Storage, s *Server, ready <-chan interface{}) *Raft {
	log.Info("raft::raft::NewRaft; started")
	r := &Raft{
		id:          kvConfig.ID,
		raftStorage: raftStorage,
		allProgress: initProgress(kvConfig.ID, kvConfig.Peers),
		istate: &internalState{
			clientRequests:       make(chan interface{}),
			clientResponses:      make(chan bool),
			lastAppendOrVoteTime: time.Now(),
		},
		kvConfig:     kvConfig,
		s:            s,
		maxRaftState: -1,
		mu:           new(sync.RWMutex),
	}

	// volatile state
	r.commitIndex = 0
	r.lastApplied = 0
	r.role = follower

	// persistent state
	r.currentTerm = r.getRaftMetaVal(termKey)
	r.votedFor = r.getRaftMetaVal(votedForKey)
	r.lastLogIndex = r.getRaftMetaVal(lastLogIndexKey)

	// todo: apply the logs to the state machine.

	// init election timer and apply committed entry routines
	go func() {
		<-ready

		r.mu.Lock()
		r.istate.lastAppendOrVoteTime = time.Now()
		r.mu.Unlock()
		r.electionTimerChecker()
	}()
	go r.applyCommittedEntriesRoutine()

	log.Info("raft::raft::NewRaft; done")
	return r
}

//
// utilities
//

// setTerm sets the current term.
// NOTE: expects caller to hold lock
func (r *Raft) setTerm(term uint64) {
	r.currentTerm = term
	err := r.raftStorage.Set(termKey, common.U64ToByte(term), &storage.WriteOptions{Sync: true})
	if err != nil {
		log.Error(fmt.Sprintf("raft::raft::setTerm; error during persisting current term %v", err.Error()))
	}
}

// setVotedFor sets the voted for
// NOTE: expects caller to hold lock
func (r *Raft) setVotedFor(id uint64) {
	r.votedFor = id
	err := r.raftStorage.Set(votedForKey, common.U64ToByte(id), &storage.WriteOptions{Sync: true})
	if err != nil {
		log.Error(fmt.Sprintf("raft::raft::setVotedFor; error during persisting votedFor %v", err.Error()))
	}
}

// setLastLogIndex sets the last log index
// NOTE: expects caller to hold lock
func (r *Raft) setLastLogIndex(index uint64) {
	r.lastLogIndex = index
	err := r.raftStorage.Set(lastLogIndexKey, common.U64ToByte(index), &storage.WriteOptions{Sync: true})
	if err != nil {
		log.Error(fmt.Sprintf("raft::raft::setLastLogIndex; error during persisting last log index %v", err.Error()))
	}
}

// getRaftMetaVal gets the value of raft meta value persisted on the disk
// in case of error, it assumes that this is the first boot, hence returns 0
func (r *Raft) getRaftMetaVal(key []byte) uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	val, err := r.raftStorage.Get(key, nil)
	if err != nil {
		return 0
	}

	return common.ByteToU64(val)
}

// getLogEntryOrDefault returns the raft log at given index or dummy.
func (r *Raft) getLogEntryOrDefault(idx uint64) *raftLog {
	log.Info(fmt.Sprintf("raft::raft::getLogEntryOrDefault; getting log entry with idx: %d", idx))
	rl := &raftLog{
		term: 0,
	}

	if idx > 0 {
		b, err := r.raftStorage.Get(common.U64ToByte(idx), &storage.ReadOptions{})
		if err != nil {
			log.Error(fmt.Sprintf("raft::raft::getLogEntryOrDefault; error in fetching log entry: %v", err))
		} else {
			rl, err = deserializeRaftLog(b)
			if err != nil {
				log.Error(fmt.Sprintf("raft::raft::getLogEntryOrDefault; error in deserializing log entry: %v", err))
			}
		}
	}

	return rl
}

// initProgress inits the progress map.
// This is temporary and in case this node becomes the leader, it'll be
// reupdated in the initializeLeaderVolatileState function
func initProgress(id uint64, peers []common.Peer) map[uint64]*Progress {
	log.Info("raft::raft::initProgress; started")
	m := make(map[uint64]*Progress)

	m[id] = &Progress{
		Match: 0,
		Next:  1,
	}

	for _, p := range peers {
		m[p.ID] = &Progress{
			Match: 0,
			Next:  1,
		}
	}

	log.Info("raft::raft::initProgress; started")
	return m
}

func (r *Raft) close() {
	// close channels etc.

	// close raft storage
	r.raftStorage.Close()
}
