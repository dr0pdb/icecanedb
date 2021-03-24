package raft

import (
	"context"
	"fmt"
	"sync"
	"time"

	common "github.com/dr0pdb/icecanedb/pkg/common"
	pb "github.com/dr0pdb/icecanedb/pkg/protogen"
	"github.com/dr0pdb/icecanedb/pkg/storage"
	log "github.com/sirupsen/logrus"
)

const (
	// MinElectionTimeout is the min duration for which a follower waits before becoming a candidate
	MinElectionTimeout = 2000 * time.Millisecond
	// MaxElectionTimeout is the max duration for which a follower waits before becoming a candidate
	MaxElectionTimeout = 3000 * time.Millisecond

	requestTimeoutInterval = 1 * time.Second
)

// persistent state of the node
var (
	termKey         = []byte("termKey")
	votedForKey     = []byte("votedFor")
	lastLogIndexKey = []byte("lastLogIndex")
)

const (
	leader uint64 = iota
	follower
	candidate
	norole
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
	// IMP: index starts from 1.
	raftStorage *storage.Storage

	// lastLogIndex contains the index of the highest log entry stored in raftStorage.
	// commitIndex <= lastLogIndex
	lastLogIndex common.ProtectedUint64

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

	// snapshot if log size exceeds it. -1 indicates no snapshotting
	maxRaftState int64

	mu *sync.RWMutex
}

type internalState struct {
	// grpc requests from the client
	clientRequests  chan interface{}
	clientResponses chan bool

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
	endLeader   chan bool

	followerRunning, candRunning, leaderRunning common.ProtectedBool
}

//
// grpc server calls
//

// getLeaderID returns the id of the node which this node thinks is the leader.
func (r *Raft) getLeaderID() uint64 {
	if r.role.Get() == leader {
		return r.id
	}

	return r.istate.currentLeader.Get()
}

func (r *Raft) clientSetRequest(key, value []byte, meta bool) error {
	log.WithFields(log.Fields{"id": r.id, "key": string(key), "value": string(value)}).Info("raft::raft::clientSetRequest; received set request")
	sr := &setRequest{
		key:   key,
		value: value,
		meta:  meta,
	}

	r.istate.clientRequests <- sr
	resp := <-r.istate.clientResponses

	if !resp {
		log.WithFields(log.Fields{"id": r.id}).Error("raft::raft::clientSetRequest; error")
		return fmt.Errorf("set request failed")
	}

	return nil
}

func (r *Raft) clientDeleteRequest(key []byte, meta bool) error {
	log.WithFields(log.Fields{"id": r.id, "key": string(key)}).Info("raft::raft::clientDeleteRequest; received delete request")
	dr := &deleteRequest{
		key:  key,
		meta: meta,
	}

	r.istate.clientRequests <- dr
	resp := <-r.istate.clientResponses

	if !resp {
		log.WithFields(log.Fields{"id": r.id}).Error("raft::raft::clientDeleteRequest; error")
		return fmt.Errorf("delete request failed")
	}

	return nil
}

// RequestVote is used by the raft candidate to request for votes.
// The current node has received a request to vote by another peer.
func (r *Raft) requestVote(ctx context.Context, request *pb.RequestVoteRequest) (resp *pb.RequestVoteResponse, err error) {
	log.WithFields(log.Fields{"id": r.id, "candidateId": request.CandidateId, "candidateTerm": request.Term}).Info("raft::raft::requestVote; received grpc request to vote;")

	r.istate.requestVoteRequests <- request
	voteGranted := <-r.istate.requestVoteResponses

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
	resp = <-r.istate.appendEntriesReponses

	return resp, err
}

//
// internal functions
//

func (r *Raft) sendRequestVote(rl *raftLog, voterID uint64) (*pb.RequestVoteResponse, error) {
	log.WithFields(log.Fields{"id": r.id}).Info(fmt.Sprintf("raft::raft::sendRequestVote; sending vote request to peer %d", voterID))
	req := &pb.RequestVoteRequest{
		Term:         r.currentTerm.Get(),
		CandidateId:  r.id,
		LastLogIndex: r.commitIndex.Get(),
		LastLogTerm:  rl.term,
	}
	return r.s.sendRequestVote(voterID, req)
}

// sendAppendEntries sends append entries to the given receiver using the info stored in r.allProgress
// also acts as heartbeat.
func (r *Raft) sendAppendEntries(receiverID uint64) (*pb.AppendEntriesResponse, error) {
	log.WithFields(log.Fields{"id": r.id}).Info(fmt.Sprintf("raft::raft::sendAppendEntries; sending append entries to peer %d", receiverID))

	// we have to send entries from index p.Next to r.lastLogIndex
	p := r.allProgress[receiverID]
	lastRl := r.getLogEntryOrDefault(p.Next - 1)
	prevLogIndex := p.Next - 1
	prevLogTerm := lastRl.term

	var entries []*pb.LogEntry
	lim := r.lastLogIndex.Get()
	for i := p.Next; i <= lim; i++ {
		rl := r.getLogEntryOrDefault(i)
		entry := &pb.LogEntry{
			Entry: rl.toBytes(),
		}
		entries = append(entries, entry)
	}

	log.WithFields(log.Fields{"id": r.id, "p.Next": p.Next, "lim": lim}).Info(fmt.Sprintf("raft::raft::sendAppendEntries; Number of append entries to peer %d: %d", receiverID, len(entries)))

	req := &pb.AppendEntriesRequest{
		Term:         r.currentTerm.Get(),
		LeaderId:     r.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: r.commitIndex.Get(),
		Entries:      entries,
	}
	return r.s.sendAppendEntries(receiverID, req)
}

// follower does the role of a raft follower.
func (r *Raft) follower() {
	log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::followerroutine; became follower;")

	r.istate.followerRunning.Set(true)
	defer r.istate.followerRunning.Set(false)

	for {
		select {
		case <-r.istate.endFollower:
			log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::followerroutine; received on endFollower chan")
			goto end

		case req := <-r.istate.requestVoteRequests:
			log.WithFields(log.Fields{"id": r.id, "candidate": req.CandidateId, "candidateTerm": req.Term, "currentTerm": r.currentTerm.Get(), "votedFor": r.votedFor.Get()}).Info("raft::raft::followerroutine; request for vote received")
			ct := r.currentTerm.Get()

			if (req.Term > ct || (req.Term == ct && r.votedFor.Get() == noVote)) && r.isUpToDate(req) {
				log.WithFields(log.Fields{"id": r.id, "candidate": req.CandidateId}).Info("raft::raft::followerroutine; vote yes")
				r.istate.lastAppendOrVoteTime = time.Now()
				r.setTerm(req.Term)
				r.votedFor.Set(req.CandidateId)
				r.istate.requestVoteResponses <- true
			} else {
				log.WithFields(log.Fields{"id": r.id, "candidate": req.CandidateId}).Info("raft::raft::followerroutine; vote no")
				r.istate.requestVoteResponses <- false
			}

		case req := <-r.istate.appendEntriesRequests:
			log.WithFields(log.Fields{"id": r.id, "leader": req.LeaderId, "leaderTerm": req.Term}).Info("raft::raft::followerroutine; leader append request received")
			r.istate.lastAppendOrVoteTime = time.Now()

			ct := r.currentTerm.Get()
			success := req.Term >= ct

			if success {
				lrl := r.getLogEntryOrDefault(req.PrevLogIndex)
				if lrl.term == req.PrevLogTerm {
					for idx, rlb := range req.Entries {
						// insert at prevLogIndex + idx (0 based indexing) + 1
						err := r.raftStorage.Set(common.U64ToByte(uint64(idx+1)+req.PrevLogIndex), rlb.Entry, nil)
						if err != nil {
							success = false // ?
							break
						}
						r.lastLogIndex.Set(uint64(idx+1) + req.PrevLogIndex)
					}
				} else {
					success = false
				}

				r.istate.currentLeader.Set(req.LeaderId)
				r.setTerm(req.Term)
			}

			resp := &pb.AppendEntriesResponse{
				Term:        ct,
				Success:     success,
				ResponderId: r.id,
			}

			r.istate.appendEntriesReponses <- resp

			// update commit index
			if req.LeaderCommit > r.commitIndex.Get() {
				r.commitIndex.Set(common.MinU64(req.LeaderCommit, r.lastLogIndex.Get()))
			}

			err := r.applyCommittedEntries()
			if err != nil {
				log.WithFields(log.Fields{"id": r.id, "leader": req.LeaderId, "leaderTerm": req.Term}).Error("raft::raft::followerroutine; error in applying log entry")
			}
		}
	}

end:
	log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::followerroutine; follower routine ending;")
}

// isUpToDate returns if the candidate's log is to update in comparison to this node.
// it compares the log of the candidate with this node's log
// for the last entry of the committed log,
// if it's terms are different, we favour the later term.
// in case of tie, we favour the index of the last term.
// for more info check section 5.4.1 last paragraph of the paper.
func (r *Raft) isUpToDate(req *pb.RequestVoteRequest) bool {
	log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::isUpToDate; starting")

	// commit Index is 0 when the server just starts.
	// In this case, the candidate will always be at least up to date as us.
	if r.commitIndex.Get() > 0 {
		b, err := r.raftStorage.Get(common.U64ToByte(r.commitIndex.Get()), &storage.ReadOptions{})
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
		if rl.term > req.LastLogTerm || r.commitIndex.Get() > req.LastLogIndex {
			log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::isUpToDate; returning false since candidate log is not up to date.")
			return false
		}
	}

	log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::isUpToDate; done. returning true")
	return true
}

func (r *Raft) candidate() {
	log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::candidateroutine; became candidate;")
	r.istate.candRunning.Set(true)
	defer r.istate.candRunning.Set(false)
	for {
		log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::candidateroutine; requesting votes")
		cnt := 1 // counting self votes
		voteRecCh := make(chan *pb.RequestVoteResponse, len(r.allProgress))

		r.currentTerm.Increment()
		r.votedFor.Set(r.id)
		rl := r.getLogEntryOrDefault(r.lastLogIndex.Get())

		for i := range r.allProgress {
			if i != r.id {
				go func(id uint64) {
					resp, err := r.sendRequestVote(rl, id)
					if err != nil {
						log.Error(fmt.Sprintf("raft::raft::candidateroutine; error response to the vote request: %v", err))
						return
					}

					// this is required since the voteRecCh channel could be closed due to timeout.
					// TODO: find a better way if possible?
					defer func() {
						if rec := recover(); rec != nil {
							log.Warn(fmt.Sprintf("raft::raft::candidateroutine; send request vote routine failed with: %v", rec))
						}
					}()
					voteRecCh <- resp
				}(i)
			}
		}

		t := time.After(getElectionTimeout())

		for {
			select {
			case req := <-r.istate.appendEntriesRequests:
				log.WithFields(log.Fields{"id": r.id}).Info(fmt.Sprintf("raft::raft::candidateroutine; received append request from %d", req.LeaderId))

				if req.Term >= r.currentTerm.Get() {
					log.Info(fmt.Sprintf("raft::raft::candidateroutine; request from %d is up to date. becoming follower", req.LeaderId))
					r.istate.lastAppendOrVoteTime = time.Now()
					r.setTerm(req.Term)
					r.becomeFollower(candidate)
					goto end
				} else {
					log.Info(fmt.Sprintf("raft::raft::candidateroutine; request from %d is stale. ignoring", req.LeaderId))
				}

			case resp := <-voteRecCh:
				log.WithFields(log.Fields{"id": r.id, "voterId": resp.VoterId, "granted": resp.VoteGranted}).Info(fmt.Sprintf("raft::raft::candidateroutine; received vote from %d", resp.VoterId))
				if resp.Term > r.currentTerm.Get() {
					log.WithFields(log.Fields{"id": r.id, "voterId": resp.VoterId}).Info(fmt.Sprintf("raft::raft::candidateroutine; vote from %d has higher term. becoming follower", resp.VoterId))
					r.istate.lastAppendOrVoteTime = time.Now()
					r.setTerm(resp.Term)
					r.becomeFollower(candidate)
					goto end
				}

				if resp.VoteGranted {
					cnt++
				}

				// become a leader as soon as you get majiority
				if isMajiority(cnt, len(r.allProgress)) {
					log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::candidateroutine; received majiority of votes. becoming leader")
					r.becomeLeader(candidate)
					goto end
				}

			case <-t:
				log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::candidateroutine; leader election timed out.")
				close(voteRecCh)
				goto restart
			}
		}

	restart:
		log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::candidateroutine; didn't receive majiority votes; restarting...")
	}

end:
	log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::candidateroutine; ending;")
}

func (r *Raft) leader() {
	log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::leaderroutine; became leader;")
	r.istate.leaderRunning.Set(true)
	defer r.istate.leaderRunning.Set(false)

	r.initializeLeaderVolatileState()

	for {
		select {
		case <-r.istate.endLeader:
			log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::leaderroutine; received on endLeader chan")
			goto end

		case req := <-r.istate.clientRequests:
			log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::leaderroutine; received a client request")
			var rl *raftLog

			switch request := req.(type) {
			case *setRequest:
				if !request.meta {
					log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::leaderroutine; set request")
					rl = newSetRaftLog(r.currentTerm.Get(), request.key, request.value)
				} else {
					log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::leaderroutine; metaset request")
					rl = newMetaSetRaftLog(r.currentTerm.Get(), request.key, request.value)
				}
			case *deleteRequest:
				if !request.meta {
					log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::leaderroutine; delete request")
					rl = newDeleteRaftLog(r.currentTerm.Get(), request.key)
				} else {
					log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::leaderroutine;metadelete request")
					rl = newMetaDeleteRaftLog(r.currentTerm.Get(), request.key)
				}
			default:
				log.WithFields(log.Fields{"id": r.id}).Error("raft::raft::leaderroutine; invalid request")

				// TODO: handle
			}

			lastIdx := r.lastLogIndex.Get()
			err := r.raftStorage.Set(common.U64ToByte(lastIdx+1), rl.toBytes(), nil)
			if err != nil {
				// handle error
			}
			r.lastLogIndex.Increment()

			// wait for the log to get replicated on a majiority by the append entry routine.
			for {
				if r.commitIndex.Get() >= r.lastLogIndex.Get() {
					break
				}
				log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::leaderroutine; waiting for log to get replicated...")
				time.Sleep(MinElectionTimeout / 5)
			}

			r.istate.clientResponses <- true

			err = r.applyCommittedEntries()
			if err != nil {
				// handle error in application.
			}
		}
	}

end:
	log.WithFields(log.Fields{"id": r.id}).Info("raft::raft::leaderroutine; ending;")
}

func (r *Raft) initializeLeaderVolatileState() {
	for id, prog := range r.allProgress {
		if id != r.id {
			prog.Next = r.lastLogIndex.Get() + 1
			prog.Match = 0
		}
	}
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

// applyCommittedEntries applies the already committed entries that haven't been applied yet.
func (r *Raft) applyCommittedEntries() error {
	log.Info("raft::raft::applyCommittedEntries; started")

	la := r.lastApplied.Get()
	lli := r.commitIndex.Get()

	// we simply reapply all the entries.
	// the paper suggests to check the log entries and only apply those
	// which have a conflicting entry (term) at the same index.
	for idx := la + 1; idx <= lli; idx++ {
		rl := r.getLogEntryOrDefault(idx)
		err := r.s.applyEntry(rl)
		if err != nil {
			log.Error(fmt.Sprintf("raft::raft::applyCommittedEntries; error in applying entry. Err: %v", err.Error()))
			return err
		}

		r.lastApplied.Set(idx)
	}

	log.Info("raft::raft::applyCommittedEntries; done")
	return nil
}

// setTerm sets the current term.
// every update to the current term should be done via this function.
func (r *Raft) setTerm(term uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	old := r.currentTerm.SetIfGreater(term)

	// term was updated successfully, so persist it.
	if old < term {
		r.raftStorage.Set(termKey, common.U64ToByte(term), &storage.WriteOptions{Sync: true})
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

// becomeFollower updates the role to follower and blocks untill the cand and leader routines stop.
// this is done to ensure that we only have one routine running at a time.
// src - the current role from which this method has been called.
// eg: if src = candidate, then it means that this method has been called from the candidate routine.
// In this case we can expect the candidate routine to clean up itself and hence we don't wait for it to end.
func (r *Raft) becomeFollower(src uint64) {
	log.Info("raft::raft::becomeFollower; started")

	r.role.Set(follower)
	for {
		if (src == candidate || !r.istate.candRunning.Get()) && (src == leader || !r.istate.leaderRunning.Get()) {
			break
		}
	}

	log.Info("raft::raft::becomeFollower; done")
}

// becomeCandidate updates the role to follower and blocks untill the follower and leader routines stop.
// this is done to ensure that we only have one routine running at a time.
func (r *Raft) becomeCandidate(src uint64) {
	log.Info("raft::raft::becomeCandidate; started")

	if r.istate.followerRunning.Get() {
		r.istate.endFollower <- true
	}
	r.votedFor.Set(noVote)
	r.role.Set(candidate)
	for {
		if (src == follower || !r.istate.followerRunning.Get()) && (src == leader && !r.istate.leaderRunning.Get()) {
			break
		}
	}

	log.Info("raft::raft::becomeCandidate; done")
}

// becomeLeader updates the role to follower and blocks untill the cand and follower routines stop.
// this is done to ensure that we only have one routine running at a time.
func (r *Raft) becomeLeader(src uint64) {
	log.Info("raft::raft::becomeLeader; started")

	if r.istate.followerRunning.Get() {
		r.istate.endFollower <- true
	}
	r.role.Set(leader)
	for {
		if (src == candidate || !r.istate.candRunning.Get()) && (src == follower || !r.istate.followerRunning.Get()) {
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
			if r.role.Get() == follower {
				ts := getElectionTimeout()
				if time.Now().Sub(r.istate.lastAppendOrVoteTime) > ts {
					log.Info("raft::raft::electionTimerRoutine; election timeout triggered; sending end signal to follower")
					r.becomeCandidate(norole)
				}
			}

			time.Sleep(1000 * time.Millisecond)
		}
	}()

	log.Info("raft::raft::electionTimerRoutine; done")
}

// appendEntryRoutine sends append entry requests to the followers.
// If there is nothing to send, it sends heartbeats.
func (r *Raft) appendEntryRoutine() {
	log.Info("raft::raft::appendEntryRoutine; started")

	go func() {
		for {
			if r.role.Get() == leader {
				ch := make(chan *pb.AppendEntriesResponse, len(r.allProgress))

				for i := range r.allProgress {
					if i != r.id {
						go func(id uint64) {
							resp, err := r.sendAppendEntries(id)
							if err != nil {
								log.Error(fmt.Sprintf("raft::raft::appendEntryRoutine; error in send append entries %v", err))
								return
							}

							// this is required since the ch channel could be closed due to timeout.
							// TODO: find a better way if possible?
							defer func() {
								if rec := recover(); rec != nil {
									log.Warn(fmt.Sprintf("raft::raft::appendEntryRoutine; send append entries routine failed with: %v", rec))
								}
							}()

							ch <- resp
						}(i)
					}
				}

				t := time.After(MinElectionTimeout / 3)
				cnt := 1

				for {
					select {
					case resp := <-ch:
						if resp.Term > r.currentTerm.Get() {
							r.setTerm(resp.Term)
							r.istate.currentLeader.Set(resp.ResponderId)
							r.becomeFollower(norole)
							goto out
						}

						p := r.allProgress[resp.ResponderId]
						if resp.Success {
							p.Next = r.lastLogIndex.Get() + 1
							p.Match = r.lastLogIndex.Get()
						} else {
							p.Next-- // retry with decremented next index
						}

						cnt++
						if cnt == len(r.allProgress) {
							log.Info("raft::raft::appendEntryRoutine; received response from everyone.")
							goto out
						}

					case <-t:
						log.Warn("raft::raft::appendEntryRoutine; timed out in sending append entries.")
						close(ch)
						goto out
					}
				}
			}

		out:
			time.Sleep(MinElectionTimeout / 4)
		}
	}()

	log.Info("raft::raft::appendEntryRoutine; done")
}

func (r *Raft) commitEntryRoutine() {
	log.Info("raft::raft::commitEntryRoutine; started")

	go func() {
		for {
			if r.role.Get() == leader {
				ci := r.commitIndex.Get()
				for idx := ci + 1; idx <= r.lastLogIndex.Get(); idx++ {
					rl := r.getLogEntryOrDefault(idx)
					log.Info(fmt.Sprintf("raft::raft::commitEntryRoutine; trying to commit idx: %d", idx))
					if rl.term == r.currentTerm.Get() {
						cnt := 1

						for id, p := range r.allProgress {
							if r.id != id && p.Match >= idx {
								cnt++
							}
						}

						if isMajiority(cnt, len(r.allProgress)) {
							r.commitIndex.Set(idx)
							log.Info(fmt.Sprintf("raft::raft::commitEntryRoutine; committed idx: %d", idx))
						}
					}
				}
			}

			time.Sleep(MinElectionTimeout)
		}
	}()

	log.Info("raft::raft::commitEntryRoutine; done")
}

func (r *Raft) init() {
	log.Info("raft::raft::init; started")

	r.electionTimerRoutine()
	r.appendEntryRoutine()
	r.commitEntryRoutine()

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

func (r *Raft) close() {
	// close channels etc.

	// close raft storage
	r.raftStorage.Close()
}

// NewRaft initializes a new raft state machine.
func NewRaft(kvConfig *common.KVConfig, raftStorage *storage.Storage, s *Server) *Raft {
	log.Info("raft::raft::NewRaft; started")
	r := &Raft{
		id:          kvConfig.ID,
		raftStorage: raftStorage,
		allProgress: initProgress(kvConfig.ID, kvConfig.Peers),
		istate: &internalState{
			clientRequests:        make(chan interface{}),
			clientResponses:       make(chan bool),
			appendEntriesRequests: make(chan *pb.AppendEntriesRequest),
			appendEntriesReponses: make(chan *pb.AppendEntriesResponse),
			requestVoteRequests:   make(chan *pb.RequestVoteRequest),
			requestVoteResponses:  make(chan bool),
			endFollower:           make(chan bool),
			endLeader:             make(chan bool),
			lastAppendOrVoteTime:  time.Now(),
		},
		kvConfig:     kvConfig,
		s:            s,
		maxRaftState: -1,
		mu:           new(sync.RWMutex),
	}

	// persistent state
	r.currentTerm.Set(r.getRaftMetaVal(termKey))
	r.votedFor.Set(r.getRaftMetaVal(votedForKey))
	r.lastLogIndex.Set(r.getRaftMetaVal(lastLogIndexKey))

	// volatile state
	r.commitIndex.Set(0)
	r.lastApplied.Set(0)
	r.istate.currentLeader.Set(noLeader)
	r.role.Set(follower)

	r.init()
	log.Info("raft::raft::NewRaft; done")
	return r
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
