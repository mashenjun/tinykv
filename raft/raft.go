// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"math"
	"math/rand"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

func (progress *Progress) String() string {
	return fmt.Sprintf("match:%+v, next:%+v", progress.Match, progress.Next)
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int64
	// baseline of election interval
	electionTimeout int64
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int64
	// number of ticks since it reached last electionTimeout
	electionElapsed int64

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// customized field
	peers []uint64

	dice *rand.Rand

	prevLead  uint64
	prevState StateType

	prevHardState pb.HardState
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := Raft{
		id:               c.ID,
		RaftLog:          newLog(c.Storage),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             make([]pb.Message, 0),
		heartbeatTimeout: int64(c.HeartbeatTick),
		electionTimeout:  int64(c.ElectionTick),
		leadTransferee:   0,
		PendingConfIndex: 0,
		peers:            c.peers,
		Prs:              make(map[uint64]*Progress),
		dice:             rand.New(rand.NewSource(int64(c.ID))),
	}
	r.RaftLog.DebugID = c.ID
	lastLogIdx := r.RaftLog.LastIndex()

	// todo refactor
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	// if c.peer is empty, use confState to set the peers
	if len(r.peers) == 0 && len(cs.GetNodes()) > 0 {
		r.peers = cs.GetNodes()
	}

	r.Term = hs.Term
	// todo RaftLog.committed -> log position
	// todo hs.Commit -> log index
	if hs.Vote != 0 && hs.Vote != r.id {
		r.Vote = hs.Vote
		r.votes[r.Vote] = true
		r.Lead = hs.Vote
	}
	if c.Applied != 0 {
		r.RaftLog.applied = c.Applied
	}

	// todo according to the paper, nextIndex = lastLogIndex + 1
	for _, p := range r.peers {
		r.Prs[p] = &Progress{
			Match: 0,
			Next:  lastLogIdx + 1,
		}
	}

	r.resetElectionTick()
	return &r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).

	// determine MessageType_MsgAppend or MessageType_MsgSnapshot
	var err error
	// first, err := r.RaftLog.storage.FirstIndex()
	// if err == nil && r.Prs[to].Next < first {
	// 	sendSP = true
	// }
	snapshot, sendSP := r.shouldSendSnapshot(to)
	if sendSP {
		// snapshot, err := r.RaftLog.storage.Snapshot()
		// if err != nil {
		// 	log.Errorf("[P%+v] snapshot err:%+v", r.id, err)
		// 	return false
		// }
		msg := pb.Message{
			MsgType:  pb.MessageType_MsgSnapshot,
			To:       to,
			From:     r.id,
			Term:     r.Term,
			Snapshot: &snapshot,
		}
		r.msgs = append(r.msgs, msg)
		return true
	}

	// calculate the term for prev log term
	var prevTerm uint64 = 0
	var preIdx uint64 = 0
	if r.Prs[to].Next > 0 {
		preIdx = r.Prs[to].Next - 1
		prevTerm, err = r.RaftLog.Term(preIdx)
		if err != nil {
			return false
		}
	}

	// prepare the entries need to append
	ents := make([]*pb.Entry, 0)
	// calculate the log position, Next is the next index we want to send,
	// the associated entry's log position is

	if r.Prs[to].Next <= r.RaftLog.stabled {
		sEnts, err := r.RaftLog.storage.Entries(r.Prs[to].Next, r.RaftLog.stabled+1)
		if err != nil {
			log.Errorf("[P%+v] [%v:%v] to peer %+v:%+v, sendSP:%+v err:%+v", r.id, r.Prs[to].Next, r.RaftLog.stabled+1, to, r.Prs[to], sendSP, err)
			panic(err)
		}
		for i, _ := range sEnts {
			ents = append(ents, &sEnts[i])
		}
		pos := r.RaftLog.findPosition(r.RaftLog.stabled + 1)
		if pos >= 0 {
			for i := pos; i < len(r.RaftLog.entries); i++ {
				ents = append(ents, &r.RaftLog.entries[i])
			}
		}
	} else {
		pos := r.RaftLog.findPosition(r.Prs[to].Next)
		if pos >= 0 {
			for i := pos; i < len(r.RaftLog.entries); i++ {
				ents = append(ents, &r.RaftLog.entries[i])
			}
		}
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevTerm, // prevLogTerm
		Index:   preIdx,   // prevLogIndex
		Entries: ents,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg, err := r.getMsgHeartBeat(to)
	if err != nil {
		panic("must panic")
	}
	r.msgs = append(r.msgs, *msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	// only leader node will send heartbeat and only none leader node start election
	if r.State != StateLeader {
		r.electionElapsed++
	} else {
		r.heartbeatElapsed++
	}

	if r.electionElapsed >= r.electionTimeout {
		// time to start a new election
		r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgHup})
	}
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.broadcastHeartBeat()
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Vote = lead
	r.Lead = lead
	r.leadTransferee = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Vote = r.id
	r.leadTransferee = 0
	r.votes = make(map[uint64]bool)
	r.Term++
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	// rest the raft state first
	log.Warnf("%+v become leader", r.id)
	r.State = StateLeader
	r.Lead = r.id
	r.leadTransferee = 0
	r.resetElectionTick()
	lastLogIdx := r.RaftLog.LastIndex()
	for _, p := range r.peers {
		r.Prs[p] = &Progress{
			Match: 0,
			Next:  lastLogIdx + 1,
		}
	}
	// then send noop entry with append message request
	noopEntry := pb.Entry{
		Term:  r.Term,
		Index: r.RaftLog.LastIndex() + 1,
		Data:  nil,
	}
	r.RaftLog.AppendEntries(noopEntry)
	// send self a MessageType_MsgAppendResponse message
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch m.GetMsgType() {
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
		return nil
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
		return nil
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
		return nil
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
		return nil
	case pb.MessageType_MsgHup:
		if r.State != StateLeader {
			r.startNewElection()
		}
		return nil

	default:

	}
	// process response msg
	switch r.State {
	case StateFollower:
		switch m.GetMsgType() {
		case pb.MessageType_MsgTimeoutNow:
			if _, ok := r.Prs[r.id]; !ok {
				return nil
			}
			r.startNewElection()
		case pb.MessageType_MsgTransferLeader:
			if _, ok := r.Prs[r.id]; !ok {
				return nil
			}
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case StateCandidate:
		// only candidate need to process request vote response
		switch m.GetMsgType() {
		case pb.MessageType_MsgTransferLeader:
			if _, ok := r.Prs[r.id]; !ok {
				return nil
			}
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		case pb.MessageType_MsgRequestVoteResponse:
			if m.GetReject() {
				// if the response has larger term, downgrade state to follow
				r.votes[m.From] = false
				if m.GetTerm() > r.Term {
					r.becomeFollower(m.GetTerm(), 0)
					return nil
				}
				// if we get reject from majority, also should downgrade to follow
				rejectCount := 0
				for _, vote := range r.votes {
					if !vote {
						rejectCount++
					}
				}
				if rejectCount > len(r.peers)>>1 {
					r.becomeFollower(r.Term, 0)
				}
				return nil
			}
			r.votes[m.From] = true
			// grant vote from majority
			voteCount := 0
			for _, vote := range r.votes {
				if vote {
					voteCount++
				}
			}
			if voteCount > len(r.peers)>>1 {
				r.becomeLeader()
				r.broadcastAppend(m)
			}
		default:

		}
	case StateLeader:
		switch m.GetMsgType() {
		case pb.MessageType_MsgPropose:
			if len(m.GetEntries()) == 0 {
				panic("empty entries in a Propose msg")
			}
			if r.leadTransferee != 0 {
				return ErrProposalDropped
			}
			entry := m.GetEntries()[0]
			entry.Term = r.Term
			entry.Index = r.RaftLog.LastIndex() + 1
			r.RaftLog.AppendEntries(*entry)

			r.broadcastAppend(m)
		case pb.MessageType_MsgBeat:
			r.broadcastHeartBeat()
		case pb.MessageType_MsgAppendResponse:
			// if message's reject is true, update peer's Next to the m.GetIndex()
			// m.GetIndex() in a reject response indicates the index of the first entry having the conflict term
			// leader should send entries starting form the m.GetIndex() (including) to the rejected follower
			if m.GetReject() {
				if m.GetTerm() > r.Term {
					r.becomeFollower(m.GetTerm(), 0)
					return nil
				}
				// in this case, the follower has no log
				// leader should send snapshot to it
				if m.GetIndex() == uint64(math.MaxUint64) {
					snapshot, err := r.RaftLog.storage.Snapshot()
					if err != nil {
						return err
					}
					m := pb.Message{
						MsgType:  pb.MessageType_MsgSnapshot,
						To:       m.GetFrom(),
						From:     r.id,
						Term:     r.Term,
						Snapshot: &snapshot,
					}
					r.msgs = append(r.msgs, m)
					return nil
				}
				r.Prs[m.GetFrom()].Next = max(m.GetIndex(), r.Prs[m.GetFrom()].Match+1)
				r.sendAppend(m.GetFrom())
				return nil
			}
			if m.GetSnapshot() != nil {
				log.Warnf("%v get response for sp form %+v, index:%+v", r.id, m.GetFrom(), m.GetIndex())
			}
			// update progress status for each peer if message's reject is false
			from := m.GetFrom()
			if m.GetIndex()+1 > r.Prs[from].Next {
				r.Prs[from].Next = m.GetIndex() + 1
			}
			if m.GetIndex() > r.Prs[from].Match {
				r.Prs[from].Match = m.GetIndex()
			}
			// update the commit index if get positive response more than majority
			// If there exists an N such that N > commitIndex, a majority
			// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
			// set commitIndex = N (§5.3, §5.4).

			var shouldApply bool
			for n := r.RaftLog.committed + 1; n <= m.GetIndex(); n++ {
				term, err := r.RaftLog.Term(n)
				if err != nil {
					continue
				}
				if r.Term != term {
					continue
				}
				count := 0
				for _, prs := range r.Prs {
					if prs.Match >= n {
						count++
					}
				}
				if count > (len(r.peers) >> 1) {
					shouldApply = true
					r.RaftLog.committed = n
				}
				// todo save entries into storage, how to do?
			}
			// check if any entries can be applied, if so send heart beat
			if shouldApply {
				for _, v := range r.peers {
					if v == r.id {
						continue
					}
					r.sendAppend(v)
				}
			}
			// process leader transfer after target catch up the logs
			if from == r.leadTransferee && r.Prs[from].Match == r.RaftLog.committed {
				r.sendTimeoutNow(from)
			}
		case pb.MessageType_MsgHeartbeatResponse:
			if m.GetReject() && m.GetTerm() > r.Term {
				r.becomeFollower(m.GetTerm(), 0)
				return nil
			}
			if r.Prs[m.GetFrom()].Match < r.RaftLog.LastIndex() {
				r.sendAppend(m.GetFrom())
			}
		case pb.MessageType_MsgTransferLeader:
			// the upper application set transfer target in the message.From
			transferTarget := m.GetFrom()
			// this MessageType_MsgTransferLeader message can be ignored under the following cases:
			if r.id == transferTarget || r.leadTransferee == transferTarget {
				return nil
			}
			if _, ok := r.Prs[transferTarget]; !ok {
				return nil
			}
			r.resetElectionTick()
			r.leadTransferee = transferTarget
			if r.Prs[transferTarget].Match == r.RaftLog.LastIndex() {
				r.sendTimeoutNow(transferTarget)
			} else {
				if transferTarget == 6 {
					log.Infof("%+v sendAppend to %+v", r.id, m.GetFrom())
				}
				r.sendAppend(transferTarget)
			}
		default:
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	reply := r.genResponseMsg(m)
	reply.MsgType = pb.MessageType_MsgAppendResponse
	// request form old term should not disturb any thing
	// todo move to a common function???
	if m.GetTerm() < r.Term {
		reply.Reject = true
		reply.Term = r.Term
		r.msgs = append(r.msgs, reply)
		return
	}
	r.resetElectionTick()
	if m.GetTerm() >= r.Term {
		r.becomeFollower(m.GetTerm(), m.GetFrom())
	}

	// m.GetIndex() -> prevLogIndex
	// m.GetLogTerm() -> prevLogTerm
	// todo if local log entries does not contain prevLogIndex
	if term, err := r.RaftLog.Term(m.GetIndex()); err != nil {
		reply.Reject = true
		// m.GetIndex() may larger than last index or the storage is empty
		reply.Index = r.RaftLog.LastIndex() + 1
		r.msgs = append(r.msgs, reply)
		return
	} else if term != m.GetLogTerm() {
		reply.Reject = true
		reply.Index = r.RaftLog.FirstIndexOfTerm(term)
		log.Warnf("%+v index:%+v, term:%v not match term:%+v, fI2T:%+v", r.id, m.GetIndex(), m.GetTerm(), term, reply.Index)
		r.msgs = append(r.msgs, reply)
		return
	}

	l := len(m.GetEntries())
	lastIdx := r.RaftLog.LastIndex()
	// scan all entries for rule 3
	i := 0 // i points to the log position of the entries in the message
	for i < l {
		idx := m.GetEntries()[i].GetIndex()
		term := m.GetEntries()[i].GetTerm()
		if idx > lastIdx {
			break
		} else if logTerm, err := r.RaftLog.Term(idx); err != nil {
			break
		} else if logTerm != term {
			// if we discard some entries, we should update the stable index if we also discard stabled entries
			r.RaftLog.Discard(idx)
			//r.RaftLog.entries = r.RaftLog.entries[:idx-1]
			break
		}
		i++
	}
	for j := i; j < l; j++ {
		r.RaftLog.AppendEntries(*m.GetEntries()[j])
	}

	if m.GetCommit() > r.RaftLog.committed {
		newCommitted := min(m.GetCommit(), m.GetIndex()+uint64(len(m.GetEntries())))
		r.RaftLog.committed = newCommitted
	}

	// todo if success set index in reply to the local last log index
	reply.Index = m.Index + uint64(len(m.Entries))
	r.msgs = append(r.msgs, reply)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	reply := r.genResponseMsg(m)
	reply.MsgType = pb.MessageType_MsgHeartbeatResponse

	if m.GetTerm() < r.Term {
		reply.Reject = true
		reply.Term = r.Term
		r.msgs = append(r.msgs, reply)
		return
	}
	r.resetElectionTick()
	if m.GetTerm() > r.Term {
		r.becomeFollower(m.GetTerm(), m.From)
	}

	if m.GetCommit() > r.RaftLog.committed {
		r.RaftLog.committed = min(m.GetCommit(), r.RaftLog.LastIndex())
	}
	r.msgs = append(r.msgs, reply)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).

	reply := r.genResponseMsg(m)
	reply.MsgType = pb.MessageType_MsgAppendResponse
	// the data field on eraftpb.Snapshot does not represent the actual state machine data but some metadata used for the upper application you can ignore it for now.
	if m.GetTerm() < r.Term {
		reply.Reject = true
		reply.Term = r.Term
		r.msgs = append(r.msgs, reply)
		return
	}
	r.resetElectionTick()
	snapshot := m.GetSnapshot()
	// log.Warnf("[P%+v] snapshot metadata: %+v", r.id, snapshot.GetMetadata())
	lastIdxBeforeSp := r.RaftLog.LastIndex()
	// nothing to update
	if snapshot.GetMetadata().GetIndex() <= r.RaftLog.committed {
		reply.Index = r.RaftLog.committed
		r.msgs = append(r.msgs, reply)
		return
	}

	if term, err := r.RaftLog.Term(snapshot.GetMetadata().GetIndex()); err == nil {
		if term == snapshot.GetMetadata().GetTerm() {
			if snapshot.GetMetadata().GetIndex() > lastIdxBeforeSp {
				panic("snapshot mess up")
			}
			r.RaftLog.committed = snapshot.GetMetadata().GetIndex()
			r.RaftLog.applied = snapshot.GetMetadata().GetIndex()
		}
		reply.Index = r.RaftLog.committed
		r.msgs = append(r.msgs, reply)
		return
	}
	// need to update state
	r.RaftLog.committed = snapshot.GetMetadata().GetIndex()
	r.RaftLog.applied = snapshot.GetMetadata().GetIndex()
	r.RaftLog.stabled = snapshot.GetMetadata().GetIndex()
	if r.Lead == 0 {
		r.Lead = m.GetFrom()
	}

	if len(snapshot.GetMetadata().GetConfState().GetNodes()) > 0 {
		lookup := map[uint64]struct{}{}
		for _, id := range snapshot.GetMetadata().GetConfState().GetNodes() {
			lookup[id] = struct{}{}
			if _, ok := r.Prs[id]; ok {
				if snapshot.GetMetadata().GetIndex() > r.Prs[id].Match {
					r.Prs[id].Match = snapshot.GetMetadata().GetIndex()
					r.Prs[id].Next = r.Prs[id].Match + 1
				}
			} else {
				r.Prs[id] = &Progress{
					Match: snapshot.GetMetadata().GetIndex(),
					Next:  snapshot.GetMetadata().GetIndex() + 1,
				}
			}
		}
		r.peers = snapshot.GetMetadata().GetConfState().GetNodes()
		// check if any peer is removed
		if len(r.peers) < len(r.Prs) {
			for id, _ := range r.Prs {
				if _, ok := lookup[id]; !ok {
					delete(r.Prs, id)
				}
			}
		}
	}
	if lastIdxBeforeSp <= snapshot.GetMetadata().Index {
		r.RaftLog.entries = nil
	}
	r.RaftLog.pendingSnapshot = m.GetSnapshot()

	reply.Index = r.RaftLog.LastIndex()
	reply.Snapshot = m.Snapshot // just for debugging
	r.msgs = append(r.msgs, reply)
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. If votedFor is null or candidateId, and candidate’s log is at
// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
func (r *Raft) handleRequestVote(m pb.Message) {

	reply := r.genResponseMsg(m)
	reply.MsgType = pb.MessageType_MsgRequestVoteResponse

	// Reply false if term < currentTerm (§5.1)
	if m.GetTerm() < r.Term {
		reply.Reject = true
		reply.Term = r.Term
		r.msgs = append(r.msgs, reply)
		return
	}
	r.resetElectionTick()

	if m.GetTerm() > r.Term {
		r.becomeFollower(m.GetTerm(), 0)
	}
	// If votedFor is null or candidateId, and candidate's log is at
	// least as complete as local log (§5.2, §5.4), grant vote and
	// reset election timeout

	if r.Vote != 0 && r.Vote != m.GetFrom() {
		reply.Reject = true
		r.msgs = append(r.msgs, reply)
		return
	}

	if r.isMoreUpToDate(m.GetLogTerm(), m.GetIndex()) {
		reply.Reject = true
		r.msgs = append(r.msgs, reply)
		return
	}
	r.Vote = m.GetFrom()
	r.msgs = append(r.msgs, reply)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; ok {
		return
	}
	first, _ := r.RaftLog.storage.FirstIndex()
	r.Prs[id] = &Progress{
		Next:  first,
		Match: 0,
	}
	r.peers = append(r.peers, id)
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; !ok {
		return
	}
	target := 0
	for i, v := range r.peers {
		if v == id {
			target = i
			delete(r.Prs, v)
			break
		}
	}
	r.peers[target], r.peers[len(r.peers)-1] = r.peers[len(r.peers)-1], r.peers[target]
	r.peers = r.peers[:len(r.peers)-1]
	// since quorum requirement is reduced, update the committed index
	quorum := len(r.peers) >> 1
	maxIdx := r.RaftLog.committed
	for _, prs := range r.Prs {
		maxIdx = max(maxIdx, prs.Match)
	}
	for i := maxIdx; i > r.RaftLog.committed; i-- {
		count := 0
		for _, prs := range r.Prs {
			if prs.Match >= i {
				count++
			}
		}
		if count >= quorum+1 {
			r.RaftLog.committed = i
			break
		}
	}

	// If the removed node is the leadTransferee, then abort the leadership transferring.
	if r.State == StateLeader && r.leadTransferee == id {
		r.leadTransferee = 0
	}
}

// helper function
func (r *Raft) getMsgRequestVote(peer uint64) (*pb.Message, error) {
	lastIdx := r.RaftLog.LastIndex()
	lastTerm, err := r.RaftLog.Term(lastIdx)
	if err != nil {
		return nil, err
	}
	return &pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      peer,
		Term:    r.Term,
		Index:   lastIdx,
		LogTerm: lastTerm,
	}, nil
}

func (r *Raft) getMsgHeartBeat(peer uint64) (*pb.Message, error) {
	// we also need to attach commit info within heartbeat
	return &pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      peer,
		Term:    r.Term,
		Commit:  min(r.Prs[peer].Match, r.RaftLog.committed),
	}, nil
}

func (r *Raft) getMsgTimeoutNow(peer uint64) (*pb.Message, error) {
	// we also need to attach commit info within heartbeat
	return &pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		From:    r.id,
		To:      peer,
		Term:    r.Term,
	}, nil
}

// isMoreUpToDate check if local log entries are more up to date
func (r *Raft) isMoreUpToDate(lastLogTerm, lastLogIndex uint64) bool {
	myLastLogIdx := r.RaftLog.LastIndex()
	myLastLogTerm, err := r.RaftLog.Term(myLastLogIdx)
	if err != nil {
		return false
	}

	if myLastLogTerm > lastLogTerm {
		return true
	} else if myLastLogTerm == lastLogTerm && myLastLogIdx > lastLogIndex {
		return true
	}
	return false
}

func (r *Raft) startNewElection() {
	if r.State == StateFollower {
		r.becomeCandidate()
	} else {
		r.Term++
	}
	for _, peerID := range r.peers {
		if peerID == r.id {
			// send to myself
			reply := pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      r.id,
				From:    r.id,
				Term:    r.Term,
				Reject:  false,
			}
			r.Step(reply)
			continue
		}
		msg, err := r.getMsgRequestVote(peerID)
		if err != nil {
			panic("must panic")
		}
		r.msgs = append(r.msgs, *msg)
	}
	r.resetElectionTick()
}

func (r *Raft) broadcastHeartBeat() {
	r.heartbeatElapsed = 1
	for peerID, _ := range r.Prs {
		if peerID == r.id {
			continue
		}
		r.sendHeartbeat(peerID)
	}
}

func (r *Raft) broadcastAppend(source pb.Message) {
	for id := range r.Prs {
		if id == r.id {
			// send self a MessageType_MsgAppendResponse message
			reply := pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				To:      r.id,
				From:    r.id,
				Term:    r.Term,
				Index:   r.RaftLog.LastIndex(),
				Reject:  false,
			}
			r.Step(reply)
		} else {
			r.sendAppend(id)
		}
	}
}

func (r *Raft) sendTimeoutNow(to uint64) {
	msg, err := r.getMsgTimeoutNow(to)
	if err != nil {
		panic("must panic")
	}
	r.msgs = append(r.msgs, *msg)
}

func (r *Raft) genResponseMsg(m pb.Message) pb.Message {
	reply := new(pb.Message)
	reply.From = m.GetTo()
	reply.To = m.GetFrom()
	reply.Term = r.Term
	return *reply
}

func (r *Raft) resetElectionTick() {
	et := r.dice.Int63n(r.electionTimeout)
	r.electionElapsed = 0 - et
}

func (r *Raft) IsSoftStateUpdate() bool {
	return r.prevLead != r.Lead || r.prevState != r.State
}

func (r *Raft) SyncSoftState(state SoftState) {
	r.prevLead, r.prevState = state.Lead, state.RaftState
}

func (r *Raft) IsHardStateUpdate() bool {
	return r.prevHardState.Commit != r.RaftLog.committed ||
		r.prevHardState.Term != r.Term ||
		r.prevHardState.Vote != r.Vote
}

func (r *Raft) GetHardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) SyncHardState(state pb.HardState) {
	r.prevHardState = state
}

func (r *Raft) shouldSendSnapshot(to uint64) (pb.Snapshot, bool) {
	_, errt := r.RaftLog.Term(r.Prs[to].Next - 1)
	i := r.RaftLog.findPosition(r.Prs[to].Next)
	if errt == nil && i >= 0 {
		return pb.Snapshot{}, false
	}
	snapshot, err := r.RaftLog.snapshot()
	if err != nil {
		return snapshot, false
	}
	if r.Prs[to].Next > snapshot.GetMetadata().GetIndex() {
		return snapshot, false
	}
	return snapshot, true
}

func (r *Raft) IsMember(id uint64) bool {
	if id == None {
		return false
	}
	_, ok := r.Prs[id]
	return ok
}
