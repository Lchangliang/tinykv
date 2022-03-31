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
	"math/rand"
	"sort"
	"time"

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

var ErrTermSmaller = errors.New("from term is smaller than to")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	//ElectionTick is the number of Node.Tick invocations that must pass between
	//elections. That is, if a follower does not receive any message from the
	//leader of current term before ElectionTick has elapsed, it will become
	//candidate and start an election. ElectionTick must be greater than
	//HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	//unnecessary leader switching.
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
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// election timeout base
	electionTimeoutLower int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

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

	// Rand
	rand *rand.Rand
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	r := Raft{}
	r.RaftLog = newLog(c.Storage)
	r.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	r.id = c.ID
	r.State = StateFollower
	r.Term = 0
	r.heartbeatTimeout = c.HeartbeatTick
	r.electionTimeoutLower = c.ElectionTick
	r.resetRandomizedElectionTimeout()
	r.Lead = None
	r.msgs = make([]pb.Message, 0)
	r.Prs = make(map[uint64]*Progress)
	r.Vote = None
	for _, id := range c.peers {
		r.Prs[id] = &Progress{Match: 0, Next: 0}
	}
	hardState, _, _ := c.Storage.InitialState()
	if hardState.Term != 0 {
		r.Term = hardState.Term
		r.Vote = hardState.Vote
	}
	if c.Applied > 0 {
		// TODO
	}
	r.becomeFollower(r.Term, None)
	return &r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	m := newMessage(to, pb.MessageType_MsgAppend, r.id)
	m.Term = r.Term
	entries :=  r.RaftLog.Entries(r.Prs[to].Next)
	for _, entry := range entries {
		m.Entries = append(m.Entries, &entry)
	}
	m.Index = r.Prs[to].Next - 1
	m.LogTerm, _ = r.RaftLog.Term(m.Index)
	m.Commit = r.RaftLog.committed
	r.send(m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	m := newMessage(to, pb.MessageType_MsgHeartbeat, r.id)
	m.Term = r.Term
	m.Commit = min(r.Prs[to].Match, r.RaftLog.committed)
	r.send(m)
}

func (r *Raft) tickElection() bool {
	r.electionElapsed++
	if r.electionElapsed < r.electionTimeout {
		return false
	}
	r.electionElapsed = 0
	r.Step(pb.Message{From: r.id, To: None, MsgType: pb.MessageType_MsgHup})
	return true
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{From: r.id, To: None, MsgType: pb.MessageType_MsgBeat})
	}
}
// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateFollower:
		fallthrough
	case StateCandidate:
		r.tickElection()
	case StateLeader:
		r.tickHeartbeat()
	}
}

func (r *Raft) resetRandomizedElectionTimeout() {
	r.electionTimeout = r.rand.Intn(r.electionTimeoutLower) + r.electionTimeoutLower
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None
	r.resetRandomizedElectionTimeout()
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.votes = make(map[uint64]bool)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.reset(term)
	r.Lead = lead
	r.State = StateFollower
}

func (r *Raft) startVotes() {
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.electionElapsed = 0
	r.electionTimeout = r.rand.Intn(r.electionTimeoutLower) + r.electionTimeoutLower
	for id := range r.Prs {
		if id != r.id {
			r.msgs = append(r.msgs, pb.Message{From: r.id, To: id, Term: r.Term, MsgType: pb.MessageType_MsgRequestVote})
		}
	}
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	term := r.Term + 1
	r.reset(term)
	r.State = StateCandidate
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
}

func (r *Raft) appendEntry(entris []*pb.Entry) bool {
	li := r.RaftLog.LastIndex()
	for i, e := range entris {
		e.Term = r.Term
		e.Index = li + uint64(i) + 1
	}
	r.RaftLog.Append(entris)
	li = r.RaftLog.LastIndex()
	if r.Prs[r.id].Match < li {
		r.Prs[r.id].Match = li
	}
	r.Prs[r.id].Next = max(r.Prs[r.id].Next, li+1)
	return true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	for id, _ := range r.Prs {
		if id == r.id {
			// 加上空entry
			r.Prs[id].Next = r.RaftLog.LastIndex() + 2
			r.Prs[id].Match = r.RaftLog.LastIndex() + 1
		} else {
			r.Prs[id].Next = r.RaftLog.LastIndex() + 1
		}
	}
	m := newMessage(r.id, pb.MessageType_MsgPropose, r.id)
	m.Entries = []*pb.Entry{{Data: nil, Term: r.Term, Index: r.RaftLog.LastIndex()+1}}
	r.Step(*m)
}

func newMessage(to uint64, messageType pb.MessageType, from uint64) *pb.Message {
	return &pb.Message {
		MsgType: messageType,
		To: to,
		From: from,
	}
}

func (r *Raft) send(m *pb.Message) {
	r.msgs = append(r.msgs, *m)
}

func (r *Raft) poll() bool {
	var gr int
	for _, vote := range r.votes {
		if vote {
			gr++
		}
	}
	if gr >= (len(r.Prs) / 2) + 1 {
		r.becomeLeader()
		return true
	}
	return false
}
func (r *Raft) campaign() {
	r.becomeCandidate()
	if r.poll() {
		return
	}
	for id, _ := range r.Prs {
		if id == r.id {
			continue
		}
		m := newMessage(id, pb.MessageType_MsgRequestVote, r.id)
		m.Term = r.Term
		m.Index = r.RaftLog.LastIndex()
		m.LogTerm, _ = r.RaftLog.Term(r.RaftLog.LastIndex())
		r.send(m)
	}
}

func (r *Raft) hup() {
	if r.State == StateLeader {
		return
	}
	r.campaign()
}

func (r* Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleAppendEntries(m)
	case pb.MessageType_MsgPropose:
		if r.Lead == None {
			return errors.New("no leader; dropping proposal")
		}
		m.To = r.Lead
		r.send(&m)
	}
	return nil
}

func (r* Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVoteResponse:
		r.votes[m.From] = !m.Reject
		r.poll()
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgPropose:
		return errors.New("no leader; dropping proposal")
	}
	return nil
}

func (r* Raft) bcastHeartbeat() {
	for id, _ := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id)
	}
	r.heartbeatElapsed = 0
}

func (r* Raft) bcastAppend() {
	for id, _ := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

func (r* Raft) leaderCommit() {
	match := make([]uint64, len(r.Prs))
	for i, prs := range r.Prs {
		match[i-1] = prs.Match
	}
	sort.Slice(match, func(i, j int) bool {
		return match[i] > match[j]
	})
	n := match[(len(r.Prs)-1)/2]

	if n > r.RaftLog.committed {
		logTerm, err := r.RaftLog.Term(n)
		if err != nil {
			panic(err)
		}
		if logTerm == r.Term {
			r.RaftLog.committed = n
			//r.bcastAppend()
		}
	}
}

func (r* Raft) handleAppendResponse(m pb.Message) {
	if m.Reject {
		nextProbeIndex := m.RejectHint
		if m.LogTerm > 0 {
			nextProbeIndex, _ = r.RaftLog.FindConflictByTerm(m.RejectHint, m.LogTerm)
		}
		r.Prs[m.From].Next = max(min(m.Index, nextProbeIndex+1), uint64(1))
		r.sendAppend(m.From)
	} else {
		if r.Prs[m.From].Match < m.Index {
			r.Prs[m.From].Match = m.Index
		}
		r.Prs[m.From].Next = max(r.Prs[m.From].Next, m.Index+1)
		r.leaderCommit()
	}
}


func (r* Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
	case pb.MessageType_MsgPropose:
		if len(m.Entries) == 0 {
			panic("stepped empty MsgProp")
		}
		if _, ok := r.Prs[m.From]; !ok {
			return errors.New("dropping proposal")
		}
		for _, entry := range m.Entries {
			if entry.EntryType == pb.EntryType_EntryConfChange {

			}
		}
		if !r.appendEntry(m.Entries) {

		}
		r.Prs[r.id].Match = r.RaftLog.LastIndex()
		r.Prs[r.id].Next =r.RaftLog.LastIndex() + 1
		r.bcastAppend()
		if len(r.Prs) == 1 {
			r.RaftLog.committed = r.Prs[r.id].Match
		}
		return nil
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	}
	return nil
}
// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	if m.Term == 0 {

	} else if m.Term > r.Term {
		switch m.MsgType {
		case pb.MessageType_MsgHeartbeat:
			fallthrough
		case pb.MessageType_MsgAppend:
			r.becomeFollower(m.Term, m.From)
		default:
			r.becomeFollower(m.Term, None)
		}
	} else if m.Term < r.Term {
		switch m.MsgType {
		case pb.MessageType_MsgHeartbeat:
			fallthrough
		case pb.MessageType_MsgAppend:
			r.msgs = append(r.msgs, pb.Message{From: None, To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgAppendResponse})
		case pb.MessageType_MsgRequestVote:
			r.msgs = append(r.msgs, pb.Message{From: None, To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
		default:
			// ignore other cases
			return nil
		}
	}
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.hup()
	case pb.MessageType_MsgRequestVote:
		canVote := (r.Vote == m.From) || (r.Vote == None && r.Lead == None)
		lastTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
		toSend := newMessage(m.From, pb.MessageType_MsgRequestVoteResponse, r.id)
		toSend.Term = r.Term
		if canVote && (m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.Index >= r.RaftLog.LastIndex())) {
			toSend.Reject = false
			r.electionElapsed = 0
			r.Vote = m.From
		} else {
			toSend.Reject = true
		}
		r.send(toSend)
	default:
		switch r.State {
		case StateFollower:
			return r.stepFollower(m)
		case StateCandidate:
			return r.stepCandidate(m)
		case StateLeader:
			return r.stepLeader(m)
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	toSend := newMessage(m.From, pb.MessageType_MsgAppendResponse, r.id)
	toSend.Term = r.Term
	if m.Index < r.RaftLog.Committed() {
		toSend.Index = r.RaftLog.Committed()
		r.send(toSend)
		return
	}
	_, lastIdx, canAppend := r.RaftLog.MaybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries)
	if canAppend {
		toSend.Index = lastIdx
	} else {
		hintIndex := min(m.Index, r.RaftLog.LastIndex())
		hintIndex, hintTerm := r.RaftLog.FindConflictByTerm(hintIndex, m.LogTerm)
		toSend.Index = m.Index
		toSend.Reject = true
		toSend.RejectHint = hintTerm
		toSend.LogTerm = hintTerm
	}
	r.send(toSend)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	r.RaftLog.CommitTo(m.Commit)
	toSend := newMessage(m.From, pb.MessageType_MsgHeartbeatResponse, r.id)
	toSend.Term = r.Term
	toSend.Commit = r.RaftLog.committed
	r.send(toSend)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
