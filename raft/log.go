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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pkg/errors"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	offset uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()
	entries, _ := storage.Entries(firstIndex, lastIndex+1)
	return &RaftLog{
		storage: storage,
		stabled: lastIndex,
		committed: firstIndex - 1,
		applied: firstIndex - 1,
		offset: firstIndex,
		entries: entries,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) > 0 {
		return l.entries[l.stabled-l.offset+1:]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[l.applied-l.offset+1 : l.committed-l.offset+1]
	}
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	return 0
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if i == 0 {
		return 0, nil
	}
	if len(l.entries) > 0 && i >= l.offset {
		return l.entries[i-l.offset].Term, nil
	}
	return 0, errors.New("error")
}

func (l *RaftLog) Committed() uint64 {
	return l.committed
}

func (l *RaftLog) Append(ents []*pb.Entry) {
	var entries []pb.Entry
	for _, e := range ents {
		entries = append(entries, *e)
	}
	l.entries = append(l.entries, entries...)
}

func (l *RaftLog) append(ents []*pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.entries[len(l.entries)-1].Index
	}
	after := ents[0].Index
	if after-1 < l.committed {
		panic("out of range commit")
	}

	if (len(l.entries) == 0 && l.offset == after) || after == l.entries[len(l.entries)-1].Index + 1 {

	} else if after <= l.entries[0].Index {
		// 没有快照
	} else {
		truncate := after - l.entries[0].Index
		l.entries = l.entries[truncate:]
	}
	for _, ent := range ents {
		l.entries = append(l.entries, *ent)
	}
	return l.entries[len(l.entries)-1].Index
}

func (l *RaftLog) CommitTo(committed uint64) {
	if l.committed >= committed {
		return
	}
	if committed > l.LastIndex() {
		panic("to commited out of range")
	}
	l.committed = committed
}

func (l *RaftLog) Entries(i uint64) []pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}
	last := len(l.entries) - 1
	for last >= 0 {
		if l.entries[last].Index == i {
			break
		}
		last -= 1
	}
	return l.entries[last:]
}

func (l *RaftLog) findConflict(ents []*pb.Entry) uint64 {
	for _, ent := range ents {
		t, err := l.Term(ent.Index)
		if err != nil || t != ent.Term {
			return ent.Index
		}
	}
	return 0
}

func (l *RaftLog) MaybeAppend(idx uint64, term uint64, commited uint64, ents []*pb.Entry) (uint64, uint64, bool){
	t, err := l.Term(idx)
	if err == nil && t == term {
		conflictIdx := l.findConflict(ents)
		if conflictIdx == 0 {

		} else {
			start := conflictIdx - (idx + 1)
			l.append(ents[start:])
			if l.stabled > conflictIdx-1 {
				l.stabled = conflictIdx - 1
			}
		}
		lastNewIdx := idx + uint64(len(ents))
		l.committed = min(commited, lastNewIdx)
		return conflictIdx, lastNewIdx, true
	}
	return 0, 0, false
}

func (l *RaftLog) term(conflictIndex uint64) uint64 {
	if conflictIndex == 0 {
		return 0
	}
	return l.entries[conflictIndex-1].Term
}

func (l *RaftLog) FindConflictByTerm(index uint64, term uint64) (uint64, uint64) {
	conflictIndex := index
	for {
		t := l.term(conflictIndex)
		if t > term {
			conflictIndex -= 1
		} else {
			return conflictIndex, t
		}
	}
}