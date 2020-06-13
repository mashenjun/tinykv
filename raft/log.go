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
	"fmt"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
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

	TermToFirstIndex map[uint64]uint64
	DebugID          uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	// we need to set the applied, committed and stabled
	raftLog := RaftLog{
		storage:          storage,
		entries:          make([]pb.Entry, 0),
		TermToFirstIndex: make(map[uint64]uint64),
	}
	var first, last uint64 = 0, 0
	var err error
	last, err = storage.LastIndex()
	if err != nil {
		panic(err)
	}
	first, err = storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	raftLog.stabled = last
	raftLog.applied = first - 1

	hs, _, err := storage.InitialState()
	if err != nil {
		panic(err)
	}
	raftLog.committed = hs.Commit

	if last+1 > first {
		ents, err := storage.Entries(first, last+1)
		if err != nil {
			panic(err)
		}
		for i := 0; i < len(ents); i++ {
			if _, ok := raftLog.TermToFirstIndex[ents[i].GetTerm()]; !ok {
				raftLog.TermToFirstIndex[ents[i].GetTerm()] = ents[i].GetIndex()
			}
		}
		raftLog.entries = ents
	}

	return &raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	// todo move log entries to storage
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 || l.stabled == 0 {
		return l.entries
	}
	pos := l.findPosition(l.stabled)
	if pos < 0 {
		return l.entries
	}
	return l.entries[pos+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	// todo l.applied -> log index
	// todo l.committed -> log index

	if l.applied == l.committed {
		return
	}
	if len(l.entries) == 0 {
		return
	}
	base := l.entries[0].GetIndex()
	ents = append(ents, l.entries[l.applied-base+1:l.committed-base+1]...)
	return
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	// consider the snapshot and the entries not in the snapshot???
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].GetIndex()
	}
	lastIncludedIdx, err := l.storage.LastIndex()
	if err != nil {
		return 0
	}
	if IsEmptySnap(l.pendingSnapshot) {
		return lastIncludedIdx
	}
	return l.pendingSnapshot.GetMetadata().GetIndex()
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if !IsEmptySnap(l.pendingSnapshot) && l.pendingSnapshot.GetMetadata().GetIndex() >= i {
		return l.pendingSnapshot.GetMetadata().GetTerm(), nil
	}
	if i <= l.stabled || len(l.entries) == 0 {
		return l.storage.Term(i)
	}
	pos := i - l.entries[0].GetIndex()
	if pos >= uint64(len(l.entries)) {
		return 0, fmt.Errorf("index %+v is out of range", pos)
	}
	return l.entries[pos].GetTerm(), nil
}

// Discard remove all entries whose index >= i form the log entries
func (l *RaftLog) Discard(i uint64) {
	if len(l.entries) == 0 {
		return
	}
	hiTerm, err := l.Term(l.LastIndex())
	if err != nil {
		return
	}
	pos := l.findPosition(i)
	l.entries = l.entries[:pos]
	if i <= l.stabled {
		l.stabled = i - 1
	}
	loTerm, err := l.Term(l.LastIndex())
	if err != nil {
		return
	}

	for term := loTerm + 1; term <= hiTerm; term++ {
		delete(l.TermToFirstIndex, term)
	}
}

// FirstIndexOfTerm return index of the first log entry whose term equals to given term
func (l *RaftLog) FirstIndexOfTerm(term uint64) uint64 {
	return l.TermToFirstIndex[term]
}

func (l *RaftLog) AppendEntries(entry ...pb.Entry) {
	lastPos := len(l.entries)
	l.entries = append(l.entries, entry...)
	// update TermToFirstIndex
	for i := lastPos; i < len(l.entries); i++ {
		if _, ok := l.TermToFirstIndex[l.entries[i].GetTerm()]; !ok {
			l.TermToFirstIndex[l.entries[i].GetTerm()] = l.entries[i].GetIndex()
		}
	}
}

// findPosition returns the index in RaftLog.entries
func (l *RaftLog) findPosition(index uint64) int {
	if len(l.entries) == 0 {
		return -1
	}
	offset := l.entries[0].GetIndex()
	if index < offset {
		return -1
	}
	return int(index - offset)
}

func (r *RaftLog) snapshot() (pb.Snapshot, error) {
	// if !IsEmptySnap(r.pendingSnapshot) {
	// 	return *r.pendingSnapshot, nil
	// }
	return r.storage.Snapshot()
}
