package jsn_raft

import (
	"sync"
	"time"
)

const (
	follower = iota
	candidate
	leader
)

type Raft struct {
	who []byte

	persistentState     persistentState
	volatileState       volatileState
	leaderVolatileState leaderVolatileState

	goroutineWaitGroup sync.WaitGroup

	log JLogger

	serverState int32

	timeout time.Timer
}

func NewRaft() *Raft {
	r := new(Raft)
	r.safeGo("main loop", r.loop)
	return r
}

func (r *Raft) loop() {
}

func (r *Raft) safeGo(name string, f func()) {
	r.goroutineWaitGroup.Add(1)
	defer func() {
		defer func() {
			if err := recover(); nil != err {
				r.log.Error("safe go panic %v recover %v", name, err)
			}
			r.goroutineWaitGroup.Done()
		}()
		f()
	}()
}

func (r *Raft) fsmRun() {
	switch r.serverState {
	case follower:

	case candidate:
		r.persistentState.currentTerm++
		r.persistentState.votedFor = r.who

		r.askVote()
	case leader:

	}
}

func (r *Raft) askVote() {}

func (r *Raft) rpcHandler() {

}

func (r *Raft) appendEntries(term uint64, leaderId []byte, prevLogIndex uint64, prevLogTerm uint64, entries []JLog, leaderCommit uint64) (currentTerm uint64, success bool) {
	r.follow(term)
	currentTerm = r.persistentState.currentTerm

	if term < r.persistentState.currentTerm {
		return
	}
	if r.persistentState.matchLog(prevLogIndex, prevLogTerm) {
		return
	}
	lastLogIndex := r.persistentState.LastLogIndex()
	var newEntries []JLog
	for i, entry := range entries {
		if entry.Index() > lastLogIndex {
			newEntries = entries[i:]
			break
		}
		jlog := r.persistentState.getLog(entry.Index())
		if nil == jlog {
			return
		}
		if entry.Term() != jlog.Term() {
			r.persistentState.deleteLog(entry.Index(), lastLogIndex)
		}
	}
}

func (r *Raft) voteHandler(term uint64, candidateId []byte, lastLogIndex uint64, lastLogTerm uint64) (currentTerm uint64, voteGranted bool) {

	r.follow(term)

	if term < r.persistentState.currentTerm {
		return r.persistentState.currentTerm, false
	}
	votedFor := r.persistentState.votedFor
	if (0 == len(votedFor) || checkSame(votedFor, candidateId)) &&
		(lastLogTerm > r.persistentState.LastLogTerm()) ||
		(lastLogTerm == r.persistentState.LastLogTerm() && lastLogIndex >= r.persistentState.LastLogIndex()) {

		r.persistentState.votedFor = candidateId

		return r.persistentState.currentTerm, true
	}
	return r.persistentState.currentTerm, false
}

func (r *Raft) follow(term uint64) bool {
	if term > r.persistentState.currentTerm {
		r.persistentState.currentTerm = term
		r.serverState = follower
		r.persistentState.votedFor = nil
		return true
	}
	return false
}

func checkSame[T comparable](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}
