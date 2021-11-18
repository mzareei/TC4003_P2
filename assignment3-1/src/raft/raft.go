package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

//
// Request vote RPC msg struct
//
type RequestVoteArgs struct {
	Term         int
	CandidateID  string
}

//
// Follower -> Candidate RPC reply msg struct
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	Id          string
}

//
// Raft peer representation struct.
//
type Raft struct {
	sync.Mutex
	peers          []*labrpc.ClientEnd
	persister      *Persister
	me             int
	id             string
	state          string
	isInvalid      bool
	currentTerm    int
	votedFor       string
	leaderID       string
	log            []LogEntry
	commitIndex    int
	lastApplied    int
	nextIndex      []int
	matchIndex     []int
	sendAppendChan []chan struct{}
	lastHeartBeat  time.Time
}

//
// The service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		id:          fmt.Sprint(me),
		state:       Follower,
		commitIndex: 0,
		lastApplied: 0,
	}

	rf.readPersist(persister.ReadRaftState())
	go rf.setRFTimeout()
	go rf.localCommit(applyCh)
	return rf
}

//
// Function to handle requests from candidates, executed by receivers
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.Lock()
	defer rf.Unlock()

	reply.Term = rf.currentTerm
	reply.Id = rf.id

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if args.Term >= rf.currentTerm /*&& logUpToDate */{
		rf.setAsFollower(args.Term)
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
	}

	rf.persist()
}

//
// Handles append entries call on receiver
//
func (rf *Raft) AppendEntries(appendEntriesArgs *AppendEntriesArgs, appendEntriesReply *AppendEntriesReply) {
	rf.Lock()
	defer rf.Unlock()

	appendEntriesReply.Term = rf.currentTerm
	if appendEntriesArgs.Term < rf.currentTerm {
		appendEntriesReply.Success = false
		return
	}

	rf.setAsFollower(appendEntriesArgs.Term)
	rf.leaderID = appendEntriesArgs.LeaderID
	rf.lastHeartBeat = time.Now()

	rf.persist()
}

//
// Save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent
//
func (rf *Raft) persist() {
	buffer := new(bytes.Buffer)
	gob.NewEncoder(buffer).Encode(
		PersistentState{
			CurrentTerm: rf.currentTerm,
			VotedFor:    rf.votedFor,
		})

	rf.persister.SaveRaftState(buffer.Bytes())
}

//
// Restore previously persisted state.
//
func (rf *Raft) readPersist(input []byte) {
	readBuffer := bytes.NewBuffer(input)
	decoded := gob.NewDecoder(readBuffer)
	persistentState := PersistentState{}
	decoded.Decode(&persistentState)

	rf.currentTerm = persistentState.CurrentTerm
	rf.votedFor = persistentState.VotedFor
}

//
// Return currentTerm and whether this server believes it is the leader
//
func (rf *Raft) GetState() (int, bool) {
	rf.Lock()
	defer rf.Unlock()
	return rf.currentTerm, rf.state == Leader
}

//
// Get last peer's entry index and last peer's entry term if any, -1 for both if not found
//
func (rf *Raft) getLastEntryInfo() (int, int) {
	if len(rf.log) > 0 {
		entry := rf.log[len(rf.log)-1]
		return entry.Index, entry.Term
	}
	return -1, -1
}

//
// Get index in peer for log if any, -1 if not found
//
func (rf *Raft) findLogIndex(logIndex int) int {
	for i, e := range rf.log {
		if e.Index == logIndex {
			return i
		}
	}
	return -1
}

//
// Set peer as follower, it sets term to leader's term and cleans vote
//
func (rf *Raft) setAsFollower(newTerm int) {
	rf.state = Follower
	rf.currentTerm = newTerm
	rf.votedFor = ""
}

//
// Checks if there is something to update on peer or just heartbeat is needed
//
func (rf *Raft) PrepareEntries(peerIndex int, sendAppendChan chan struct{}) {
	rf.Lock()

	rf.Unlock()

	sendAppendEntries(rf, peerIndex, sendAppendChan)
}

//
// Makes append entries call and updates matchIndex and nextIndex
//
func sendAppendEntries(rf *Raft, peerIndex int, sendAppendChan chan struct{}) {
	appendEntriesReply := AppendEntriesReply{}
	appendEntriesArgs := AppendEntriesArgs{
		Term:             rf.currentTerm,
		LeaderID:         rf.id,
		LeaderCommit:     rf.commitIndex,
	}

	ok := RPCrequestWrapper("Raft.AppendEntries", &appendEntriesArgs, &appendEntriesReply, rf.peers[peerIndex])

	rf.Lock()
	defer rf.Unlock()
	// It means that there is a match in peer and leader for prevLogIndex and prevLogTerm
	if !(ok && appendEntriesReply.Success) || rf.state != Leader || rf.isInvalid || appendEntriesArgs.Term != rf.currentTerm {
		// Peer is in higher term than leader
		if appendEntriesReply.Term > rf.currentTerm {
			rf.setAsFollower(appendEntriesReply.Term)
		} else {
			// If there is a conflict index, then next index will overwrite starting from conflict index
			if appendEntriesReply.ConflictIndex-1 > 1 {
				rf.nextIndex[peerIndex] = appendEntriesReply.ConflictIndex - 1
			} else {
				rf.nextIndex[peerIndex] = 1
			}

			// Leader needs to send entries to this peer
			sendAppendChan <- struct{}{}
		}
	}
	rf.persist()
}

//
// Updates commit index if already committed by the majority, from newest to oldest
//
func (rf *Raft) updateCommitIndex() {
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Term == rf.currentTerm && rf.log[i].Index > rf.commitIndex {
			replicationCount := 1
			for j := range rf.peers {
				if j != rf.me && rf.matchIndex[j] >= rf.log[i].Index {
					replicationCount++
					if replicationCount > len(rf.peers)/2 {
						rf.commitIndex = rf.log[i].Index
						break
					}
				}
			}
		} else {
			break
		}
	}
}

//
// Go routine running always in background, checks if peer is behind of commit and locally commit entries
//
func (rf *Raft) localCommit(commitChannel chan ApplyMsg) {
	for {
		rf.Lock()

		if rf.commitIndex >= 0 && rf.commitIndex > rf.lastApplied {
			startIndex := rf.findLogIndex(rf.lastApplied + 1)
			if startIndex <= 0 {
				startIndex = 0
			}

			endIndex := -1
			for i := startIndex; i < len(rf.log); i++ {
				if rf.log[i].Index <= rf.commitIndex {
					endIndex = i
				}
			}

			if endIndex >= 0 {
				entries := make([]LogEntry, endIndex-startIndex+1)
				copy(entries, rf.log[startIndex:endIndex+1])
				rf.Unlock()

				for _, v := range entries {
					commitChannel <- ApplyMsg{Index: v.Index, Command: v.Command}
				}

				rf.Lock()
				rf.lastApplied += len(entries)
			}
			rf.Unlock()

		} else {
			rf.Unlock()
			time.Sleep(20 * time.Millisecond)
		}
	}
}

//
// Go routine always running, sets random timeout (from 200 to 500 ms) and begins election if needed,
// only if it wasn't been killed
//
func (rf *Raft) setRFTimeout() {
	timeout := (200 + time.Duration(rand.Intn(300))) * time.Millisecond
	now := <-time.After(timeout)

	rf.Lock()
	defer rf.Unlock()

	if !rf.isInvalid {
		if now.Sub(rf.lastHeartBeat) >= timeout && rf.state != Leader {
			go rf.startElection()
		}
		go rf.setRFTimeout()
	}
}

//
// RF transitions to candidate and sends request vote to all the peers
//
func (rf *Raft) startElection() {
	rf.Lock()

	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.id

	requestsVotesReplies := make([]RequestVoteReply, len(rf.peers))
	votesChannel := make(chan int, len(rf.peers))
	requestVoteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.id,
	}

	for i := range rf.peers {
		if i != rf.me {
			go rf.requestVoteRoutine(rf.peers[i], i, votesChannel, &requestVoteArgs, &requestsVotesReplies[i])
		}
	}

	rf.persist()
	rf.Unlock()

	countVotes(requestsVotesReplies, votesChannel, rf, requestVoteArgs.Term)
}

//
// Calls RPC's wrapper requesting votes to peers, if request succeeded then add id to channel
//
func (rf *Raft) requestVoteRoutine(server *labrpc.ClientEnd, peerId int, votesChannel chan int, requestVoteArgs *RequestVoteArgs, requestsVotesReplies *RequestVoteReply) {
	if RPCrequestWrapper("Raft.RequestVote", requestVoteArgs, requestsVotesReplies, server) {
		votesChannel <- peerId
	}
}

//
// Count votes checking if reply's term is ahead of the current Term, the current server is downgraded to follower and its Term is also downgraded to the current Term,
// otherwise if it gets majority of votes then it is promoted to leader
//
func countVotes(requestsVotesReplies []RequestVoteReply, votesChannel chan int, rf *Raft, currentTerm int) {
	votes := 1
	for i := 0; i < len(requestsVotesReplies); i++ {
		reply := requestsVotesReplies[<-votesChannel]
		rf.Lock()

		if reply.Term > rf.currentTerm {
			rf.setAsFollower(reply.Term)
			break
		} else {
			if reply.VoteGranted {
				votes += 1
			}
			if votes > len(requestsVotesReplies)/2 {
				go rf.promoteToLeader()
				break
			}
		}
		rf.Unlock()
	}
	rf.persist()
	rf.Unlock()
}

//
// Set RF as leader and starts consistency check for each peer
//
func (rf *Raft) promoteToLeader() {
	rf.Lock()
	defer rf.Unlock()

	rf.state = Leader
	rf.leaderID = rf.id
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.sendAppendChan = make([]chan struct{}, len(rf.peers))

	for i := range rf.peers {
		if i != rf.me {
			rf.nextIndex[i] = len(rf.log) + 1
			rf.matchIndex[i] = 0
			rf.sendAppendChan[i] = make(chan struct{}, 1)

			go rf.leaderBackgroundProcess(i, rf.sendAppendChan[i])
		}
	}
}

//
// Go routine for the leader to decide when to send heartbeats and when to send entries to peers
//
func (rf *Raft) leaderBackgroundProcess(peerIndex int, sendAppendChan chan struct{}) {
	leaderTimer := time.NewTicker(10 * time.Millisecond)

	rf.PrepareEntries(peerIndex, sendAppendChan)
	lastCommunicationTime := time.Now()

	for {
		rf.Lock()
		if rf.state != Leader || rf.isInvalid {
			leaderTimer.Stop()
			rf.Unlock()
			break
		}
		rf.Unlock()

		select {
		case <-sendAppendChan:
			lastCommunicationTime = time.Now()
			rf.PrepareEntries(peerIndex, sendAppendChan)
		case timeAsLeader := <-leaderTimer.C:
			if timeAsLeader.Sub(lastCommunicationTime) >= TimeOut {
				lastCommunicationTime = time.Now()
				rf.PrepareEntries(peerIndex, sendAppendChan)
			}
		}
	}
}

//
// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	term, isLeader := rf.GetState()

	if !isLeader {
		return -1, term, isLeader
	}

	rf.Lock()
	defer rf.Unlock()

	nextIndex := func() int {
		if len(rf.log) > 0 {
			return rf.log[len(rf.log)-1].Index + 1
		}
		return 1
	}()

	entry := LogEntry{Index: nextIndex, Term: rf.currentTerm, Command: command}
	rf.log = append(rf.log, entry)

	return nextIndex, term, isLeader
}

//
// The tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//

func (rf *Raft) Kill() {
	rf.Lock()
	rf.isInvalid = true
	rf.Unlock()
}

//
// Send the actual RPC request using labrpc methods
//
func RPCrequestWrapper(requestName string, args interface{}, reply interface{}, server *labrpc.ClientEnd) bool {
	makeRequest := func(successChan chan struct{}) {
		if server.Call(requestName, args, reply) {
			successChan <- struct{}{}
		}
	}

	for attempts := 0; attempts < RPCRetries; attempts++ {
		rpcChan := make(chan struct{}, 1)
		go makeRequest(rpcChan)
		select {
		case <-rpcChan:
			return true
		case <-time.After(TimeOut):
		}
	}
	return false
}

const (
	Follower   = "Follower"
	Candidate  = "Candidate"
	Leader     = "Leader"
	TimeOut    = 200 * time.Millisecond
	RPCRetries = 3
)

//
// Struct used to persist the current peer state
//
type PersistentState struct {
	CurrentTerm int
	Log         []LogEntry
	VotedFor    string
}

//
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// Atomic unit to be stored in log and state machine
//
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

//
// Message struct for RPC calls from leader to followers
//
type AppendEntriesArgs struct {
	Term             int
	LeaderID         string
	PreviousLogIndex int
	PreviousLogTerm  int
	LogEntries       []LogEntry
	LeaderCommit     int
}

//
// Reply message for RPC from follower to leader
//
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictEntry int
	ConflictIndex int
}
