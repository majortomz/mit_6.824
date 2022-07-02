package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"math/rand"
	"sort"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	FOLLOWER  = 1
	CANDIDATE = 2
	LEADER    = 3
)

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	statMu        sync.Mutex
	mu            sync.Mutex          // Lock to protect shared access to this peer's state
	peers         []*labrpc.ClientEnd // RPC end points of all peers
	persister     *Persister          // Object to hold this peer's persisted state
	majorityCount int
	me            int   // this peer's index into peers[]
	dead          int32 // set by Kill()
	state         int

	applyChan chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	electionTimeout   int64
	electionStartTime int64
	heartbeatTimeout  int64
	lastLeaderReqTime int64
	votedFor          int
	votes             int

	currentTerm      int
	log              []LogEntry
	commitIndex      int
	lastAppliedIndex int
	firstEntryIndex  int

	// leader only
	readyForServing bool
	nextIndex       []int
	matchIndex      []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var voteFor int
	var currentTerm int
	var log []LogEntry
	if d.Decode(&voteFor) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&log) != nil {
		ErrorPrintf("Node-[%v] read persist fail, some data is null.", rf.me)
	} else {
		rf.votedFor = voteFor
		rf.currentTerm = currentTerm
		rf.log = log
		if len(log) > 0 {
			rf.firstEntryIndex = rf.log[0].Index
		}
		DPrintf("Node-[%v] read persist, voteFor:%v, currentTerm:%v, logSize:%v, log[last]:%v", rf.me,
			rf.votedFor, rf.currentTerm, len(rf.log), rf.log[len(rf.log)-1])
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteForTerm int
	VoteGranted bool
}

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry

	LeaderCommitIndex int
}

type AppendEntryReply struct {
	Term               int
	Success            bool
	ConflictTerm       int
	ConflictStartIndex int
}

func (rf *Raft) calculateMemArrayIndex(index int) int {
	return index - rf.firstEntryIndex
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	TPrintf("Node-[%v] term:%v receive requestVote from Node-[%v] argTerm:[%v] argLogTerm:[%v] argLogIndex:[%v]",
		rf.me, rf.currentTerm, args.CandidateId, args.Term, args.LastLogTerm, args.LastLogIndex)
	reply.VoteForTerm = args.Term
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	lastLogIndex := 0
	lastLogTerm := 0
	if CurrentMilliSeconds()-rf.lastLeaderReqTime <= rf.electionTimeout {
		// reject vote, if election-timeout event doesn't happen in this node
		TPrintf("Node-[%v] term:%v reject requestVote from Node-[%v] because of not see timeout. ", rf.me,
			rf.currentTerm, args.CandidateId)
		reply.VoteGranted = false
		return
	}
	if len(rf.log) > 0 {
		lastLogIndex = rf.log[len(rf.log)-1].Index
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	TPrintf("Node-[%v] lastLogTerm:%v lastLogIndex:%v voteFor:%v", rf.me, lastLogTerm, lastLogIndex, rf.votedFor)
	if args.Term > rf.currentTerm && (rf.state == CANDIDATE || rf.state == LEADER) {
		// candidate's term is new and this node is in CANDIDATE state, this node return to follower state
		rf.toFollowerState()
	}
	if rf.state == FOLLOWER && (rf.votedFor < 0 || rf.votedFor == args.CandidateId) && args.Term >= rf.currentTerm &&
		(args.LastLogTerm > lastLogTerm || args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
		// voteFor is null or voteFor is candidateId
		// candidate's term is gte this node's currentTerm
		// candidate's log is at least up-to-date as this node
		rf.votedFor = args.CandidateId
		rf.electionStartTime = CurrentMilliSeconds()
		InfoPrintf("Node-[%v] vote for Node-[%v].", rf.me, args.CandidateId)
		reply.VoteGranted = true
		rf.persist()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	DPrintf("Node-[%v] receive appendEntries from Node-[%v], empty:[%v], leaderCommitIndex:[%v], prevLogIndex:[%v], "+
		"prevLogTerm:[%v] entryDetail:[%v]",
		rf.me, args.LeaderId, args.Entries == nil, args.LeaderCommitIndex, args.PrevLogIndex, args.PrevLogIndex, args.Entries)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.killed() == true {
		reply.Success = false
		return
	}
	if rf.currentTerm > args.Term {
		// Reply false if term < currentTerm
		DPrintf("Node-[%v] AppendEntries reply false because of larger term.", rf.me)
		reply.Success = false
		return
	}

	if rf.currentTerm < args.Term || rf.state != LEADER {
		rf.toFollowerState()
		rf.currentTerm = args.Term
	}

	if len(rf.log) > 0 {
		rf.firstEntryIndex = rf.log[0].Index
	} else {
		rf.firstEntryIndex = 0
	}
	prevLogMemIndex := rf.calculateMemArrayIndex(args.PrevLogIndex)
	rf.lastLeaderReqTime = CurrentMilliSeconds()
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if prevLogMemIndex < 0 ||
		(args.PrevLogIndex > 0 && (prevLogMemIndex >= len(rf.log) || rf.log[prevLogMemIndex].Term != args.PrevLogTerm)) {
		DPrintf("Node-[%v] AppendEntries reply false because of no entry for %v, memIdx:%v, reqTerm:[%v]",
			rf.me, args.PrevLogIndex, prevLogMemIndex, args.PrevLogTerm)
		// truncate conflict
		if prevLogMemIndex > 0 && prevLogMemIndex < len(rf.log) && rf.log[prevLogMemIndex].Term != args.Term {
			memIdx := prevLogMemIndex - 1
			term := rf.log[prevLogMemIndex].Term
			for ; memIdx >= 0 && rf.log[memIdx].Term == term; memIdx-- {
			}
			reply.ConflictTerm = term
			reply.ConflictStartIndex = rf.log[memIdx+1].Index
			rf.log = rf.log[0:prevLogMemIndex]
		} else if prevLogMemIndex > 0 && len(rf.log) > 0 && prevLogMemIndex > len(rf.log) {
			memIdx := len(rf.log) - 1
			reply.ConflictTerm = rf.log[memIdx].Term
			reply.ConflictStartIndex = rf.log[memIdx].Index
		}
		rf.persist()
		reply.Success = false
		return
	}
	// truncate if conflict and append
	if args.Entries != nil {
		if len(rf.log) == 0 {
			rf.log = append(rf.log, args.Entries...)
		} else {
			for idx, entry := range args.Entries {
				memIdx := prevLogMemIndex + idx + 1
				if memIdx >= len(rf.log) || rf.log[memIdx].Term != entry.Term {
					rf.log = append(rf.log[0:memIdx], args.Entries[idx:]...)
					break
				}
			}
		}
	}

	lastLogEntryIndex := 0
	if len(rf.log) > 0 {
		lastLogEntryIndex = rf.log[len(rf.log)-1].Index
		rf.firstEntryIndex = rf.log[0].Index
	}
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommitIndex > rf.commitIndex {
		newCommitIndex := Min(args.LeaderCommitIndex, lastLogEntryIndex)
		if rf.log[rf.calculateMemArrayIndex(newCommitIndex)].Term == args.Term {
			rf.commitIndex = newCommitIndex
			rf.tryApplyMsg()
		}
	}
	rf.persist()
	reply.Success = true
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	done := make(chan bool, 1)
	go func() {
		done <- rf.peers[server].Call("Raft.RequestVote", args, reply)
	}()
	select {
	case ok := <-done:
		return ok
	case <-time.After(time.Duration(time.Millisecond * 30)):
		return false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	done := make(chan bool, 1)
	go func() {
		done <- rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}()
	select {
	case ok := <-done:
		return ok
	case <-time.After(time.Duration(time.Millisecond * 25)):
		return false
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	DPrintf("Node-[%v] receive command [%v].", rf.me, command)
	rf.statMu.Lock()
	rf.mu.Lock()

	term := rf.currentTerm
	if rf.state != LEADER {
		rf.mu.Unlock()
		rf.statMu.Unlock()
		TPrintf("")
		return -1, term, false
	}
	if !rf.readyForServing {
		rf.mu.Unlock()
		rf.statMu.Unlock()
		return -1, term, true
	}

	// Your code here (2B).
	prevLogIndex := 0
	if len(rf.log) > 0 {
		prevLogIndex = rf.log[len(rf.log)-1].Index
	}
	newEntry := LogEntry{}
	newEntry.Term = rf.currentTerm
	newEntry.Index = prevLogIndex + 1
	newEntry.Command = command
	// append entry to local log
	rf.log = append(rf.log, newEntry)
	TPrintf("Node-[%v] leader append log entry for term:%v index:%v command:%v.",
		rf.me, rf.currentTerm, newEntry.Index, newEntry.Command)
	rf.firstEntryIndex = rf.log[0].Index

	rf.matchIndex[rf.me] = newEntry.Index

	latch := AppendEntryHandler{waitGroup: &sync.WaitGroup{}, threshold: rf.majorityCount}
	latch.onResp(true)
	latch.waitGroup.Add(1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := rf.generateAppendReq(i, false)
		go rf.newAppendEntryTask(i, &args, &latch)
	}

	rf.mu.Unlock()

	latch.waitGroup.Wait()
	// respond after entry applied to state machine
	rf.mu.Lock()
	// a leader never overwrites or deletes entries in its log; it only appends new entries
	if latch.isOk() {
		rf.tryUpdateCommitIdx()
	}
	rf.persist()
	DPrintf("Node-[%v] isOk:%v, index:%v, term:%v command:[%v], logSize:[%v]",
		rf.me, latch.isOk(), newEntry.Index, term, command, len(rf.log))
	rf.mu.Unlock()
	rf.statMu.Unlock()
	return newEntry.Index, term, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	start := false
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		if start {
			time.Sleep(time.Duration(100) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(rand.Int63n(150)) * time.Millisecond)
		}
		start = true
		rf.mu.Lock()
		if rf.state == LEADER ||
			((rf.state == CANDIDATE) && CurrentMilliSeconds()-rf.electionStartTime <= rf.electionTimeout) ||
			(rf.state == FOLLOWER && CurrentMilliSeconds()-rf.lastLeaderReqTime <= rf.heartbeatTimeout) {
			// candidate -> follower may trigger another election immediately
			rf.mu.Unlock()
			continue
		}

		// if electionTimeout elapses without receiving appendEntries from current leader or granting vote for candidate
		// converting to CANDIDATE
		TPrintf("Node:[%v] duration:%v", rf.me, CurrentMilliSeconds()-rf.lastLeaderReqTime)
		WarnPrintf("Node-[%v] enter CANDIDATE state.", rf.me)
		rf.state = CANDIDATE
		rf.electionStartTime = CurrentMilliSeconds()
		rf.currentTerm += 1
		rf.votedFor = rf.me
		rf.votes = 1
		rf.heartbeatTimeout = rand.Int63n(500) + 150
		rf.electionTimeout = rand.Int63n(1000) + 500

		req := RequestVoteArgs{}
		req.Term = rf.currentTerm
		req.CandidateId = rf.me
		if len(rf.log) > 0 {
			entry := rf.log[len(rf.log)-1]
			req.LastLogTerm = entry.Term
			req.LastLogIndex = entry.Index
		}
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.newElectionTask(i, req)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) newElectionTask(peerIdx int, req RequestVoteArgs) {
	resp := RequestVoteReply{}
	ok := rf.sendRequestVote(peerIdx, &req, &resp)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if resp.VoteForTerm != rf.currentTerm {
		// ignore outdated election voting
		return
	}
	if resp.VoteGranted {
		if rf.state != CANDIDATE {
			return
		} else {
			rf.votes += 1
			if rf.votes >= rf.majorityCount {
				rf.fromCandidateToLeader()
			}
		}
	} else {
		if resp.Term > rf.currentTerm {
			rf.toFollowerState()
		}
	}
}

func (rf *Raft) fromCandidateToLeader() {
	WarnPrintf("Node-[%v] becomes leader for term [%v].", rf.me, rf.currentTerm)
	rf.state = LEADER
	rf.votedFor = -1
	rf.votes = 0
	rf.electionStartTime = 0

	rf.readyForServing = false
	rf.nextIndex = make([]int, len(rf.peers))
	prevLogIndex := 0
	if len(rf.log) > 0 {
		prevLogIndex = rf.log[len(rf.log)-1].Index
	}
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = prevLogIndex + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
}

func (rf *Raft) toFollowerState() {
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.votes = 0
	rf.electionStartTime = 0

	rf.readyForServing = false
}

func (rf *Raft) heartbeatEntryTicker() {
	fakeHandler := AppendEntryHandler{}
	sleepTime := 50
	for rf.killed() == false {
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
		rf.mu.Lock()
		if rf.state != LEADER {
			// release lock
			rf.mu.Unlock()
			continue
		}
		TPrintf("Node-[%v] heart beat ticker.", rf.me)
		rf.tryUpdateCommitIdx()
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			args := rf.generateAppendReq(i, true)
			go rf.newAppendEntryTask(i, &args, &fakeHandler)
		}
		if rf.state == LEADER && !rf.readyForServing {
			sleepTime = 10
		} else {
			sleepTime = 50
		}
		// release lock
		rf.mu.Unlock()
	}
}

func (rf *Raft) generateAppendReq(peer int, heartBeat bool) AppendEntryArgs {
	nextIndex := rf.nextIndex[peer]
	prevIndex := nextIndex - 1
	prevLogTerm := 0
	var entries []LogEntry
	memIndex := rf.calculateMemArrayIndex(nextIndex)
	prevMemIndnex := rf.calculateMemArrayIndex(prevIndex)
	if prevIndex > 0 && prevMemIndnex < len(rf.log) {
		prevLogTerm = rf.log[prevMemIndnex].Term
	}
	TPrintf("Node-[%v] nextIndex:%v memIndex:%v logSize:%v", peer, nextIndex, memIndex, len(rf.log)-memIndex)
	if nextIndex >= 1 && memIndex < len(rf.log) {
		if heartBeat {
			if memIndex < len(rf.log) {
				entries = append(entries, rf.log[memIndex:]...)
			}
		} else {
			entries = append(entries, rf.log[memIndex:]...)
			if memIndex < len(rf.log) && rf.log[memIndex].Index != prevIndex+1 {
				WarnPrintf("Node-[%v] prevLogIndex != nextEntryIndex + 1, prev:%v, nextEntry:%v.", rf.me,
					prevIndex, rf.log[memIndex].Index)
			}
		}
	}

	args := AppendEntryArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = prevIndex
	args.PrevLogTerm = prevLogTerm
	args.Entries = entries
	args.LeaderCommitIndex = rf.commitIndex
	return args
}

func (rf *Raft) newAppendEntryTask(peer int, args *AppendEntryArgs, handler *AppendEntryHandler) {
	reply := AppendEntryReply{}
	TPrintf("Node:[%v] send AppendEntries to Node:[%v].", rf.me, peer)
	ok := rf.sendAppendEntries(peer, args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		handler.onResp(false)
		return
	}
	if reply.Term > rf.currentTerm {
		rf.toFollowerState()
		handler.onResp(false)
	}

	if rf.state != LEADER {
		handler.onResp(false)
		return
	}
	if args.Term != rf.currentTerm || (rf.nextIndex[peer] >= 0 && args.PrevLogIndex+1 != rf.nextIndex[peer]) {
		DPrintf("Node-[%v] ignore outdated AppendEntry response.", rf.me)
		handler.onResp(false)
		return
	}

	if reply.Success == false {
		beforeNextIndex := rf.nextIndex[peer]
		// decrement nextIndex and retry
		if reply.ConflictTerm > 0 && rf.nextIndex[peer] >= 1 {
			prevIndex := rf.nextIndex[peer] - 2
			for ; prevIndex > 0 && prevIndex >= reply.ConflictStartIndex; prevIndex-- {
				memIdx := rf.calculateMemArrayIndex(prevIndex)
				if rf.log[memIdx].Term == reply.ConflictTerm {
					break
				}
			}
			rf.nextIndex[peer] = prevIndex + 1
		} else if rf.nextIndex[peer] >= 1 {
			rf.nextIndex[peer] = rf.nextIndex[peer] - 1
		} else {
			TPrintf("Node-[%v] skip decrease Node-[%v] rf.nextIndex[peer] may be negative", rf.me, peer)
		}
		handler.onResp(false)
		DPrintf("Node-[%v] decrease Node-[%v] nextIndex from [%v] to [%v], conflictTerm:%v, conflictIndex:%v.",
			rf.me, peer, beforeNextIndex, rf.nextIndex[peer], reply.ConflictTerm, reply.ConflictStartIndex)
		argsNew := rf.generateAppendReq(peer, true)
		go rf.newAppendEntryTask(peer, &argsNew, handler)
	} else {
		// update nextIndex and matchIndex for follower
		rf.nextIndex[peer] = Max(rf.nextIndex[peer], args.PrevLogIndex+len(args.Entries)+1)
		rf.matchIndex[peer] = Max(rf.matchIndex[peer], args.PrevLogIndex+len(args.Entries))
		DPrintf("Node-[%v] update Node-[%v] nextIndex to [%v], matchIndex to [%v]", rf.me, peer, rf.nextIndex[peer],
			rf.matchIndex[peer])
		if rf.readyForServing == false {
			count := 0
			for i := 0; i < len(rf.peers); i++ {
				if rf.matchIndex[peer]+1 == rf.nextIndex[peer] {
					count++
				}
			}
			rf.readyForServing = count+1 >= len(rf.peers)
			InfoPrintf("Node-[%v] ready for serving!", rf.me)
		}
		handler.onResp(true)
	}
}

func (rf *Raft) tryUpdateCommitIdx() {
	if rf.state != LEADER {
		return
	}
	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
	count := 0
	idxArr := make([]int, 0)
	for _, idx := range rf.matchIndex {
		if idx <= 0 {
			continue
		}
		entry := rf.log[rf.calculateMemArrayIndex(idx)]
		if entry.Term == rf.currentTerm {
			idxArr = append(idxArr, entry.Index)
			count++
		}
	}
	if count >= rf.majorityCount {
		sort.Ints(idxArr)
		rf.commitIndex = Max(rf.commitIndex, idxArr[count-rf.majorityCount])
		InfoPrintf("Node-[%v] Leader update commitIdx to %v", rf.me, rf.commitIndex)
	}
	rf.tryApplyMsg()
}

func (rf *Raft) tryApplyMsg() {
	if rf.commitIndex > rf.lastAppliedIndex {
		for i := rf.lastAppliedIndex + 1; i <= rf.commitIndex; i++ {
			entry := rf.log[rf.calculateMemArrayIndex(i)]
			applyMsg := ApplyMsg{}
			applyMsg.Command = entry.Command
			applyMsg.CommandValid = true
			applyMsg.CommandIndex = entry.Index
			TPrintf("Node-[%v] apply channel for commandIndex:[%v]", rf.me, entry.Index)
			rf.applyChan <- applyMsg
			rf.lastAppliedIndex = i
		}
		InfoPrintf("Node-[%v], isLeader:%v,  update LastAppliedIndex to %v", rf.me, rf.state == LEADER,
			rf.lastAppliedIndex)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.majorityCount = len(rf.peers)/2 + 1
	rf.state = FOLLOWER
	rf.applyChan = applyCh
	rf.heartbeatTimeout = rand.Int63n(500) + 150
	rf.electionTimeout = rand.Int63n(1000) + 500
	rf.lastAppliedIndex = 0
	rf.firstEntryIndex = 0

	TPrintf("Node-[%v] election timeout:[%v] ms, heartbeat timeout:[%v]ms", rf.me, rf.electionTimeout,
		rf.heartbeatTimeout)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.heartbeatEntryTicker()

	return rf
}
