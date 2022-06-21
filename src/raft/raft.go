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
	"math/rand"
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
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
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
	lastLeaderReqTime int64
	electionStartTime int64
	votedFor          int
	votes             int

	currentTerm      int
	log              []LogEntry
	commitIndex      int
	lastAppliedIndex int
	firstEntryIndex  int

	// leader only
	nextIndex  []int
	matchIndex []int
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	Term    int
	Success bool
}

func (rf *Raft) calculateLogIndex() int {
	return rf.lastAppliedIndex - rf.firstEntryIndex
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	TPrintf("Node-[%v] receive requestVote from Node-[%v].", rf.me, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteForTerm = args.Term
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	lastLogTerm := 0
	if CurrentMilliSeconds()-rf.lastLeaderReqTime <= rf.electionTimeout {
		// reject vote, if election-timeout event doesn't happen in this node
		reply.VoteGranted = false
		return
	}
	if rf.lastAppliedIndex > 0 {
		lastLogTerm = rf.log[rf.calculateLogIndex()].Term
	}
	if args.Term > rf.currentTerm && (rf.state == CANDIDATE || rf.state == LEADER) {
		// candidate's term is new and this node is in CANDIDATE state, this node return to follower state
		rf.toFollowerState()
	}
	if rf.state == FOLLOWER && (rf.votedFor < 0 || rf.votedFor == args.CandidateId) && args.Term >= rf.currentTerm &&
		args.LastLogTerm >= lastLogTerm && args.LastLogIndex >= rf.lastAppliedIndex {
		// voteFor is null or voteFor is candidateId
		// candidate's term is gte this node's currentTerm
		// candidate's log is at least up-to-date as this node
		rf.votedFor = args.CandidateId
		DPrintf("Node-[%v] vote for Node-[%v].", rf.me, args.CandidateId)
		reply.VoteGranted = true
	}
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	DPrintf("Node-[%v] receive appendEntries from Node-[%v].", rf.me, args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.killed() == true {
		reply.Success = false
		return
	}
	rf.lastLeaderReqTime = CurrentMilliSeconds()
	if rf.currentTerm < args.Term {
		rf.toFollowerState()
		rf.currentTerm = args.Term
	}
	if rf.state == CANDIDATE {
		if rf.currentTerm <= args.Term {
			rf.toFollowerState()
			rf.currentTerm = args.Term
			reply.Success = true
		}
	} else if rf.state == LEADER {
		if rf.currentTerm < args.Term {
			rf.toFollowerState()
			rf.currentTerm = args.Term
		}
	} else {
		rf.currentTerm = args.Term
		reply.Success = true
	}
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(time.Duration(rf.electionTimeout) * time.Millisecond)
		rf.mu.Lock()
		if rf.state == LEADER ||
			(rf.state == CANDIDATE && CurrentMilliSeconds()-rf.electionStartTime <= rf.electionTimeout) ||
			(rf.state == FOLLOWER && CurrentMilliSeconds()-rf.lastLeaderReqTime <= rf.electionTimeout) {
			rf.mu.Unlock()
			continue
		}

		TPrintf("Node:[%v] duration:%v", rf.me, CurrentMilliSeconds()-rf.lastLeaderReqTime)
		DPrintf("Node-[%v] enter CANDIDATE state.", rf.me)
		rf.state = CANDIDATE
		rf.electionStartTime = CurrentMilliSeconds()
		rf.currentTerm += 1
		rf.votedFor = rf.me
		rf.votes = 1

		req := RequestVoteArgs{}
		req.Term = rf.currentTerm
		req.CandidateId = rf.me
		req.LastLogIndex = rf.lastAppliedIndex
		if rf.lastAppliedIndex > 0 {
			req.LastLogTerm = rf.log[rf.lastAppliedIndex-rf.firstEntryIndex].Term
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
	WarnPrintf("Node-[%v] becomes leader.", rf.me)
	rf.state = LEADER
	rf.votedFor = -1
	rf.votes = 0
	rf.electionStartTime = 0
}

func (rf *Raft) toFollowerState() {
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.votes = 0
	rf.electionStartTime = 0
}

func (rf *Raft) heartbeatEntryTicker() {
	for rf.killed() == false {
		time.Sleep(time.Duration(50) * time.Millisecond)
		rf.mu.Lock()
		if rf.state != LEADER {
			// release lock
			rf.mu.Unlock()
			continue
		}
		TPrintf("Node-[%v] heart beat ticker.", rf.me)
		args := AppendEntryArgs{}
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.newAppendEntryTask(i, args)
		}
		// release lock
		rf.mu.Unlock()
	}
}

func (rf *Raft) newAppendEntryTask(peer int, args AppendEntryArgs) {
	reply := AppendEntryReply{}
	ok := rf.sendAppendEntries(peer, &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.toFollowerState()
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
	rf.electionTimeout = rand.Int63n(500) + 500
	rf.lastAppliedIndex = 0
	rf.firstEntryIndex = 0

	TPrintf("Node-[%v] election timeout:[%v] ms", rf.me, rf.electionTimeout)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.heartbeatEntryTicker()

	return rf
}
