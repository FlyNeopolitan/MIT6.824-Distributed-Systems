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
	//"crypto/rand"
	//"fmt"
	//"fmt"
	"bytes"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"math/rand"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

var (
	CLOCK_UNIT                 = 100 // ms
	MaxInt                     = math.MaxInt32
	follower                   = "followers"
	candidate                  = "candidate"
	leader                     = "leader"
	ElectionTimeoutLowerBound  = 500  // ms
	ElectionTimeoutUpperBound  = 1000 // ms
	CandidateTimeoutLowerBound = 500  // ms
	CandidateTimeoutUpperBound = 1000 // ms
	HeartBeatsRate             = 150  // ms
	Max                        = func(a, b int) int { return int(math.Max(float64(a), float64(b))) }
	Min                        = func(a, b int) int { return int(math.Min(float64(a), float64(b))) }
	containEntry               = "containEntry"
	conflictEntry              = "conflictEntry"
	missingEntry               = "missingEntry"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg       // apply channel

	// Your data here (2A, 2B, 2C).
	// logic control data
	serverType      string       // type of server: follower, candidate or leader
	clock           Clock        // Clock
	applyCond       *sync.Cond   // conditional variables for applying
	replicationCond []*sync.Cond // conditional variables for replication

	// Persistent state on all servers
	currentTerm int   // latest term server has seen
	votedFor    int   // candidate that received vote in current term
	logs        []Log // log entries; each entry contains command and term when entry was received by leader

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// Volatile state on leaders
	nextIndex  []int // index of next log entry to send to the server for each server
	matchIndex []int // index of highest log entry knwon to be replicated on server for each server

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type Log struct {
	Command      interface{}
	TermReceived int
}

type Clock struct {
	clockTime time.Time  // current time in clock
	clockMu   sync.Mutex // clock's mutex
	clockCond *sync.Cond // clock's conditional variables
	timeLimit int        // when time reaches time Limit, the clock would remind sleeping thread
	ifClean   bool       // set to true to clean all waiting clocks
	kill      bool       // set to true to kill the clock
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
	isleader = rf.serverType == leader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VotedFor    int 
	var Logs        []Log
	if d.Decode(&CurrentTerm) != nil || d.Decode(&VotedFor) != nil || d.Decode(&Logs) != nil {
		DPrintf("Error in decoding")
	} else {
		rf.currentTerm = CurrentTerm
		rf.votedFor    = VotedFor
		rf.logs        = Logs
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandiateId   int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidates' last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candiate to update itself
	VoteGranted bool // true means candiate received vote
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	reply.VoteGranted = false
	if rf.currentTerm < args.Term {
		rf.toFollower(args.Term)
	}
	if (rf.votedFor == -1) && rf.upToDate(args) {
		reply.VoteGranted = true
		rf.votedFor = args.CandiateId
		rf.persist()
		rf.clock.reset() // restart your election timer when granting a vote to another peer.
	} 
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

// (2B)
// Return true if the logs in args are more updated or same updated
func (rf *Raft) upToDate(args *RequestVoteArgs) bool {
	if rf.currentTerm > args.Term {
		return false
	}
	receiverLogIdx, receiverLogTerm := rf.lastLog()
	return args.LastLogTerm > receiverLogTerm ||
		(args.LastLogTerm == receiverLogTerm && args.LastLogIndex >= receiverLogIdx)
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

type AppendEntriesArgs struct {
	Term         int   // leader???s term
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // log entries to store
	LeaderCommit int   // leader's commitIndex
}

type AppendEntriesReply struct {
	Term          int  // current Term, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex int  // index of conflict entry: for accelerated log backtracking optimization
	ConflictTerm  int  // term  of conflict entry: for accelerated log backtracking optimization
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	// Reply false if term < currentTerm
	if rf.currentTerm > args.Term {
		return
	}
	// Maintain server's type
	rf.clock.reset() //reset timing for timeout
	if rf.currentTerm < args.Term || rf.serverType == candidate {
		rf.toFollower(args.Term)
		reply.Term = rf.currentTerm
	}
	// Start Appending
	if !preEntryMatch(rf.logs, args, reply) {
		return
	}
	reply.Success = true
	numExists := 0
loop:
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		switch CheckEntry(rf.logs, index, entry.TermReceived) {
		case conflictEntry:
			rf.logs = rf.logs[:index] // delete the existing entry and all that follow it
			break loop
		case containEntry:
			numExists += 1
		default:
			break loop
		}
	}
	lastNewEntry := args.PrevLogIndex + len(args.Entries)
	if len(args.Entries) > 0 {
		logBefore, logAfter := rf.logs[:args.PrevLogIndex + 1 + numExists], rf.logs[args.PrevLogIndex + 1 + numExists:]
		rf.logs = append(logBefore, args.Entries[numExists:]...)
		rf.logs = append(rf.logs, logAfter...)
		rf.persist()
	}
	rf.updateCommitFollower(args.LeaderCommit, lastNewEntry)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.logs) + 1
	term := rf.currentTerm
	isLeader := rf.serverType == leader

	// Your code here (2B).
	if isLeader {
		rf.logs = append(rf.logs, Log{Command: command, TermReceived: rf.currentTerm})
		rf.persist()
		for peer := range rf.peers {
			if peer != rf.me {
				rf.replicationCond[peer].Broadcast()
			}
		}
	}

	return index, term, isLeader
}

func (rf *Raft) logReplication(oldTerm int) {
	// start making agreements
	for peer := range rf.peers {
		if peer != rf.me {
			go rf.logReplicationFor(peer, oldTerm)
		}
	}
}

func (rf *Raft) logReplicationFor(server int, oldTerm int) {
	for {
		rf.mu.Lock()
		for !rf.needsReplication(server) {
			rf.replicationCond[server].Wait()
		}
		if rf.currentTerm != oldTerm || rf.serverType != leader || rf.killed() {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		rf.AppendEntriesFor(false, server, oldTerm)
	}
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
	rf.clock.killClock() //kill all waiting clock threads!
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	rf := &Raft{
		// logic control data
		peers:		     peers,
		persister:       persister,
		me:              me,
		applyCh:         applyCh,
		// logic control data
		serverType:      follower,
		replicationCond: make([]*sync.Cond, len(peers)),
		// Persistent state on all servers
		currentTerm:     1,
		votedFor: 	     -1,
		logs:            make([]Log, 0),
		// Volatile state on all servers
		commitIndex:     -1,
		lastApplied:     -1,
		// Volatile state on all servers
		nextIndex: 	     make([]int, len(peers)),
		matchIndex:      make([]int, len(peers)),
	}
	// Your initialization code here (2A, 2B, 2C).
	rand.Seed(time.Now().UnixNano())
	rf.applyCond = sync.NewCond(&rf.mu)
	for i := 0; i < len(peers); i++ {
		rf.replicationCond[i] = sync.NewCond(&rf.mu)
	}
	rf.clock.createClock()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// Every server needs to perform timeoutCheck and periodic apply check
	go rf.timeoutCheck()
	go rf.periodicApplyCheck()

	return rf
}

func (rf *Raft) periodicApplyCheck() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
		}
		rf.lastApplied += 1
		command, lastApplied := rf.logs[rf.lastApplied].Command, rf.lastApplied
		rf.mu.Unlock()
		rf.applyCommand(true, command, lastApplied)
	}
}

func (rf *Raft) timeoutCheck() {
	for !rf.killed() {
		timeLimit := MaxInt
		rf.mu.Lock()
		switch rf.serverType {
		case follower:
			rf.mu.Unlock()
			timeLimit = randInt(ElectionTimeoutLowerBound, ElectionTimeoutUpperBound)
			rf.clock.wait(timeLimit)
			rf.toCandidate()
		case candidate:
			rf.mu.Unlock()
			go rf.startElection()
			timeLimit = randInt(CandidateTimeoutLowerBound, CandidateTimeoutUpperBound)
			rf.clock.wait(timeLimit)
		case leader: // don't need to perform timeout check!
			rf.mu.Unlock()
			rf.clock.wait(timeLimit)
		}
	}
}

// The function will start election for candidate:
// 1.Increment currentTerm 2.Vote for self 3.Reset election timer
// 4.Send RequestVote RPCs to all other servers
func (rf *Raft) startElection() {
	// (1), (2), (3)
	rf.mu.Lock()
	rf.currentTerm += 1
	oldTerm := rf.currentTerm
	rf.votedFor = rf.me
	lastLogIdx, lastLogTerm := rf.lastLog()
	rf.persist()
	rf.mu.Unlock()
	// (4)
	counts := 1
	for peer := range rf.peers {
		args := RequestVoteArgs{CandiateId: rf.me, Term: oldTerm, LastLogIndex: lastLogIdx, LastLogTerm: lastLogTerm}
		reply := RequestVoteReply{}
		go func(server int) {
			if server != rf.me && rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm { // convert to follower
					rf.toFollower(reply.Term)
				}
				if reply.VoteGranted && reply.Term == rf.currentTerm { // check if we have the major votes and convert to leader
					counts += 1
					if rf.continueElection(oldTerm) && rf.hasMajorVotes(counts) {
						rf.toLeader()
					}
				}
				rf.mu.Unlock()
			}
		}(peer)
	}
}

func (rf *Raft) startHeartBeat(oldTerm int) {
	for rf.continueHeartBeat(oldTerm) {
		for peer := range rf.peers {
			heartbeat := func(rf *Raft, server int) {
				rf.AppendEntriesFor(true, server, oldTerm)
			}
			if peer != rf.me {
				go heartbeat(rf, peer)
			}
		}
		time.Sleep(time.Duration(HeartBeatsRate) * time.Millisecond)
	}
}

// perform append entries to target server
// appending starting at idx and requires entries starting at idx
func (rf *Raft) AppendEntriesFor(isHeartBeat bool, targetServer int, term int) {
	// generate args
	rf.mu.Lock()
	var idx int
	switch isHeartBeat {
	case true: 
		idx = len(rf.logs)
	case false:
		idx = rf.nextIndex[targetServer]
	}
	entries := rf.logs[idx:]
	prevLogIdx, prevLogTerm, commitIdx := idx - 1, rf.logTermAt(idx - 1), rf.commitIndex
	rf.mu.Unlock()
	// send RPC, and handle responses
	args := AppendEntriesArgs{Term: term, PrevLogIndex: prevLogIdx,
		Entries: entries, PrevLogTerm: prevLogTerm, LeaderCommit: commitIdx}
	reply := AppendEntriesReply{}
	if rf.sendAppendEntries(targetServer, &args, &reply) {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm { // current Term is out-dated!
			rf.toFollower(reply.Term)
		} 
		if term == rf.currentTerm && reply.Term == rf.currentTerm { // the data is not out-dated
			switch reply.Success {
			case true:
				rf.nextIndex[targetServer] = idx + len(entries)
				rf.matchIndex[targetServer] = idx + len(entries) - 1
				rf.updateCommitLeader()
			case false:
				rf.nextIndex[targetServer] = Min(rf.nextIndex[targetServer], getNextIdx(rf.logs, &reply))
				if rf.needsReplication(targetServer) {
					rf.replicationCond[targetServer].Broadcast()
				}
			}
		}
		rf.mu.Unlock()
	}
}

func getNextIdx(logs []Log, reply *AppendEntriesReply) (nextIdx int) {
	conflictIdx, conflictTerm := reply.ConflictIndex, reply.ConflictTerm
	if conflictTerm > 0 {
		upper := Min(conflictIdx, len(logs) - 1)
		if find := search(logs, conflictTerm, false, 0, upper); find >= 0 {
			nextIdx = find + 1
			return
		}
	}
	return conflictIdx
}

/***************************** clock related functions ********************************/

// try to reset the clock timing
// will NOT clean ANY waiting threads on clock
func (clock *Clock) reset() {
	clock.clockMu.Lock()
	clock.clockTime = time.Now()
	clock.clockMu.Unlock()
}

// will clean ALL waiting threads on clock
func (clock *Clock) clean() {
	clock.clockMu.Lock()
	clock.ifClean = true
	clock.clockCond.Broadcast()
	clock.clockMu.Unlock()
}

// initialize the clock
// create a background clock that will try to wake up any waiting threads on clock once time has reached upper liits
func (clock *Clock) createClock() {
	clock.timeLimit = MaxInt
	clock.kill = false
	clock.ifClean = false
	clock.clockCond = sync.NewCond(&clock.clockMu)
	go func() {
		for {
			clock.clockCond.Broadcast()
			time.Sleep(time.Duration(CLOCK_UNIT) * time.Millisecond)
			if clock.kill {
				break
			}
		}
	}()
}

// sleep/wait until clock has reached time limit
func (clock *Clock) wait(timeLimit int) {
	clock.clockMu.Lock()
	clock.ifClean = false
	clock.clockTime = time.Now()
	timeElapse := time.Since(clock.clockTime)
	for timeElapse < time.Duration(timeLimit) * time.Millisecond && !clock.kill && !clock.ifClean {
		clock.clockCond.Wait()
		timeElapse = time.Since(clock.clockTime)
	}
	clock.clockMu.Unlock()
}

// kill all clock-related threads
func (clock *Clock) killClock() {
	clock.clockMu.Lock()
	clock.kill = true
	clock.clockCond.Broadcast()
	clock.clockMu.Unlock()
}

/******************************************** Tiny Helpers ***********************************/

func randInt(min int, max int) int {
	return rand.Intn(max - min) + min
}

func (rf *Raft) continueElection(oldTerm int) bool {
	return rf.serverType == candidate && oldTerm == rf.currentTerm
}

func (rf *Raft) hasMajorVotes(counts int) bool {
	return counts >= 1 + int(math.Floor(float64(len(rf.peers)) / 2.0))
}

func (rf *Raft) continueHeartBeat(oldTerm int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.serverType == leader && oldTerm == rf.currentTerm && !rf.killed()
}

// only candidate can become leader
// nextIndex: initialized to leader last log index + 1
// matchIndex: initialized to -1
func (rf *Raft) toLeader() {
	rf.serverType = leader
	oldTerm := rf.currentTerm
	go rf.startHeartBeat(oldTerm)
	go rf.logReplication(oldTerm)
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = -1, len(rf.logs)
	}
	rf.clock.clean()
}

// candidate, follower, leads all could be follower
func (rf *Raft) toFollower(newTerm int) {
	/* to Follower */
	if rf.currentTerm < newTerm {
		rf.votedFor = -1
		rf.currentTerm = newTerm
		rf.persist()
	}
	if rf.serverType != follower {
		rf.clock.clean()
	}
	rf.serverType = follower
}

// only follower or candidate can become candidate
func (rf *Raft) toCandidate() {
	rf.mu.Lock()
	rf.serverType = candidate
	rf.mu.Unlock()
}

func (rf *Raft) updateCommitFollower(leaderCommit int, lastNewEntry int) {
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	originalCommit := rf.commitIndex
	if leaderCommit > rf.commitIndex {
		rf.commitIndex = Min(leaderCommit, lastNewEntry)
		if rf.commitIndex > originalCommit {
			rf.applyCond.Broadcast()
		}
	}
}

// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ??? N,
// and log[N].term == currentTerm:
// set commitIndex = N
func (rf *Raft) updateCommitLeader() {
	for N := len(rf.logs) - 1; N > rf.commitIndex; N-- {
		count := 1
		for peer := range rf.peers {
			if peer != rf.me && rf.matchIndex[peer] >= N {
				count += 1
			}
		}
		if rf.hasMajorVotes(count) && rf.logs[N].TermReceived == rf.currentTerm {
			rf.commitIndex = N
			rf.applyCond.Broadcast()
			break
		}
	}
}

func (rf *Raft) applyCommand(valid bool, command interface{}, index int) {
	// apply command
	applyMsg := ApplyMsg{CommandValid: valid, Command: command, CommandIndex: index + 1}
	rf.applyCh <- applyMsg
}

func preEntryMatch(logs []Log, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	switch CheckEntry(logs, args.PrevLogIndex, args.PrevLogTerm) {
	case missingEntry:
		reply.ConflictIndex, reply.ConflictTerm = len(logs), 0
		return false
	case conflictEntry:
		reply.ConflictTerm = logs[args.PrevLogIndex].TermReceived
		reply.ConflictIndex = search(logs, reply.ConflictTerm, true, 0, args.PrevLogIndex)
		return false
	default:
		return true
	}
}

// check status of entry(index, term) in logs:
// containEntry, conflictEntry or missingEntry
func CheckEntry(logs []Log, index int, term int) string {
	if index < 0 {
		return containEntry
	}
	length := len(logs)
	if index >= length {
		return missingEntry
	}
	switch logs[index].TermReceived {
	case term:
		return containEntry
	default:
		return conflictEntry
	}
}

func (rf *Raft) lastLog() (int, int) {
	lastLogIdx := len(rf.logs) - 1
	lastLogTerm := 0
	if lastLogIdx >= 0 {
		lastLogTerm = rf.logs[lastLogIdx].TermReceived
	}
	return lastLogIdx, lastLogTerm
}

func (rf *Raft) needsReplication(server int) bool {
	return len(rf.logs) - 1 >= rf.nextIndex[server]
}

// return term at log[i] of rf, return 0 if i doesn't exist
func (rf *Raft) logTermAt(i int) (int) {
	if i >= 0 {
		return rf.logs[i].TermReceived
	}
	return 0
}

func search(logs []Log, term int, first bool, low int, high int) int {
	bruteForce := func(low int, high int) int {
		for i := len(logs) - 1; i >= 0; i-- {
			log := logs[i]
			if log.TermReceived == term && !first {
				return i
			}
			if log.TermReceived == term && first {
				if i - 1 >= 0 && logs[i - 1].TermReceived == term {
					continue
				}
				return i
			}
		}
		return -1
	}
	var binarySearch func(int, int) int
	binarySearch = func(low int, high int) int {
		if high - low <= 4 {
			return bruteForce(low, high)
		}
		mid := (low + high) / 2
		if logs[mid].TermReceived < term {
			return binarySearch(mid + 1, high)
		} 
		if logs[mid].TermReceived > term {
			return binarySearch(low, mid - 1)
		} 
		// logs[mid].TermReceived == term 
		if first {
			return binarySearch(low, mid)
		} else {
			return binarySearch(mid, high)
		}
	}
	if logs[high].TermReceived == term && first {
		return bruteForce(low, high)
	}
	return binarySearch(low, high)
}
