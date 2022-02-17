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
	"math"
	"sync"
	"sync/atomic"
	"time"

	"math/rand"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

var (
	CLOCK_UNIT = 50 // ms
	MaxInt = math.MaxInt32 
	follower  = "followers"
	candidate = "candidate"
	leader    = "leader"
	ElectionTimeoutLowerBound = 500   // ms
	ElectionTimeoutUpperBound = 1000 // ms
	CandidateTimeoutLowerBound = 500  // ms
	CandidateTimeoutUpperBound = 1000 // ms
	HeartBeatsRate = 150              // ms
	Max = func (a, b int) int {return int(math.Max(float64(a), float64(b)))}
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

	// Your data here (2A, 2B, 2C).
	// logic control data
	serverType   string     // type of server: follower, candidate or leader
	clock        Clock      // Clock
	// Persistent state on all servers
	currentTerm  int   // latest term server has seen 
	votedFor	 int   // candidate that received vote in current term
	logs         []Log // log entries; each entry contains command and term when entry was received by leader

	// Volatile state on all servers
	commitIndex  int    // index of highest log entry known to be committed
	lastApplied  int    // index of highest log entry applied to state machine

	// Volatile state on leaders
	nextIndex    []int // index of next log entry to send to the server for each server
	matchIndex   []int // index of highest log entry knwon to be replicated on server for each server
	

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type Log struct {
	Command 	 interface{}
	TermReceived int
}

type Clock struct {
	clockTime    int        // current time in clock (in ms)
	clockMu      sync.Mutex // clock's mutex
	clockCond    *sync.Cond // clock's conditional variables
	timeLimit    int        // when time reaches time Limit, the clock would remind sleeping thread
	kill         bool       // set to true to kill the background clock
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
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	reply.VoteGranted = false
	if rf.currentTerm < args.Term {
		rf.toFollower(args.Term)
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandiateId) && rf.upToDate(args) {
		reply.VoteGranted = true
		rf.votedFor = args.CandiateId
	}
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

// (2B)
func (rf *Raft) upToDate(args *RequestVoteArgs) (bool) {
	return rf.currentTerm <= args.Term
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
	Term int // leader’s term
	
}

type AppendEntriesReply struct {
	Term    int  // current Term, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and pREVlOGtERM
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term { // if current term > requester's term, reject request
		return
	}
	rf.clock.reset() //reset timing for timeout
	if rf.currentTerm < args.Term || rf.serverType == candidate {
		rf.toFollower(args.Term)
	}
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
	rf.clock.killClock()  //kill all waiting clock threads!
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rand.Seed(time.Now().UnixNano())
	rf.serverType, rf.currentTerm, rf.votedFor = follower, 1, -1
	rf.clock.createClock()
	go rf.timeoutCheck()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}

func (rf *Raft) timeoutCheck() {
	for !rf.killed() {
		timeLimit := MaxInt
		rf.mu.Lock()
		serverType := rf.serverType
		rf.mu.Unlock()
		switch serverType {
		case follower:
			timeLimit = randInt(ElectionTimeoutLowerBound, ElectionTimeoutUpperBound)
			rf.clock.wait(timeLimit)
			rf.toCandidate()
		case candidate:
			go rf.startElection()
			timeLimit = randInt(CandidateTimeoutLowerBound, CandidateTimeoutUpperBound)
			rf.clock.wait(timeLimit)
		case leader: // don't need to perform timeout check!
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
	rf.mu.Unlock()
	rf.clock.reset()
	// (4)
	counts := 0
	for peer := range rf.peers {
		args := RequestVoteArgs{CandiateId: rf.me, Term: oldTerm}
		reply := RequestVoteReply{}
		go func(server int) {
			if (rf.sendRequestVote(server, &args, &reply)) {
				rf.mu.Lock()
				if (reply.Term > rf.currentTerm) { // convert to follower
					rf.toFollower(reply.Term)
				}
				if reply.VoteGranted { // check if we have the major votes and convert to leader
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

func (rf *Raft) startHeartBeat() {
	rf.mu.Lock()
	oldTerm := rf.currentTerm
	rf.mu.Unlock()
	for {
		if !rf.continueHeartBeat(oldTerm) {
			break
		}
		for peer := range rf.peers {
			heartbeat := func(server int) {
				args := AppendEntriesArgs{Term: oldTerm}
				reply := AppendEntriesReply{}
				if rf.sendAppendEntries(server, &args, &reply) {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.toFollower(reply.Term)
					}
					rf.mu.Unlock()
				}
			}
			if peer != rf.me {
				go heartbeat(peer)
			}
		}
		time.Sleep(time.Duration(HeartBeatsRate) * time.Millisecond)
	}
}

/***************************** clock related functions ********************************/

// try to reset the clock timing
// will NOT clean ANY waiting threads on clock
func (clock *Clock) reset() {
	clock.clockMu.Lock()
	clock.clockTime = 0
	clock.clockMu.Unlock()
}

// will clean ALL waiting threads on clock
func (clock *Clock) clean() {
	clock.clockMu.Lock()
	clock.timeLimit = 0
	clock.clockCond.Broadcast()
	clock.clockMu.Unlock()
}

// initialize the clock
// create a background clock that will try to wake up any waiting threads on clock once time has reached upper liits
func (clock *Clock) createClock() {
	clock.clockTime = 0
	clock.clockCond = sync.NewCond(&clock.clockMu)
	clock.timeLimit = MaxInt
	clock.kill      = false
	go func () {
		for {
			time.Sleep(time.Duration(CLOCK_UNIT) * time.Millisecond)
			clock.clockMu.Lock()
			if clock.kill {
				break
			}
			clock.clockTime += CLOCK_UNIT
			if (clock.clockTime >= clock.timeLimit) {
				clock.clockCond.Broadcast()
			}
			clock.clockMu.Unlock()
		}
	} ()
}

// sleep/wait until clock has reached time limit
func (clock *Clock) wait(timeLimit int) {
	clock.clockMu.Lock()
	clock.clockTime, clock.timeLimit = 0, timeLimit
	for clock.clockTime < clock.timeLimit {
		clock.clockCond.Wait()
	}
	clock.clockMu.Unlock()
}

// kill all clock-related threads
func (clock *Clock) killClock() {
	clock.clockMu.Lock()
	clock.kill = true
	clock.clockMu.Unlock()
	clock.clean()
}

/******************************************** Tiny Helpers ***********************************/

func randInt(min int, max int) (int) {
	return rand.Intn(max - min) + min
}

func (rf *Raft) continueElection(oldTerm int) bool{
	return rf.serverType == candidate && oldTerm == rf.currentTerm && !rf.killed()
}

func (rf *Raft) hasMajorVotes(counts int) bool {
	return counts >= 1 + int(math.Floor(float64(len(rf.peers)) / 2.0)) && !rf.killed()
}

func (rf *Raft) continueHeartBeat(oldTerm int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.serverType == leader && oldTerm == rf.currentTerm
}

// only candidate can become leader
func (rf *Raft) toLeader() {
	rf.clock.clean()
	rf.serverType = leader
	go rf.startHeartBeat()
}

// candidate, follower, leads all could be folloer
func (rf *Raft) toFollower(newTerm int) {
	/* to Follower */
	rf.clock.clean()
	if rf.currentTerm < newTerm {
		rf.votedFor = -1
	}
	rf.currentTerm = newTerm
	rf.serverType = follower
}

func (rf *Raft) toCandidate() {
	rf.mu.Lock()
	rf.serverType = candidate
	rf.mu.Unlock()
}


