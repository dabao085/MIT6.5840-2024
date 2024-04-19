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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

const (
	HeartbeatTimeout = 100 // 100 ms
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int // who I vote?
	log         []*LogEntry

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders, reinitialize after election
	nextIndex  []int
	matchIndex []int

	// other state
	role          Role
	lastRenewTime time.Time
	rd            *rand.Rand
	votedCount    int // how many votes have I got?
}

type LogEntry struct {
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lastRenewTime = time.Now()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
	}

	if args.LeaderCommit > rf.commitIndex {
		// todo:
	}

	//log.Printf("%d get AppendEntries LeaderId: %d term: %d\n", rf.me, args.LeaderId, args.Term)
	rf.role = Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
	}
	reply.Term = rf.currentTerm
	reply.Success = true
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	isleader = rf.role == Leader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
// as a Follower, respond to RPCs from candidates and leaders
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.lastRenewTime = time.Now()
	if args.Term < rf.currentTerm {
		//log.Printf("218: me:%d %d < %d\n", rf.me, args.Term, rf.currentTerm)
		reply.VoteGranted = false
	} else if args.Term > rf.currentTerm {
		//log.Printf("221: me:%d %d > %d\n", rf.me, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1 // ready to vote again in new term
	}

	//log.Printf("226: me:%d rf.votedFor %d\n", rf.me, rf.votedFor)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// todo: check candidate's log is at least as up-to-date as receiver's log
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	// todo: do I need to get my lastRenewTime updated?
	if reply.Term > rf.currentTerm {
		//log.Printf("%d 270: reply.Term: %d > rf.currentTerm %d\n", rf.me, reply.Term, rf.currentTerm)
		rf.role = Follower
	}

	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) electLeader() {
	rf.currentTerm += 1
	rf.role = Candidate
	rf.votedFor = rf.me // vote for self
	rf.votedCount = 1
	rf.lastRenewTime = time.Now() // reset election timer

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me, // please vote for me
		LastLogIndex: 0,     // todo: fill this field
		LastLogTerm:  0,     // todo: fill this field
	}

	// send RequestVote RPCs to all other servers
	for i := 0; i < len(rf.peers); i++ {
		// skip self
		if i == rf.me {
			continue
		}
		// collect votes
		//log.Printf("%d collect votes from %d\n", rf.me, i)
		go rf.collectVotes(i, args)
	}
}

func (rf *Raft) collectVotes(index int, args *RequestVoteArgs) {
	voteGranted := rf.getVoteResult(index, args)

	if !voteGranted {
		//log.Printf("%d from %d voteGranted failed\n", rf.me, index)
		return
	}
	//log.Printf("%d from %d voteGranted successed\n", rf.me, index)

	if rf.votedCount > len(rf.peers)/2 {
		//log.Printf("%d is already leader\n", rf.me)
		return
	}

	rf.votedCount += 1
	if rf.votedCount > len(rf.peers)/2 {
		rf.role = Leader
		//log.Printf("%d is leader now\n", rf.me)
		go rf.sendHeartbeat()
	}
}

func (rf *Raft) getVoteResult(index int, args *RequestVoteArgs) bool {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(index, args, &reply)
	if !ok {
		return false
	}

	if reply.Term > rf.currentTerm {
		//log.Printf("%d 376: reply.Term: %d > rf.currentTerm %d\n", rf.me, reply.Term, rf.currentTerm)
		rf.role = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
	}

	return reply.VoteGranted
}

// send heartbeat as a leader, it should be in a loop
func (rf *Raft) sendHeartbeat() {
	//log.Printf("%d start to send heartbeat\n", rf.me)
	for !rf.killed() {
		for i := 0; i < len(rf.peers); i++ {
			// skip self
			if i == rf.me {
				continue
			}

			if rf.role != Leader {
				//log.Printf("%d not a leader anymore\n", rf.me)
				return
			}

			// empty args as heartbeat
			args := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			reply := AppendEntriesReply{}
			//log.Printf("%d call Raft.AppendEntries to %d\n", rf.me, i)
			ok := rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
			//log.Printf("%d send Raft.AppendEntries to %d\n", rf.me, i)
			if !ok {
				//log.Printf("%d failed to call Raft.AppendEntries to %d\n", rf.me, i)
			} else {
				rf.lastRenewTime = time.Now()
				//log.Printf("%d success to call Raft.AppendEntries to %d\n", rf.me, i)
			}

			if reply.Term > rf.currentTerm {
				//log.Printf("%d 406: %d > %d\n", rf.me, reply.Term, rf.currentTerm)
				rf.currentTerm = reply.Term
				rf.role = Follower
				rf.votedFor = -1
				return
			}
		}

		// sleep for a while
		time.Sleep(time.Duration(HeartbeatTimeout) * time.Millisecond)
	}
	//log.Printf("%d is killed\n", rf.me)
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		// election timeouts(should be bigger than 150 to 300 slightly)
		randElectionTimeout := int(rf.rd.Float64()*150) + 400
		//log.Printf("%d randElectionTimeout: %d\n", rf.me, randElectionTimeout)
		if time.Since(rf.lastRenewTime) > time.Duration(randElectionTimeout)*time.Millisecond {
			if rf.role != Leader {
				// a leader election should be started
				//log.Printf("%d ready to electLeader\n" , rf.me)
				go rf.electLeader()
			} else { // 当前Leader（自己）失联
				//log.Printf("Leader自己失联\n")
				rf.role = Follower
				rf.votedFor = -1
			}
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		//log.Printf("%d sleep for %d millisecond to check if a leader election should be started", rf.me, ms)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = Follower
	rf.lastRenewTime = time.Now()
	rf.rd = rand.New(rand.NewSource(int64(rf.me)))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
