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
	"strconv"

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

func (am *ApplyMsg) print() {
	DPrintf("CommandValid:%t Command:%v CommandIndex:%d\n", am.CommandValid, am.Command, am.CommandIndex)
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
	votedFor    int // who I voted?
	log         []LogEntry

	// volatile state on all servers
	commitIndex int // index of highest log entry known to be committed(initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine(initialized to 0, increases monotonically)

	// volatile state on leaders, reinitialize after election
	nextIndex  []int // for each server(follower), index of the next log entry to send to that server(initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server(...)

	applyCh chan ApplyMsg // apply log entry and send it to tester/client

	// other state
	role          Role
	lastRenewTime time.Time
	rd            *rand.Rand
	votedCount    int // how many votes have I got?
}

type LogEntry struct {
	Command interface{} // command for state machine
	Term int // when entry was received by leader, first term is 1
	Index int // the index of this LogEntry in 'log []*LogEntry', start from 1?
}

func (le *LogEntry) print() {
	DPrintf("Command:%v, Term:%d, Index:%d\n", le.Command, le.Term, le.Index)
}

type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int // so follower can redirect clients
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int // term of PrevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat, may send more than one for efficiency)
	LeaderCommit int // leader's commitIndex
}

func (ae *AppendEntriesArgs) print() {
	DPrintf("Term:%d, LeaderId:%d, PrevLogIndex:%d, PrevLongTerm:%d, Entries:%v, LeaderCommit:%d\n",
		ae.Term, ae.LeaderId, ae.PrevLogIndex, ae.PrevLogTerm, ae.Entries, ae.LeaderCommit)
}

type AppendEntriesReply struct {
	Term    int
	Success bool // true if follower contains entry that matches prevLogIndex and prevLogTerm
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func (rf *Raft) peek() {
	message := "Server: " + strconv.Itoa(rf.me)
	PrintLogEntries(message, rf.log)
}

// 一致性检查失败时reply.Success为false，成功时reply.Success为true
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastRenewTime = time.Now()
	// just ignore it if the term in args is out of date
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	DPrintf("Follower %d get AppendEntries from LeaderId: %d term: %d\n", rf.me, args.LeaderId, args.Term)
	reply.Term = rf.currentTerm
	rf.role = Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		//reply.Term = rf.currentTerm
		//reply.Success = true
		//return
	}

	// empty Entries in args means it's a heartbeat
	if len(args.Entries) == 0 {
		DPrintf("Follower %d 接收到心跳包 from %d\n", rf.me, args.LeaderId)
	} else {
		DPrintf("Follower %d 收到新增日志 from %d\n", rf.me, args.LeaderId)
	}

	// todo: 检查日志的一致性,还是有问题，重写一致性检查
	newestLogOfFollower := rf.log[len(rf.log) - 1]
	// args.PrevLogIndex在Follower的rf.log里没有对应的日志，返回false
	// Follower's log doesn't contain an log entry at PrevLogIndex whose term matches prevLogTerm
	DPrintf("Follower %d args.PrevLogIndex %d newestLogOfFollower.Index %d\n", rf.me, args.PrevLogIndex, newestLogOfFollower.Index)
	if args.PrevLogIndex > newestLogOfFollower.Index {
		reply.Success = false

	} else {
		// prev log match!
		DPrintf("Follower %d args.PrevLogTerm %d rf.log[args.PrevLogIndex].Term %d\n", rf.me, args.PrevLogTerm, rf.log[args.PrevLogIndex].Term)
		if args.PrevLogTerm == rf.log[args.PrevLogIndex].Term {
			message := "Follower " + strconv.Itoa(rf.me)
			// todo: 继续比较新的log entry和Follower对应的日志是否一致(如果有)
			// todo: 找到最长的一致的地方，或许只要检查新增日志对应区间的日志是否匹配(只要看一个新的日志吗？)
			var appendEntries []LogEntry
			DPrintf("Follower %d len(args.Entries) %d\n", rf.me, len(args.Entries))
			PrintLogEntries(message + " args.Entries", args.Entries)
			for i, log := range args.Entries {
				//fmt.Printf("Follower %d log.Index=%d len(rf.log)=%d log.Term=%d rf.log[log.Index].Term=%d\n",
				//	rf.me, log.Index, len(rf.log), log.Term, rf.log[log.Index].Term)
				// 这里有问题

				if log.Index >= len(rf.log) {
					appendEntries = append(appendEntries, log)
				} else {
					if log.Term != rf.log[log.Index].Term {
						rf.log = rf.log[:log.Index]
						appendEntries = args.Entries[i:]
						break
					} else {
						continue
					}
				}
			}

			// todo: append new entries to follower's log
			PrintLogEntries(message + " rf.log", rf.log)
			PrintLogEntries(message + " appendEntries", rf.log)

			rf.log = append(rf.log, appendEntries...)
			PrintLogEntries(message + " after append rf.log", rf.log)
			reply.Success = true
		} else {
			reply.Success = false
		}
	}
	//	DPrintf("(169) Follower: %d 检查日志一致性 args.PrevLogIndex: %d, len(rf.log)-1: %d\n", rf.me, args.PrevLogIndex, len(rf.log) - 1)
	//	if args.PrevLogIndex > rf.log[len(rf.log) - 1].Index {
	//		// Follower's log doesn't contain an log entry at PrevLogIndex whose term matches prevLogTerm
	//		reply.Success = false
	//		//return // ? 直接返回，这样让Leader找正确的地方
	//	} else {
	//		// Leader's prev log entry and Follower's latest log entry match!
	//		if args.PrevLogTerm == rf.log[args.PrevLogIndex].Term {
	//			// match!
	//		} else {
	//			// XXX: delete the existing entry and all that follow it
	//			// todo: set reply.Success false?
	//
	//			rf.log = rf.log[:args.PrevLogIndex]
	//		}
	//		DPrintf("Leader %d 发送给 Follower %d 的日志args.Entries的大小 %d\n", args.LeaderId, rf.me, len(args.Entries))
	//		rf.log = append(rf.log, args.Entries...) // Entries可能有多个
	//		DPrintf("Follower %d 添加完日志后，本地有效日志长度为 %d\n", rf.me, len(rf.log) - 1)
	//		reply.Success = true
	//	}

	// if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	//DPrintf("(264) Follower: %d AppendEntries args.LeaderCommit: %d rf.commitIndex=%d\n", rf.me, args.LeaderCommit, rf.commitIndex)
	if args.LeaderCommit > rf.commitIndex {
		// fixme: 日志不一致怎么办？不能盲目提交，要先找到最后一致的地方，与Leader同步后再提交
		if reply.Success {
			rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
		}
		//reply.Success = true // ?
		//reply.Term = rf.currentTerm
		//DPrintf("(269) Follower: %d AppendEntries rf.commitIndex=%d\n", rf.me, rf.commitIndex)
		//return
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastRenewTime = time.Now()
	if args.Term < rf.currentTerm {
		//DPrintf("218: me:%d %d < %d\n", rf.me, args.Term, rf.currentTerm)
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		//DPrintf("221: me:%d %d > %d\n", rf.me, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1 // ready to vote again in new term
	} else {
		//
	}

	//DPrintf("226: me:%d rf.votedFor %d\n", rf.me, rf.votedFor)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// todo: check candidate's log is at least as up-to-date as receiver's log
		// todo: 如果对方的log不比自己旧，我才给它投票
		lastLogEntry := rf.log[len(rf.log)-1]
		DPrintf("%d-%d\n", args.LastLogTerm, rf.currentTerm)
		if args.LastLogTerm > lastLogEntry.Term {
			reply.VoteGranted = true
		} else if args.LastLogTerm == lastLogEntry.Term {
			DPrintf("%d %d\n", args.LastLogIndex, rf.log[len(rf.log) - 1].Index)
			if args.LastLogIndex >= rf.log[len(rf.log) - 1].Index {
				reply.VoteGranted = true
			} else {
				reply.VoteGranted = false
			}
		} else {
			reply.VoteGranted = false
		}

		if reply.VoteGranted == true {
			rf.votedFor = args.CandidateId
		}

		//rf.votedFor = args.CandidateId
		//reply.VoteGranted = true
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		rf.lastRenewTime = time.Now()
	}

	if reply.Term > rf.currentTerm {
		//DPrintf("%d 270: reply.Term: %d > rf.currentTerm %d\n", rf.me, reply.Term, rf.currentTerm)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	if rf.role != Leader {
		isLeader = false
		return index, term, false
	}

	DPrintf("%d is Leader\n", rf.me)
	index = len(rf.log) // index starts from 1
	term = rf.currentTerm

	logEntry := LogEntry{
		Command: command,
		Term:    term,
		Index:   index,
	}

	// append new log entry
	DPrintf("Leader: %d 添加日志: %v\n", rf.me, command)
	rf.log = append(rf.log, logEntry)

	// start a agreement process
	go rf.sendAppendEntries()

	return index, term, isLeader
}
// todo: If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N, and log[N].term == currentTerm,
// set commitIndex = N

// only called by leader
func (rf *Raft) sendAppendEntries() {
	var successNum int64 = 1 // since Leader itself has keep the entry into local log.
	index := 0
	rf.mu.Lock()
	for i := 0; i < len(rf.peers); i++ {
		// skip self
		if i == rf.me {
			continue
		} else {
			DPrintf("=======Leader %d %d %d\n",rf.me, rf.log[len(rf.log)-1].Index, rf.nextIndex[i])
			message := "Leader " + strconv.Itoa(rf.me) + " send to Follower " + strconv.Itoa(i)
			if rf.log[len(rf.log)-1].Index >= rf.nextIndex[i] {
				//DPrintf("Leader %d nextIndex[%d]下标 %d\n", rf.me, i, rf.nextIndex[i])
				//DPrintf("Leader %d len(rf.log)的长度啊: %d\n", rf.me, len(rf.log))
				// rf.nextIndex[i]错了,有时候变成0了，真是匪夷所思
				DPrintf("Leader %d send to Follower %d nextIndex %d\n", rf.me, i, rf.nextIndex[i])
				PrintLogEntries(message + " (482) Leader rf.log ", rf.log)
				entries := rf.log[rf.nextIndex[i]:]
				PrintLogEntries(message + " (484) entries", entries)
				DPrintf("Leader %d 发送日志消息给节点: %d, 日志下标%d-%d\n", rf.me, i, entries[0].Index, entries[len(entries)-1].Index)
				index = entries[len(entries)-1].Index
				go rf.sendAppendEntry(i, entries, &successNum)
			} else {
				DPrintf("Leader %d 没有日志需要发送给Follower: %d\n", rf.me, i)
			}
		}
	}

	rf.mu.Unlock()
	// wait for majority of the follower reply and then commit this log
	for {
		//DPrintf("%d got %d replies\n", rf.me, atomic.LoadInt64(&successNum))
		//DPrintf("len(rf.peers):%d\n", len(rf.peers))
		DPrintf("Leader %d 等待日志%d 的回复 successNum is %d\n", rf.me, index, int(atomic.LoadInt64(&successNum)))
		if int(atomic.LoadInt64(&successNum)) > len(rf.peers) / 2 {
			DPrintf("Leader: %d 收到日志index=%d超过半数的投票\n", rf.me, index)
			break
		}

		time.Sleep(time.Duration(100) * time.Millisecond)
	}

	rf.mu.Lock()
	DPrintf("Leader %d 更新rf.commitIndex=%d entry.Index=%d\n", rf.me, rf.commitIndex, rf.log[len(rf.log)-1].Index)
	rf.commitIndex = rf.log[len(rf.log)-1].Index // index of log to be committed
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntry(i int, entries []LogEntry, successNum *int64) {
	rf.mu.Lock()
	var prevLogIndex int
	var prevLogTerm int
	if len(rf.log) == 1 {
		prevLogIndex = 0
		prevLogTerm = 1 // is this right? term start from 1.
	} else {
		DPrintf("Leader %d send to Follower %d %d %d \n", rf.me, i, len(rf.log), len(entries))
		prevLogIndex = rf.log[rf.nextIndex[i]-1].Index
		//prevLogIndex = rf.log[len(rf.log) - len(entries) - 1].Index
		prevLogTerm = rf.log[prevLogIndex].Term
		DPrintf("Leader %d send to Follower %d, prevLogIndex=%d, prevLogTerm=%d\n", rf.me, i, prevLogIndex, prevLogTerm)

		//prevLogIndex = rf.log[rf.nextIndex[i]-1].Index
		//prevLogTerm = rf.log[rf.nextIndex[i]-1].Term
	}

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex, // the log index before this new log entry
		PrevLogTerm:  prevLogTerm, // the term of the log entry just before this new log entry
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply {}
	rf.mu.Unlock()

	// todo: 如果AppendEntries失败，应该把对应的nextIndex-1，直到AppendEntries成功
	DPrintf("Leader %d 调用Raft.AppendEntries to Follower %d\n", rf.me, i)
	ok := rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
	rf.mu.Lock()
	if ok {
		rf.lastRenewTime = time.Now()
		DPrintf("(530) %d send AppendEntry reply to Leader %d\n", i, rf.me)

		if reply.Success {
			atomic.AddInt64(successNum, 1)
			// todo: 这里nextIndex的更新正确吗？
			DPrintf("Leader %d nextIndex[%d] 是 %d\n", rf.me, i, entries[len(entries) - 1].Index + 1)
			rf.nextIndex[i] = entries[len(entries) - 1].Index + 1
			rf.matchIndex[i] = entries[len(entries) - 1].Index // todo: 这里matchIndex的更新要注意是否正确
		} else {
			DPrintf("(561) Leader %d from Follower %d rf.nextIndex[%d]-1 = %d\n", rf.me, i, i, rf.nextIndex[i]-1)
			rf.nextIndex[i]-- // todo: 什么时候重试呢？ 心跳的时候就能重试了
		}
	}
	rf.mu.Unlock()
}

// find the largest commitIndex to check
//func findMaxGreaterThanHalf(arr []int) int {
//	sort.Ints(arr)
//
//	halfCount := len(arr) / 2
//
//	for i := halfCount; i < len(arr); i++ {
//		// what if every element is the same?
//		if arr[i] >= arr[halfCount-1] {
//			return arr[i]
//		}
//	}
//
//	return -1
//}
//
//// only for Leader to commit log
//func (rf *Raft) commitLog() {
//	for {
//		rf.mu.Lock()
//		if rf.role != Leader {
//			rf.mu.Unlock()
//			break
//		} else {
//			// the match index we're going to check, we can optimize it later
//			// 需要找到最大的那个超过半数的index
//			// Commit this log entry
//			rf.commitIndex = findMaxGreaterThanHalf(rf.matchIndex)
//
//			rf.mu.Unlock()
//			// sleep
//			time.Sleep(time.Duration(10) * time.Millisecond)
//		}
//	}
//}

func (rf *Raft) applyLog() {
	for {
		// nothing to apply to state machine, just wait for 100 millisecond
		rf.mu.Lock()
		if rf.commitIndex == rf.lastApplied {
			//DPrintf("applyLog %d sleep 100 millisecond\n", rf.me)
			rf.mu.Unlock()
			time.Sleep(time.Duration(100) * time.Millisecond)
		} else if rf.commitIndex > rf.lastApplied {
			DPrintf("applyLog rf.me: %d rf.commitIndex: %d rf.lastApplied=%d len(rf.log)=%d\n", rf.me, rf.commitIndex, rf.lastApplied, len(rf.log))

			msg := &ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied + 1].Command,
				CommandIndex: rf.lastApplied + 1,
			}

			rf.applyCh <- *msg
			rf.lastApplied++
			rf.mu.Unlock()
		} else {
			// ?
			DPrintf("applyLog %d ??????????\n", rf.me)
			rf.mu.Unlock()
		}
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm += 1
	rf.role = Candidate
	rf.votedFor = rf.me // vote for self
	rf.votedCount = 1
	rf.lastRenewTime = time.Now() // reset election timer

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me, // please vote for me
		LastLogIndex: rf.log[len(rf.log)-1].Index,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	// send RequestVote RPCs to all other servers
	for i := 0; i < len(rf.peers); i++ {
		// skip self
		if i == rf.me {
			continue
		}
		// collect votes
		//DPrintf("%d collect votes from %d\n", rf.me, i)
		go rf.collectVotes(i, args)
	}
}

func (rf *Raft) collectVotes(index int, args *RequestVoteArgs) {
	voteGranted := rf.getVoteResult(index, args)

	if !voteGranted {
		DPrintf("%d from %d voteGranted failed\n", rf.me, index)
		return
	}
	DPrintf("%d from %d voteGranted successed\n", rf.me, index)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.votedCount > len(rf.peers)/2 {
		//DPrintf("%d is already leader\n", rf.me)
		return
	}

	rf.votedCount += 1
	if rf.votedCount > len(rf.peers)/2 {
		rf.role = Leader
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1 // the index of last logEntry + 1
			rf.matchIndex[i] = 0 // initialized to 0
		}

		DPrintf("%d is leader now\n", rf.me)
		go rf.sendHeartbeats()
		//go rf.commitLog()
	}
}

func (rf *Raft) getVoteResult(index int, args *RequestVoteArgs) bool {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(index, args, &reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		//DPrintf("%d 376: reply.Term: %d > rf.currentTerm %d\n", rf.me, reply.Term, rf.currentTerm)
		rf.role = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
	}

	return reply.VoteGranted
}

// send heartbeat as a leader, it should be in a loop
func (rf *Raft) sendHeartbeats() {
	//DPrintf("%d start to send heartbeat\n", rf.me)
	for !rf.killed() {
		for i := 0; i < len(rf.peers); i++ {
			rf.mu.Lock()
			// skip self
			if i == rf.me {
				rf.mu.Unlock()
				continue
			}

			// fixme: move before 'skip self'
			if rf.role != Leader {
				DPrintf("%d not a leader anymore\n", rf.me)
				rf.mu.Unlock()
				return
			}

			go rf.sendHeartbeat(i)
			rf.mu.Unlock()
		}

		// sleep for a while
		time.Sleep(time.Duration(HeartbeatTimeout) * time.Millisecond)
	}
	//DPrintf("%d is killed\n", rf.me)
}

func (rf *Raft) sendHeartbeat(i int) {
	// empty args as heartbeat
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: rf.log[rf.nextIndex[i]-1].Index,
		PrevLogTerm: rf.log[rf.log[rf.nextIndex[i]-1].Index].Term,
	}

	// todo: 如果发现日志一致性检查失败，那么可以在心跳包里添加日志信息，加快恢复速度，否则只有等下次发送日志的时候才会把完整日志发送过去
	if rf.nextIndex[i] < len(rf.log) - 1 {
		args.Entries = rf.log[rf.nextIndex[i]:]
	}

	reply := AppendEntriesReply{}
	DPrintf("(713) Leader %d 发送心跳 to %d, commitIndex=%d\n", rf.me, i, rf.commitIndex)
	rf.mu.Unlock()
	ok := rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
	//DPrintf("%d send Raft.AppendEntries to %d\n", rf.me, i)
	rf.mu.Lock()
	if !ok {
		DPrintf("%d failed to call Raft.AppendEntries to %d\n", rf.me, i)
	} else {
		rf.lastRenewTime = time.Now()
		if !reply.Success {
			DPrintf("(740) Leader %d from Follower %d rf.nextIndex[%d]-1 = %d\n", rf.me, i, i, rf.nextIndex[i]-1)
			rf.nextIndex[i]--
			if rf.nextIndex[i] != 0 {
				go rf.sendHeartbeat(i)
			}
		}
		DPrintf("(720) %d send AppendEntry reply to Leader\n", i)
		//DPrintf("%d success to call Raft.AppendEntries to %d\n", rf.me, i)
	}

	if reply.Term > rf.currentTerm {
		//DPrintf("%d 406: %d > %d\n", rf.me, reply.Term, rf.currentTerm)
		rf.currentTerm = reply.Term
		rf.role = Follower
		rf.votedFor = -1
	}
	rf.mu.Unlock()
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		// election timeouts(should be bigger than 150 to 300 slightly)
		randElectionTimeout := int(rf.rd.Float64()*150) + 400
		//DPrintf("%d randElectionTimeout: %d\n", rf.me, randElectionTimeout)
		rf.mu.Lock()
		if time.Since(rf.lastRenewTime) > time.Duration(randElectionTimeout)*time.Millisecond {
			if rf.role != Leader {
				// a leader election should be started
				DPrintf("%d ready to electLeader\n" , rf.me)
				go rf.electLeader()
			} else { // 当前Leader（自己）失联
				DPrintf("Leader %d 自己失联\n", rf.me)
				rf.role = Follower
				rf.votedFor = -1
			}
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		//DPrintf("%d sleep for %d millisecond to check if a leader election should be started", rf.me, ms)
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
	rf.applyCh = applyCh

	logEntry := LogEntry{
		Command: nil,
		Term:    0,
		Index:   0,
	}

	// an empty log entry in rf.log[0], make index and real rf.log consistent
	rf.log = append(rf.log, logEntry)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([] int, len(peers))

	// check and apply log
	go rf.applyLog()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
