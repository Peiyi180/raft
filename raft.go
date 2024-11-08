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
	"fmt"
	"math"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

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

// entry
type Entry struct {
	Term int
	Cmd  interface{}
}

// role enum
const (
	Follower = iota
	Candidate
	Leader
)

const TickInterval int = 100
const HeartBeatTimeOut int = 50

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Figure 2 State
	// persistent state on all servers
	currentTerm int
	votedFor    int
	log         []Entry

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// others
	timestamp time.Time
	role      int

	muVote    sync.Mutex
	voteCount int

	// volatile state on all servers
	commitIndex int
	lastApplied int

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead) // is this necessary?
	return z == 1
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return -1, -1, false
	}
	newEntry := &Entry{Term: rf.currentTerm, Cmd: command}
	rf.log = append(rf.log, *newEntry)
	// Your code here (2B).

	return len(rf.log) - 1, rf.currentTerm, true
}

func (rf *Raft) CommitChecker() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied += 1
			msg := &ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Cmd,
				CommandIndex: rf.lastApplied,
			}
			rf.applyCh <- *msg
			DPrintf("server %v 准备将命令 %v(索引为 %v ) 应用到状态机", rf.me, msg.Command, msg.CommandIndex)
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(TickInterval) * time.Millisecond)
	}
}

// append entries rpc
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntries(serverTo int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[serverTo].Call("Raft.AppendEntries", args, reply)

	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		reply.Success = false
		return
	}

	rf.timestamp = time.Now()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
	}

	if len(args.Entries) == 0 {
		rf.PPrint(fmt.Sprintf("server %v 接收到 leader &%v 的心跳 - args %+v", rf.me, args.LeaderId, args))
	} else {
		rf.PPrint("server %v 收到 leader %v 的的AppendEntries: %+v ", rf.me, args.LeaderId, args)
	}

	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		reply.Success = false
		rf.PPrint("server %v 检查到心跳中参数不合法:\t args.PrevLogIndex=%v, args.PrevLogTerm=%v, \tlen(self.log)=%v, self最后一个位置term为:%v", rf.me, args.PrevLogIndex, args.PrevLogTerm, len(rf.log), rf.log[len(rf.log)-1].Term)
		return
	}

	if len(args.Entries) != 0 && len(rf.log) > args.PrevLogIndex+1 && rf.log[args.PrevLogIndex+1].Term != args.Entries[0].Term {
		rf.PPrint("server %v 的log与args发生冲突, 进行移除", rf.me)
		rf.log = rf.log[:args.PrevLogIndex+1]
	}

	rf.log = append(rf.log, args.Entries...)
	if len(args.Entries) != 0 {
		DPrintf("server %v 成功进行apeend", rf.me)
		rf.SPrint()
	}

	reply.Success = true
	reply.Term = rf.currentTerm
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
	}
	rf.mu.Unlock()

}

func (rf *Raft) handleAppendEntries(serverTo int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(serverTo, args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()

	if args.Term != rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if reply.Success {

		rf.matchIndex[serverTo] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[serverTo] = rf.matchIndex[serverTo] + 1

		N := len(rf.log) - 1
		for N > rf.commitIndex {
			count := 1
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= N && rf.log[N].Term == rf.currentTerm {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = N
				break
			}
			N -= 1
		}
		rf.mu.Unlock()
		return
	}

	if reply.Term > rf.currentTerm {
		rf.PPrint(fmt.Sprintf("server %v 旧的leader收到了心跳函数中更新的term: %v, 转化为Follower ", rf.me, reply.Term))
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.timestamp = time.Now()
		rf.mu.Unlock()
		return
	}

	if reply.Term == rf.currentTerm && rf.role == Leader {
		rf.nextIndex[serverTo]--
		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) SendHeartBeats() {
	rf.PPrint(fmt.Sprintf("server %v 开始发送心跳 ", rf.me))

	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			if rf.nextIndex[i] > len(rf.log) {
				rf.nextIndex[i] = len(rf.log)
			}

			rf.PPrint("CUURENT LOG: %v, next index - %v, i %v", rf.log, rf.nextIndex, i)
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
				LeaderCommit: rf.commitIndex,
			}
			rf.PPrint("len of log -1 %v", len(rf.log)-1)
			rf.PPrint("next index %v", rf.nextIndex[i])
			if len(rf.log)-1 >= rf.nextIndex[i] {
				args.Entries = rf.log[rf.nextIndex[i]:]
				DPrintf("leader %v 开始向 server %v 广播新的AppendEntries, args = %+v", rf.me, i, args)
			} else {
				args.Entries = make([]Entry, 0)
				DPrintf("leader %v 开始向 server %v 广播新的心跳, args = %+v  ", rf.me, i, args)
			}

			go rf.handleAppendEntries(i, args)

		}
		rf.mu.Unlock()

		time.Sleep(time.Duration(HeartBeatTimeOut) * time.Millisecond)
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.PPrint("receiving request for vote")
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		reply.VoteGranted = false
		rf.PPrint(fmt.Sprintf("server %v 拒绝向 server %v投票: 旧的term: %v,\targs= %+v", rf.me, args.CandidateId, args.Term, args))
		return
	}

	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.role = Follower
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1) {
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
			rf.role = Follower
			rf.timestamp = time.Now()

			rf.mu.Unlock()
			reply.VoteGranted = true
			rf.PPrint(fmt.Sprintf("server %v 同意向 server %v投票 \targs= %+v ", rf.me, args.CandidateId, args))
			return
		} else {
			rf.PPrint(fmt.Sprintf("server %v 拒绝向 server %v投票: 已投票 \targs= %+v ", rf.me, args.CandidateId, args))
		}
	}

	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	reply.VoteGranted = false

}

func GetRandomElectTimeOut(rd *rand.Rand) int {
	minT := 150
	maxT := 350
	return rd.Intn(maxT-minT+1) + minT
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rd := rand.New(rand.NewSource(int64(rf.me)))
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//rf.PPrint("ping")
		rdTimeOut := GetRandomElectTimeOut(rd)
		rf.mu.Lock()
		if rf.role != Leader && time.Since(rf.timestamp) > time.Duration(rdTimeOut)*time.Millisecond {
			rf.PPrint("start elect ")
			go rf.Elect()
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(TickInterval) * time.Millisecond)
	}
}

func (rf *Raft) PPrint(msg string, a ...interface{}) {
	if len(a) != 0 {
		msg = fmt.Sprintf(msg, a...)
	}
	DPrintf("%s - raft %v:{currentTerm=%v, role=%v, votedFor=%v, vote=%v} ", msg, rf.me, rf.currentTerm,
		rf.role, rf.votedFor, rf.voteCount)

}

func (rf *Raft) SPrint() {
	DPrintf("raft %v:{currentTerm=%v, role=%v, votedFor=%v, voteCount=%v, log=%v, nextIndex=%v,"+
		" matchIndex=%v, commitIndex=%v, lastApplied=%v} ", rf.me, rf.currentTerm,
		rf.role, rf.votedFor, rf.voteCount, rf.log, rf.nextIndex, rf.matchIndex, rf.commitIndex, rf.lastApplied)
}

func (rf *Raft) Elect() {
	rf.mu.Lock()

	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.timestamp = time.Now()

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.PPrint(fmt.Sprintf("asking for vote %d", i))
		go rf.collectVote(i, args)
	}
}

func (rf *Raft) collectVote(serverTo int, args *RequestVoteArgs) {
	rf.PPrint("collecting votes")
	voteAnswer := rf.GetVoteAnswer(serverTo, args)
	if !voteAnswer {
		return
	}
	rf.muVote.Lock()
	if rf.voteCount > len(rf.peers)/2 {
		rf.muVote.Unlock()
		return
	}

	rf.voteCount++
	if rf.voteCount > len(rf.peers)/2 {
		rf.mu.Lock()
		if rf.role == Follower {
			rf.mu.Unlock()
			rf.muVote.Unlock()
			return
		}

		rf.role = Leader
		rf.PPrint("becomes leader")
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 0
		}
		rf.mu.Unlock()
		go rf.SendHeartBeats()
	}

	rf.muVote.Unlock()
}

func (rf *Raft) GetVoteAnswer(serverTo int, args *RequestVoteArgs) bool {
	sendArgs := *args
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(serverTo, &sendArgs, &reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if sendArgs.Term != rf.currentTerm {
		return false
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.role = Follower
	}
	return reply.VoteGranted
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

	// Your initialization code here (2A, 2B, 2C).
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{Term: 0})
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.timestamp = time.Now()
	rf.role = Follower
	rf.applyCh = applyCh

	for i := 1; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.CommitChecker()
	rf.PPrint("Making raft ")
	return rf
}
