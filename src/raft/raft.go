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
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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
	Follower = iota
	Candidate
	Leader
)

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

	applyCh chan ApplyMsg
	count   map[int]int

	// Persistent state on all servers
	currentTerm   int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor      int        // candidateId that received vote in current term (or -1 if none)
	log           []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	supportNumber int        // voted by how many servers

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders: (Reinitialized after election)
	nextIndex    []int       // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex   []int       // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	commitNumber map[int]int // for each log, how many servers have accepted it

	// identity
	identity int // follower, candidate, or leader
	//hasLeader bool // true for already having a leader, false for no leader

	// time
	heartbeatTime int64 // time for sending heartbeat, only useful for leader
	electionTime  int64 // time for receiving heartbeat

}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	//var term int
	//var isleader bool
	// Your code here (2A).
	//return term, isleader
	return rf.currentTerm, rf.identity == Leader
}

// Get a random time for heartbeat time-out
// try 1000 - 1200 ms
func (rf *Raft) GetElectionTime() int64 {
	return time.Now().UnixMilli() + rand.Int63()%200 + 1000
}

func (rf *Raft) GetHeartbeatSendingTime() int64 {
	return time.Now().UnixMilli() + 150
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// args for AppendEntries RPC
// field names must start with capital letters!
type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

func (rf *Raft) Lock() {
	rf.mu.Lock()
}

func (rf *Raft) Unlock() {
	rf.mu.Unlock()
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.Lock()
	defer rf.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		fmt.Printf("Candidate %v has not get voted by server %v due to old term\n", args.CandidateId, rf.me)
		//rf.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.identity = Follower
		rf.currentTerm, rf.votedFor = args.Term, -1
	}
	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	// TODO: has not checked whether the candidate's log is up-to-date
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (rf.isCandidateUpToDate(args)) {
		rf.votedFor = args.CandidateId
		//fmt.Printf("Candidate %v has got voted by server %v\n", args.CandidateId, rf.me)
		reply.VoteGranted = true
	}
	//rf.Unlock()
}

// TODO: change the logic to check whether the candidate is up-to-date
func (rf *Raft) isCandidateUpToDate(args *RequestVoteArgs) bool {
	return true
}

// To append new entried to followers or send heartbeats
// TODO: add log entries function
// TODO: commit a log that at least a half server received this log
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Lock()
	defer rf.Unlock()
	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		fmt.Printf("Append logs failed due to outdated term\n")
		return
	}

	// change back to follower identity if the server is candidate
	if rf.identity == Candidate {
		rf.identity = Follower
	}
	//rf.heartbeatTime = time.Now().UnixMilli()
	rf.electionTime = rf.GetElectionTime()

	// heartbeat will return here
	if args.Entries == nil {
		return
	}

	// if PrevLogIndex is out of the follower's array, return false
	if args.PrevLogIndex+1 > len(rf.log) {
		return
	}
	// check if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		fmt.Printf("The server %v doesn't contain last log\n", rf.me)
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	rf.log = rf.log[:args.PrevLogIndex+1]
	// Append any new entries not already in the log
	rf.log = append(rf.log, args.Entries...)

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		lastNewEntryId := args.Entries[len(args.Entries)-1].Index
		if args.LeaderCommit < lastNewEntryId {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewEntryId
		}
	}

	reply.Success = true

	//fmt.Printf("AppendEntries succeed!\n")
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
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) getLastLog() LogEntry {
	l := len(rf.log)
	return rf.log[l-1]
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
	rf.Lock()
	defer rf.Unlock()
	//fmt.Printf("Start to start!!!\n")

	//index := -1
	//term := -1
	//isLeader := true

	// Your code here (2B).
	if rf.identity != Leader {
		//fmt.Printf("This server is not the leader!\n")
		return -1, -1, false
	}

	//fmt.Printf("This server is the leader!\n")
	// apply log to leader itself
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Index: rf.nextIndex[rf.me], Command: command})
	rf.nextIndex[rf.me] += 1
	rf.lastApplied += 1
	rf.count[rf.lastApplied] = 1
	// replicate logs to followers
	for i := range rf.peers {
		if i != rf.me {
			go rf.replicateLog(i, command)
		}
	}

	//fmt.Printf("Finish Start!!!\n")

	fmt.Printf("the index that the command will appear at if it's ever committed is %v\n", rf.lastApplied+1)

	go rf.checkCommitLog()

	return rf.lastApplied + 1, rf.currentTerm, true
	//return rf.lastApplied, rf.currentTerm, true
}

// send new log to servers and force servers to have the same log with leader
func (rf *Raft) replicateLog(server int, command interface{}) {
	rf.Lock()
	defer rf.Unlock()
	nextIndex := rf.nextIndex[server]
	lastLog := rf.log[nextIndex-1]
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: lastLog.Index,
		PrevLogTerm:  lastLog.Term,
		Entries: []LogEntry{{
			Term:    rf.currentTerm,
			Index:   nextIndex,
			Command: command,
		}},
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{
		Term:    rf.currentTerm,
		Success: false,
	}
	rf.sendAppendEntries(server, &args, &reply)

	//fmt.Printf("reply is %s \n", reply)
	// if log conflicts or lacks
	for !reply.Success {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.identity = Follower
			return
		}
		rf.nextIndex[server] -= 1
		nextIndex = rf.nextIndex[server]
		//fmt.Printf("nextIndex for server %v is %v\n", server, nextIndex)
		if nextIndex != 0 {
			lastLog = rf.log[nextIndex-1]
		} else {
			lastLog = LogEntry{Term: 0, Index: 0, Command: nil}
		}

		args = AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: lastLog.Index,
			PrevLogTerm:  lastLog.Term,
			Entries:      rf.log[nextIndex:],
			LeaderCommit: rf.commitIndex,
		}
		rf.sendAppendEntries(server, &args, &reply)
	}

	// for all successful log, count their accepted number and send commit
	go rf.commitLog(rf.log[rf.nextIndex[server]:])

	rf.matchIndex[server] = rf.lastApplied
	rf.nextIndex[server] = rf.lastApplied + 1
}

// check all newly applied logs and commit those count majority with current term
func (rf *Raft) commitLog(log []LogEntry) {
	rf.Lock()
	defer rf.Unlock()
	for i := range log {
		if log[i].Term == rf.currentTerm {
			rf.commitNumber[log[i].Index] += 1
		}
	}
}

// periodically check if a log can be committed by checking if the accept number is over a half
func (rf *Raft) checkCommitLog() {
	for {
		rf.Lock()
		index := rf.commitIndex + 1
		for index <= rf.lastApplied && rf.commitNumber[index] > len(rf.peers)/2 {
			rf.commitIndex = index
			fmt.Printf("Commit log has command %v\n", rf.log[index].Command)
			rf.applyCh <- ApplyMsg{Command: rf.log[index].Command, CommandValid: true, CommandIndex: index} // send committed logs to channel

			index += 1
		}

		// send a request to notice all followers to commit logs
		for i := 0; i < len(rf.peers); i += 1 {
			if i != rf.me {
				go rf.replicateLog(i, rf.log[rf.commitIndex].Command)
			}
		}

		rf.Unlock()
		// TODO: whether to send the commit log to all followers
		time.Sleep(time.Duration(500) * time.Millisecond) // sleep for 500 ms
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

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		//fmt.Printf("Server %v start ticker\n", rf.me)

		if rf.identity != Leader && rf.electionTime < time.Now().UnixMilli() {
			//fmt.Printf("Server %v starts election at term %v!\n", rf.me, rf.currentTerm+1)
			rf.startElection()
		}
		// for a leader, periodically sending heartbeat to followers
		if rf.identity == Leader && rf.heartbeatTime < time.Now().UnixMilli() {
			//fmt.Printf("Leader %v sends heartbeats\n", rf.me)
			for i, _ := range rf.peers {
				if i != rf.me {
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.log[rf.nextIndex[i]-1].Index,
						PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
						Entries:      nil,
						LeaderCommit: rf.commitIndex,
					}
					reply := AppendEntriesReply{
						Term:    rf.currentTerm,
						Success: false,
					}
					rf.sendAppendEntries(i, &args, &reply)
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.identity = Follower
						rf.heartbeatTime = rf.GetHeartbeatSendingTime()
						rf.electionTime = rf.GetElectionTime()
						break
					}
				}
			}
			rf.heartbeatTime = rf.GetHeartbeatSendingTime()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.currentTerm += 1
	rf.identity = Candidate
	rf.votedFor = rf.me
	rf.heartbeatTime = rf.GetHeartbeatSendingTime()
	rf.electionTime = rf.GetElectionTime()

	rf.supportNumber = 1 // initiate by itself
	for i, _ := range rf.peers {
		// as soon as the server get elected or change back to follower due to old term
		// it stop the election process
		if rf.identity != Candidate {
			break
		}
		if i != rf.me {
			// asynchronous start election
			go func(i int) {
				//rf.Lock()
				//defer rf.Unlock()
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: 0, // TODO
					LastLogTerm:  0, // TODO
				}
				reply := RequestVoteReply{
					Term:        rf.currentTerm,
					VoteGranted: false,
				}
				rf.sendRequestVote(i, &args, &reply)
				if reply.VoteGranted == true {
					rf.supportNumber += 1
				}
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.identity = Follower
					rf.supportNumber = 0
					rf.votedFor = -1
					rf.heartbeatTime = rf.GetHeartbeatSendingTime()
					rf.electionTime = rf.GetElectionTime()
				}

				//fmt.Printf("Server %v got %v votes\n", rf.me, rf.supportNumber)
				if rf.supportNumber > len(rf.peers)/2 {
					//fmt.Printf("server %v has been selected to be the new leader at term %v\n", rf.me, rf.currentTerm)
					rf.identity = Leader
					rf.supportNumber = 0
					rf.heartbeatTime = rf.GetHeartbeatSendingTime()
					rf.electionTime = rf.GetElectionTime()
				}
			}(i)

		}
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

	fmt.Printf("Server %v initiated\n", rf.me)

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.supportNumber = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	// put a dummy log here with index 0
	rf.log = append(rf.log, LogEntry{
		Term: 0, Index: 0, Command: nil,
	})
	rf.applyCh = applyCh

	rf.heartbeatTime = rf.GetHeartbeatSendingTime()
	rf.electionTime = rf.GetElectionTime()

	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.lastApplied + 1
	}
	rf.matchIndex = make([]int, len(peers))
	rf.count = make(map[int]int)
	rf.commitNumber = make(map[int]int)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
