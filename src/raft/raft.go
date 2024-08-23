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

	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
	LEADER    = "leader"
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	apllyCh   chan ApplyMsg
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm     int
	votedFor        int
	state           string
	electionTimeout time.Duration
	lastHeartbeat   time.Time
	log             []LogEntry
	commitIndex     int
	lastApplied     int
	nextIndex       []int
	matchIndex      []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var isLeader bool // false by default
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == LEADER {
		isLeader = true
	}
	return rf.currentTerm, isLeader
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

func (rf *Raft) InitiateElection() {
	rf.convertToCandidate()
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	startingTerm := rf.currentTerm
	DPrintf("[%d] Starting election Term %d", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	voteCount := 1
	totalCount := 1
	cond := sync.NewCond(&rf.mu)
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			args := RequestVoteArgs{Term: startingTerm, CandidateId: rf.me}
			reply := RequestVoteReply{}
			rf.mu.Unlock()
			ok := rf.sendRequestVote(server, &args, &reply)
			rf.mu.Lock()
			if ok && reply.VoteGranted {
				voteCount++
			}
			totalCount++
			rf.mu.Unlock()
			cond.Broadcast()
		}(i)
	}
	rf.mu.Lock()
	votesNeeded := len(rf.peers)/2 + 1
	for voteCount < votesNeeded && totalCount < len(rf.peers) {
		cond.Wait()
		DPrintf("[%d] Counting votes %d ; Need %d ; TotalCounted %d", rf.me, voteCount, votesNeeded, totalCount)
	}
	DPrintf("[%d] Checking for promotion Counted votes %d ; Need %d ; TotalCounted %d ; state %s ; startingTerm %d ; currentTerm %d", rf.me, voteCount, votesNeeded, totalCount, rf.state, startingTerm, rf.currentTerm)
	if voteCount >= votesNeeded && rf.currentTerm == startingTerm && rf.state != FOLLOWER {
		rf.mu.Unlock()
		rf.convertToLeader()
		rf.mu.Lock()
	} else if rf.state != FOLLOWER {
		rf.mu.Unlock()
		rf.convertToCandidate()
		rf.mu.Lock()
	} else {
		DPrintf("[%d] Not promoted :D", rf.me)
	}
	rf.mu.Unlock()
}

func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	DPrintf("[%d] Promoting to leader", rf.me)
	rf.state = LEADER
	rf.nextIndex = make([]int, len(rf.peers))
	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log) + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.mu.Unlock()
	rf.sendHeartbeats()
}

func (rf *Raft) convertToFollower() {
	rf.mu.Lock()
	DPrintf("[%d] Converting to follower", rf.me)
	rf.state = FOLLOWER
	rf.lastHeartbeat = time.Now()
	rf.votedFor = -1
	rf.setElectionTimeout()
	rf.mu.Unlock()
}

func (rf *Raft) convertToCandidate() {
	rf.mu.Lock()
	DPrintf("[%d] Converting to candidate", rf.me)
	rf.state = CANDIDATE
	rf.lastHeartbeat = time.Now()
	rf.votedFor = -1
	rf.setElectionTimeout()
	rf.mu.Unlock()
}

func (rf *Raft) setElectionTimeout() { // MUST call WITH LOCK
	ms := 600 + (rand.Int63() % 1000)
	rf.electionTimeout = time.Duration(ms) * time.Millisecond
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// voted for == null tada galima jei ne null tada negalima
	rf.mu.Lock()
	DPrintf("[%d] received RequestVote from Server %d Term %d", rf.me, args.CandidateId, args.Term)
	reply.Term = rf.currentTerm
	// if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId)) { // if new term OR if same term AND have not voted
	if args.Term > rf.currentTerm && rf.isRequestLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.VoteGranted = true
		rf.lastHeartbeat = time.Now()
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
	}
	rf.currentTerm = Max(rf.currentTerm, args.Term)
	DPrintf("[%d] RequestVote from Server %d Term %d WAS GRANTED? %v", rf.me, args.CandidateId, args.Term, reply.VoteGranted)
	rf.mu.Unlock()
}

func (rf *Raft) isRequestLogUpToDate(lastLogTerm, lastLogIndex int) bool {
	if len(rf.log) == 0 {
		return true
	}
	return rf.log[len(rf.log)-1].Term < lastLogTerm || (rf.log[len(rf.log)-1].Term < lastLogTerm && len(rf.log) <= lastLogIndex)
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

type AppendEntrieArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

func NewAppendEntrieArgs(term, leaderId, prevLogIndex, prevLogTerm, leaderCommit int, entries []LogEntry) *AppendEntrieArgs {
	return &AppendEntrieArgs{Term: term,
		LeaderId: leaderId, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: entries, LeaderCommit: leaderCommit}
}

type AppendEntrieReply struct {
	Term    int
	XTerm   int
	XIndex  int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntrieArgs, reply *AppendEntrieReply) {
	rf.mu.Lock()
	DPrintf("[%d] AppendEntries received from Server %d Term %d PrevLogIndex %d PrevLogTerm %d Entries %v LeaderCommit %d", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)
	if args.Term >= rf.currentTerm {
		// reply.Success = true
		rf.lastHeartbeat = time.Now()
		rf.setElectionTimeout()
		if rf.state != FOLLOWER {
			rf.mu.Unlock()
			rf.convertToFollower()
			rf.mu.Lock()
		}
		if len(rf.log) == 0 {
			DPrintf("[%d] APPEND ENTRIES SUCCESS, log was empty", rf.me)
			reply.Success = true
			rf.log = append(rf.log, args.Entries...)
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
			}
		} else if len(rf.log) > args.PrevLogIndex {
			DPrintf("[%d] APPEND ENTRIES SUCCESS, len(log) %d args.PrevLogIndex %d logOfsPrevLogIndex %v args.PrevLogTerm %d", rf.me, len(rf.log), args.PrevLogIndex, rf.log[args.PrevLogIndex], args.PrevLogIndex)
			reply.Success = true
			if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
				rf.log = rf.log[:args.PrevLogIndex]
			}
			rf.log = append(rf.log, args.Entries...)
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
			}
		} else {
			DPrintf("[%d] APPEND ENTRIES FAILED, len(log) %d args.PrevLogIndex %v args.PrevLogTerm %d", rf.me, len(rf.log), args.PrevLogIndex, args.PrevLogIndex)
			reply.Success = false
			if len(rf.log) > 0 {
				reply.XTerm = rf.log[len(rf.log)-1].Term
				i := len(rf.log) - 1
				for i >= 1 {
					if rf.log[i-1].Term != reply.XTerm {
						break
					}
					i--
				}
				reply.XIndex = i
			} else {
				reply.XTerm = -1
				reply.XIndex = -1
			}
		}
	} else {
		DPrintf("[%d] AppendEntries DENIED BECAUSE OF TERM from Server %d Term %d", rf.me, args.LeaderId, args.Term)
	}
	rf.currentTerm = int(math.Max(float64(rf.currentTerm), float64(args.Term)))
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

func (rf *Raft) ApplyEntries() {
	for !rf.killed() {
		rf.mu.Lock()
		DPrintf("[%d] Checking entries, commitIndex %d lastApplied %d", rf.me, rf.commitIndex, rf.lastApplied)
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			DPrintf("[%d] Applying entry number %d", rf.me, rf.lastApplied)
			rf.apllyCh <- ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied-1].Command, CommandIndex: rf.lastApplied}
		}
		rf.mu.Unlock()
		time.Sleep(1 * time.Second)
	}
}

// func (rf *Raft) FindEntry

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
	isLeader := false
	index := rf.commitIndex + 1
	term := rf.currentTerm
	if rf.state != LEADER {
		rf.mu.Unlock()
		return index, term, isLeader
	}
	DPrintf("[%d] Received command %v Sending it out", rf.me, command)
	entry := LogEntry{Command: command, Term: rf.currentTerm}
	rf.log = append(rf.log, entry)
	okCount := 1
	totalCount := 1
	countNeeded := CountNeeded(len(rf.peers))
	cond := sync.NewCond(&rf.mu)
	rf.mu.Unlock()
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			prevLogTerm := rf.currentTerm
			if len(rf.log) > 1 {
				prevLogTerm = rf.log[len(rf.log)-1-1].Term
			}
			args := NewAppendEntrieArgs(rf.currentTerm, rf.me, len(rf.log)-1-1, prevLogTerm, rf.commitIndex, []LogEntry{entry})
			reply := &AppendEntrieReply{}
			DPrintf("[%d] Sending to server %d args %v", rf.me, server, *args)
			rf.mu.Unlock()
			ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
			for !ok && !reply.Success {
				rf.mu.Lock()
				if reply.XIndex == -1 && reply.XTerm == -1 {
					args.PrevLogIndex = 0
					args.Entries = rf.log
				} else {
					args.PrevLogIndex = rf.matchXEntry(reply.XIndex, reply.XTerm)
					args.Entries = rf.log[args.PrevLogIndex+1:]
				}
				rf.mu.Unlock()
				DPrintf("[%d] RETRYING Sending to server %d args %v", rf.me, server, *args)
				ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
				time.Sleep(100 * time.Millisecond)
			}
			rf.mu.Lock()
			if reply.Success {
				okCount++
				rf.nextIndex[server]++
				rf.matchIndex[server]++
			} else {
				DPrintf("[%d] Gave up on trying to send RaftAppendEntires to server %d for no reason", rf.me, server)
			}
			totalCount++
			rf.mu.Unlock()
			cond.Broadcast()
		}(i)
	}
	rf.mu.Lock()
	for okCount < countNeeded && totalCount < len(rf.peers) {
		cond.Wait()
		DPrintf("[%d] CurrentCount %d ; Need %d ; TotalCounted %d", rf.me, okCount, countNeeded, totalCount)
	}
	if okCount >= countNeeded {
		rf.commitIndex++
		DPrintf("[%d] Got enough votes sending command, commitIndex: %d", rf.me, rf.commitIndex)
		// rf.apllyCh <- ApplyMsg{CommandValid: true, Command: command, CommandIndex: rf.commitIndex}
		// DPrintf("[%d] Sent: %d", rf.me, rf.commitIndex)
	} else {
		DPrintf("[%d] Did not Get enough votes sending command", rf.me)
	}
	isLeader = true
	rf.mu.Unlock()
	return index, term, isLeader
}

func (rf *Raft) matchXEntry(index, term int) int {
	var curIndex int
	for i := len(rf.log) - 1; i >= 0 && rf.log[i].Term > term; i-- {
		curIndex = i
		if rf.log[i].Term == term {
			break
		}
	}
	termToFindStartOf := rf.log[curIndex].Term
	for curIndex >= 1 && rf.log[curIndex-1].Term == termToFindStartOf {
		curIndex--
	}
	return curIndex
}

type LogEntry struct {
	Command interface{}
	Term    int
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
	DPrintf("[%d] Killing this server", rf.me)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Check if a leader election should be started.
		rf.mu.Lock()
		DPrintf("[%d] Tick", rf.me)
		if rf.state != LEADER && time.Now().Sub(rf.lastHeartbeat) > rf.electionTimeout {
			rf.mu.Unlock()
			rf.InitiateElection()
		} else if rf.state == LEADER {
			rf.mu.Unlock()
			rf.sendHeartbeats()
		} else {
			rf.mu.Unlock()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	startingTerm := rf.currentTerm
	id := rf.me
	DPrintf("[%d] Sending heartbeats Term %d", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	for i, _ := range rf.peers {
		// DPrintf("[%d] AppendEntries to %d", rf.me, i)
		if i == rf.me {
			// DPrintf("[%d] CANCEL AppendEntries to same %d", rf.me, i)
			continue
		}
		go func(server int) {
			term := 0
			if len(rf.log) > 0 {
				term = rf.log[len(rf.log)-1].Term
			}
			args := NewAppendEntrieArgs(rf.currentTerm, id, len(rf.log)-1, term, rf.commitIndex, []LogEntry{})
			reply := &AppendEntrieReply{}
			ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
			for !ok {
				ok = rf.peers[server].Call("Raft.AppendEntries", args, &reply)
			}
			if !reply.Success && reply.Term > startingTerm {
				rf.mu.Lock()
				rf.currentTerm = reply.Term
				rf.mu.Unlock()
				rf.convertToFollower()
			}
		}(i)
		i++
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
	rf.votedFor = -1
	rf.setElectionTimeout()
	rf.convertToFollower()
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.ApplyEntries()
	return rf
}
