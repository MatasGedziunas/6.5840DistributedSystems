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

	"log"
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
	apllyCh   *chan ApplyMsg

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
	lastRequest     map[int]int
	nextOperationId int
}

func (rf *Raft) getLogTerm(index int) int {
	return rf.log[index].Term
}

func (rf *Raft) getNextOperationId() int { // must be called with lock
	rf.nextOperationId++
	return rf.nextOperationId
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
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

func NewRequestVoteArgs(term, candidateId, lastLogIndex, lastLogTerm int) *RequestVoteArgs {
	return &RequestVoteArgs{Term: term, CandidateId: candidateId, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
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
	startingTerm := rf.currentTerm
	DPrintf("[%d] Starting election Term %d", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	voteCount := 1
	totalCount := 1
	cond := sync.NewCond(&rf.mu)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			args := NewRequestVoteArgs(rf.currentTerm, rf.me, len(rf.log)-1, rf.log[len(rf.log)-1].Term)
			reply := &RequestVoteReply{}
			rf.mu.Unlock()
			ok := rf.sendRequestVote(server, args, reply)
			rf.mu.Lock()
			if ok && reply.VoteGranted {
				voteCount++
			}
			totalCount++
			rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.convertToFollower()
				rf.persist()
				return
			}
			cond.Broadcast()
		}(i)
	}
	rf.mu.Lock()
	votesNeeded := len(rf.peers)/2 + 1
	for voteCount < votesNeeded && totalCount < len(rf.peers) && time.Since(rf.lastHeartbeat) < rf.electionTimeout {
		cond.Wait()
		DPrintf("[%d] Counting votes %d ; Need %d ; TotalCounted %d", rf.me, voteCount, votesNeeded, totalCount)
	}
	DPrintf("[%d] Checking for promotion Counted votes %d ; Need %d ; TotalCounted %d ; state %s ; startingTerm %d ; currentTerm %d", rf.me, voteCount, votesNeeded, totalCount, rf.state, startingTerm, rf.currentTerm)
	if voteCount >= votesNeeded && rf.currentTerm == startingTerm && rf.state != FOLLOWER {
		rf.mu.Unlock()
		rf.convertToLeader()
		return
	} else if rf.state != FOLLOWER && totalCount < len(rf.peers) {
		rf.currentTerm--
		rf.mu.Unlock()
		rf.InitiateElection()
		return
	} else {
		DPrintf("[%d] Not promoted :D", rf.me)
	}
	rf.mu.Unlock()
}

func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	log.Printf("[%d] Promoting to leader", rf.me)
	rf.state = LEADER
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
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
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.setElectionTimeout()
	rf.mu.Unlock()
}

// MUST call WITH LOCK
func (rf *Raft) setElectionTimeout() {
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
	if args.Term > rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isRequestLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.VoteGranted = true
		rf.lastHeartbeat = time.Now()
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
	}
	if len(rf.log) == 0 {
		DPrintf("[%d] RequestVote from Server %d Term %d WAS GRANTED %v ? MyTerm %d MyLog IsEmpty ", rf.me, args.CandidateId, args.Term, reply.VoteGranted, rf.currentTerm)
	} else {
		DPrintf("[%d] RequestVote from Server %d Term %d WAS GRANTED %v ? MyTerm %d MyLogLastTerm %d TheirLogLastTerm %d  MyLogLength %d TheirLastLogIndex %d", rf.me, args.CandidateId, args.Term, reply.VoteGranted, rf.currentTerm, rf.log[len(rf.log)-1].Term, args.LastLogTerm, len(rf.log)-1, args.LastLogIndex)
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.mu.Unlock()
		rf.convertToFollower()
		return
	}
	rf.mu.Unlock()
}

func (rf *Raft) isRequestLogUpToDate(lastLogTerm, lastLogIndex int) bool {
	if len(rf.log) == 0 {
		return true
	}
	return rf.log[len(rf.log)-1].Term < lastLogTerm || (rf.log[len(rf.log)-1].Term == lastLogTerm && len(rf.log)-1 <= lastLogIndex)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target ending heartbeats
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
	OperationId  int
}

func NewAppendEntrieArgs(term, leaderId, prevLogIndex, prevLogTerm, leaderCommit int, entries []LogEntry, operationId int) *AppendEntrieArgs {
	return &AppendEntrieArgs{Term: term,
		LeaderId: leaderId, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: entries, LeaderCommit: leaderCommit, OperationId: operationId}
}

type AppendEntrieReply struct {
	Term    int
	XTerm   int
	XIndex  int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntrieArgs, reply *AppendEntrieReply) {
	rf.mu.Lock()
	DPrintf("[%d] AppendEntries received from Server %d Term %d PrevLogIndex %d PrevLogTerm %d Entries %v LeaderCommit %d MyLog %v OperationId %v", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit, rf.log, args.OperationId)
	// if rf.lastRequest[args.LeaderId] == args.OperationId {
	// 	rf.mu.Unlock()
	// 	fmt.Printf("[%d] DUPLICATE REQUEST AppendEntries received from Server %d Term %d PrevLogIndex %d PrevLogTerm %d Entries %v LeaderCommit %d MyLog %v", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit, rf.log)
	// 	reply.Success = true
	// 	return
	// }
	// rf.lastRequest[args.LeaderId] = args.OperationId
	if args.Term >= rf.currentTerm {
		rf.lastHeartbeat = time.Now()
		rf.setElectionTimeout()
		if rf.state != FOLLOWER {
			rf.mu.Unlock()
			rf.convertToFollower()
			rf.mu.Lock()
		}
		if len(rf.log) > args.PrevLogIndex {
			reply.Success = true
			if args.PrevLogIndex != -1 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
				DPrintf("[%d] Cutting log, MyLog %v rf.log[args.PrevLogIndex].Term %d args.PrevLogTerm %d", rf.me, rf.log, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
				rf.log = rf.log[:args.PrevLogIndex]
			}
			rf.log = append(rf.log, args.Entries...)
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
				rf.mu.Unlock()
				rf.applyEntries()
				rf.mu.Lock()
			}
			log.Printf("[%d] APPEND ENTRIES SUCCESS, NewLog %v CommitIndex %v, LastApplied %v", rf.me, rf.log, rf.commitIndex, rf.lastApplied)
		} else {
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
				reply.XTerm = rf.log[i].Term
			} else {
				reply.XTerm = -1
				reply.XIndex = -1
			}
			log.Printf("[%d] APPEND ENTRIES FAILED, len(log) %d args.PrevLogIndex %v args.PrevLogTerm %d MyLog %v reply %v", rf.me, len(rf.log), args.PrevLogIndex, args.PrevLogTerm, rf.log, reply)
		}
	} else {
		DPrintf("[%d] AppendEntries DENIED BECAUSE OF TERM from Server %d Term %d MyTerm %d", rf.me, args.LeaderId, args.Term, rf.currentTerm)
	}
	rf.currentTerm = Max(rf.currentTerm, args.Term)
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

// Applies entries from lastApplied to commitIndex
func (rf *Raft) applyEntries() {
	rf.mu.Lock()
	for rf.lastApplied < rf.commitIndex {
		DPrintf("[%d] Trying to apply entries lastApplied %d, commitIndex %d, log %v", rf.me, rf.lastApplied, rf.commitIndex, rf.log)
		rf.lastApplied++
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.mu.Unlock()
		DPrintf("[%d] Applying entry number %d", rf.me, msg.CommandIndex)
		*rf.apllyCh <- msg
		log.Printf("[%d] Applied entry number %d ; command: %v ; log %v", rf.me, msg.CommandIndex, msg.Command, rf.log)
		rf.mu.Lock()
	}
	rf.mu.Unlock()
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
	if rf.state == LEADER {
		rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	}
	return rf.lastApplied + 1, rf.currentTerm, rf.state == LEADER
}

// finds first log index which term matches the argument term
// if not found, returns 0
// Must be called with lock
func (rf *Raft) matchXEntry(term int) int {
	var curIndex int
	DPrintf("[%d] Searching for entry with term %d", rf.me, term)
	for i := len(rf.log) - 1; i > 0 && rf.log[i].Term > term; i-- {
		curIndex = i
		if rf.log[i].Term == term {
			DPrintf("[%d] Found entry with term %d, logEntry %v", rf.me, term, rf.log[curIndex])
			break
		}
	}
	if curIndex == 0 {
		return curIndex
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
	for !rf.killed() {
		// Check if a leader election should be started.
		rf.mu.Lock()
		DPrintf("[%d] Tick My Log %v lastApplied %d Commit Index %d", rf.me, rf.log, rf.lastApplied, rf.commitIndex)
		if rf.state != LEADER && time.Since(rf.lastHeartbeat) > rf.electionTimeout {
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
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		rf.mu.Lock()
		entries := rf.log[rf.nextIndex[server]:]
		DPrintf("[%d] Constructing for server %d, nextIndex %d, len(rf.log) %d entries %d", rf.me, server, rf.nextIndex[server]-1, len(rf.log), rf.log[rf.nextIndex[server]:])
		args := NewAppendEntrieArgs(rf.currentTerm, rf.me, rf.nextIndex[server]-1, rf.log[rf.nextIndex[server]-1].Term, rf.commitIndex, make([]LogEntry, len(entries)), rf.getNextOperationId())
		copy(args.Entries, entries)
		reply := &AppendEntrieReply{}
		rf.mu.Unlock()
		go rf.sendEntries(server, args, reply)
	}
}

var AppendEntryTimeout time.Duration

func (rf *Raft) sendEntries(server int, args *AppendEntrieArgs, reply *AppendEntrieReply) {
	*rf.apllyCh <- ApplyMsg{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}
	DPrintf("[%d] Sent to server %d args %v got reply %v", rf.me, server, *args, *reply)
	rf.mu.Lock()
	if rf.state != LEADER || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.mu.Unlock()
		rf.convertToFollower()
		return
	}
	DPrintf("[%d] Sent to server %d args %v got reply %v", rf.me, server, *args, *reply)
	if reply.Success {
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		if newMatchIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = newMatchIndex
		}
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else if reply.XIndex == -1 {
		rf.nextIndex[server] = 1
		rf.matchIndex[server] = 0
	} else {
		newNextIndex := len(rf.log) - 1
		for ; newNextIndex >= 0; newNextIndex-- {
			if rf.log[newNextIndex].Term == reply.XTerm {
				break
			}
		}
		// if not found, set nextIndex to conflictIndex
		if newNextIndex < 0 {
			rf.nextIndex[server] = reply.XIndex
		} else {
			rf.nextIndex[server] = newNextIndex
		}
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	}

	for n := len(rf.log) - 1; n >= rf.commitIndex; n-- {
		count := 1
		if rf.log[n].Term == rf.currentTerm {
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= n {
					count++
				}
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.mu.Unlock()
			go rf.applyEntries()
			return
		}
	}
	rf.mu.Unlock()
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
	AppendEntryTimeout = 5 * time.Second
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.apllyCh = &applyCh
	rf.setElectionTimeout()
	rf.convertToFollower()
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.lastRequest = make(map[int]int, len(rf.peers))
	rf.log = append(rf.log, LogEntry{Term: 0})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
