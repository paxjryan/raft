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
	"bytes"

	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

const STATE_FOLLOWER = 0
const STATE_CANDIDATE = 1
const STATE_LEADER = 2

const TICKER_SLEEPING_TIME = time.Millisecond

const HEARTBEAT_TIMEOUT = time.Millisecond * 100
const ELECTION_TIMEOUT = 1000

// actual election timeouts are in the range [ELECTION_TIMEOUT_MS, ELECTION_TIMEOUT_VARIATION * ELECTION_TIMEOUT_MS)
const ELECTION_TIMEOUT_VARIATION = 2

const DEBUG_ALL = false
const DEBUG_ELECTION_LEADER = DEBUG_ALL && true
const DEBUG_ELECTION_CANDIDATE = DEBUG_ALL && true
const DEBUG_ELECTION_FOLLOWER = DEBUG_ALL && true
const DEBUG_APPEND_ENTRIES_LEADER = DEBUG_ALL && true
const DEBUG_APPEND_ENTRIES_FOLLOWER = DEBUG_ALL && true
const DEBUG_LOCKS = false

func printLog(on bool, format string, v ...any) {
	if on {
		log.Printf(format, v...)
	}
}

func intMax(int1 int, int2 int) int {
	if int1 > int2 {
		return int1
	}
	return int2
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyChan        chan ApplyMsg
	sendApplyMsgCond *sync.Cond

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       int
	currentTerm int
	votedFor    int // index into peers[] containing the voted-for candidate during this term
	log         []Entry

	// Index of highest log entry known to be commmitted / applied to state machine (initialized to 0, increases monotonically)
	commitIndex int
	lastApplied int

	// For each server, index of next log entry to send / highest log entry known to be replicated (init to 0, increases monotonically)
	// Volatile state for leaders only
	nextIndex  []int
	matchIndex []int

	// time.Time of next heartbeat timeout (when elapsed, send new AppendEntries() RPC)
	heartbeatTimeout time.Time

	// randomized time.Time of next election timeout (when elapsed, -> candidate and start new election)
	electionTimeout time.Time

	// how many votes this Raft has received in the current election
	voteCount int
}

type Entry struct {
	Cmd  interface{}
	Term int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// printLog(DEBUG_LOCKS, "%d GetState took lock", rf.me)
	// defer printLog(DEBUG_LOCKS, "%d GetState released lock", rf.me)

	term := rf.currentTerm
	isleader := rf.state == STATE_LEADER

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var entries []Entry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&entries) != nil {
		log.Fatal("decode unsuccessful")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = entries
	}
}

// Assumes rf is locked.
func (rf *Raft) updateState(newState int) {
	if newState == STATE_LEADER {
		rf.state = STATE_LEADER
		// printLog(DEBUG_ELECTION_LEADER, "%d is leader now", rf.me)
		// log.Printf("%d is leader now", rf.me)

		// initialize nextIndex[] and matchIndex[]
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := range rf.nextIndex {
			// nextIndex initialized to leader last log index + 1
			rf.nextIndex[i] = len(rf.log)

			// matchIndex initialized to -1
			rf.matchIndex[i] = -1
		}

		// send initial heartbeat
		rf.leaderBroadcastHeartbeat()
	} else if newState == STATE_CANDIDATE {
		rf.state = STATE_CANDIDATE
		rf.holdElection()
	} else if newState == STATE_FOLLOWER {
		rf.state = STATE_FOLLOWER
	}

	// log.Printf("raft %d state updated: %d", rf.me, newState)
}

// copied from Figure 2:
// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
// Assumes rf is locked.
func (rf *Raft) checkTerm(newTerm int) (upToDate bool) {
	if newTerm > rf.currentTerm {
		rf.currentTerm = newTerm
		rf.votedFor = -1
		rf.persist()

		// printLog(DEBUG_ELECTION_CANDIDATE, "checkTerm: server %d term to %d", rf.me, rf.currentTerm)
		rf.updateState(STATE_FOLLOWER)
		return false
	}
	return true
}

// Assumes rf is locked.
// Returns true if candidate's log is at least as up to date as rf.me.
// Taken from Section 5.4.1 of the paper:
// Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
// If the logs end with the same term, then whichever log is longer is more up-to-date.
func (rf *Raft) candidateLogUpToDate(candidateLastTerm int, candidateLastIndex int) bool {
	t, i := rf.getLastLogTermAndIndex()

	if candidateLastTerm > t {
		return true
	}

	if candidateLastTerm == t {
		if candidateLastIndex >= i {
			return true
		}
	}
	return false
}

// Assumes rf is locked.
// Get the term and index of the last entry in log. Returns (0, 0) if log is empty.
func (rf *Raft) getLastLogTermAndIndex() (term int, index int) {
	if len(rf.log) == 0 {
		return 0, 0
	}
	return rf.log[len(rf.log)-1].Term, len(rf.log) - 1
}

// Assumes rf is locked.
func (rf *Raft) setMatchIndex(i int, val int) {
	rf.matchIndex[i] = val

	newCommitIndex := -1

	// Iterate while incrementing N until conditions are not met
	// If there exists an N such that N > commitIndex...
	for n := rf.commitIndex + 1; n < len(rf.log); n++ {
		if len(rf.log)-1 < n {
			// printLog(DEBUG_APPEND_ENTRIES_LEADER, "%d len(rf.log-1) (%d) < n (%d)", rf.me, len(rf.log)-1, n)
			break
		} else if rf.log[n].Term != rf.currentTerm {
			// ... log[N].term == currentTerm...
			// printLog(DEBUG_APPEND_ENTRIES_LEADER, "%d rf.log[n].Term (%d) != rf.currentTerm (%d)", rf.me, rf.log[n].Term, rf.currentTerm)
		} else {
			// ... and a majority of matchIndex[i] >= N: set commitIndex = N.
			majority := ((len(rf.peers) - 1) / 2) + 1
			count := 1

			for ind := range rf.matchIndex {
				if rf.matchIndex[ind] >= n {
					count++
				}
			}

			if count >= majority {
				newCommitIndex = n
			}
		}
	}

	if newCommitIndex > rf.commitIndex {
		rf.commitIndex = newCommitIndex
	}
}

//
// REQUEST VOTE, ELECTIONS, AND RELATED FUNCTIONALITY
//

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

func (rf *Raft) sendRequestVote(serverIndex int, termOfElection int) {
	rf.mu.Lock()
	// printLog(DEBUG_LOCKS, "%d sendRequestVote took lock (1)", rf.me)

	// printLog(DEBUG_ELECTION_LEADER, "(%d,%d) vote requested", rf.me, serverIndex)

	if rf.state != STATE_CANDIDATE {
		// printLog(DEBUG_ELECTION_CANDIDATE, "rf.state (%d) != STATE_CANDIDATE", rf.state)
		rf.mu.Unlock()
		return
	}

	lastLogTerm, lastLogIndex := rf.getLastLogTermAndIndex()

	args := RequestVoteArgs{
		Term:         termOfElection,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	reply := RequestVoteReply{}

	rf.mu.Unlock()
	// printLog(DEBUG_LOCKS, "%d sendRequestVote released lock (1)", rf.me)

	if rf.peers[serverIndex].Call("Raft.RequestVote", &args, &reply) {
		rf.mu.Lock()
		// printLog(DEBUG_LOCKS, "%d sendRequestVote took lock (2)", rf.me)
		defer rf.mu.Unlock()
		// defer printLog(DEBUG_LOCKS, "%d sendRequestVote released lock (2)", rf.me)

		// printLog(DEBUG_ELECTION_CANDIDATE, "request vote rpc success, reply.Term: %d and termOfElection: %d", reply.Term, termOfElection)

		// check to make sure this raft's term is up to date
		if reply.Term > termOfElection {
			rf.checkTerm(reply.Term)
			// printLog(DEBUG_ELECTION_CANDIDATE, "request vote reply.Term (%d) > termOfElection (%d)", reply.Term, termOfElection)
			return
		}

		if rf.state != STATE_CANDIDATE {
			// printLog(DEBUG_ELECTION_CANDIDATE, "rf.state != STATE_CANDIDATE (%d)", rf.state)
			return
		}

		if reply.Term == termOfElection && reply.VoteGranted {
			rf.voteCount++
			voteMajority := ((len(rf.peers) - 1) / 2) + 1
			// printLog(DEBUG_ELECTION_CANDIDATE, "reply.Term == termOfElection and vote granted! vote majority: %d, voteCount: %d", voteMajority, rf.voteCount)

			if rf.voteCount >= voteMajority {
				rf.updateState(STATE_LEADER)
				// printLog(DEBUG_ELECTION_LEADER, "server %d elected with %d votes out of %d", rf.me, rf.voteCount, len(rf.peers))
			}
		}
	}

	// If sendRequestVote returns false (rpc fail), ignore it.
	// Do not retry; wait until next election to request vote from this server again.
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	// printLog(DEBUG_LOCKS, "%d RequestVote took lock", rf.me)
	defer rf.mu.Unlock()
	// defer printLog(DEBUG_LOCKS, "%d RequestVote released lock", rf.me)

	// Update rf.currentTerm if behind
	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		// printLog(DEBUG_ELECTION_FOLLOWER, "(%d,%d) vote denied: args.term (%d) < currentTerm (%d)", args.CandidateId, rf.me, args.Term, rf.currentTerm)
		return
	}

	// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.candidateLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()

		// printLog(DEBUG_ELECTION_FOLLOWER, "(%d,%d) vote granted", args.CandidateId, rf.me)
		rf.resetElectionTimeout()
		return
	}

	reply.VoteGranted = false
	// printLog(DEBUG_ELECTION_FOLLOWER, "(%d,%d) vote denied: already voted (%d) or candidate log not up to date", args.CandidateId, rf.me, rf.votedFor)
}

// The electionTicker go routine starts a new election if this peer hasn't received heartbeats recently.
func (rf *Raft) electionTicker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()
		// printLog(DEBUG_LOCKS, "%d electionTicker took lock", rf.me)

		// if rf.state == STATE_LEADER {
		// 	log.Printf("%d leader tick", rf.me)
		// }

		// log.Printf("%d election tick", rf.me)
		if rf.state != STATE_LEADER && time.Now().After(rf.electionTimeout) {
			// Convert to candidate if necessary; start new election; reset election timeout
			// log.Printf("%d non-leader tick", rf.me)
			rf.updateState(STATE_CANDIDATE)
		}

		rf.mu.Unlock()
		// printLog(DEBUG_LOCKS, "%d electionTicker released lock", rf.me)
		time.Sleep(TICKER_SLEEPING_TIME)
	}
}

// Asumes rf is locked.
func (rf *Raft) holdElection() {
	// Copied from Figure 2
	// On conversion to candidate, start election:
	// 	Increment currentTerm
	// 	Vote for self
	// 	Reset election timer
	// 	Send RequestVoteRPCs to all other servers
	// If votes received from majority of servers: become leader

	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()

	rf.voteCount = 1
	termOfElection := rf.currentTerm
	// printLog(DEBUG_ELECTION_CANDIDATE, "current term went up: server %d term to %d", rf.me, rf.currentTerm)

	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(i, termOfElection)
		}
	}

	rf.resetElectionTimeout()
}

// Asumes rf is locked.
func (rf *Raft) resetElectionTimeout() {
	// copied from Go By Example (https://gobyexample.com/random-numbers)
	// not sure if this is safe for use concurrently... it'll work for now.

	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)

	// log.Printf("now: %v + timeoutTime: %v = timeout %v", time.Now(), ELECTION_TIMEOUT*time.Duration(ELECTION_TIMEOUT_VARIATION*(1+r.Float32())),
	// 	time.Now().Add(ELECTION_TIMEOUT*time.Duration(ELECTION_TIMEOUT_VARIATION*(1+r.Float32()))))
	rf.electionTimeout = time.Now().Add(time.Millisecond * time.Duration(r.Int31n(ELECTION_TIMEOUT)))
	rf.electionTimeout = rf.electionTimeout.Add(time.Millisecond * ELECTION_TIMEOUT)
	// rf.electionTimeout = time.Now().Add(time.Millisecond * time.Duration(ELECTION_TIMEOUT*(ELECTION_TIMEOUT_VARIATION)*(r.Float32())))

	// log.Printf("%d now: %v, new election timeout: %v", rf.me, time.Now(), rf.electionTimeout)
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
// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }

// APPEND ENTRIES RPC, HEARTBEATS, AND RELATED FUNCTIONALITY

// field names must start with capital letters!
type AppendEntriesArgs struct {
	// Your data here (3A, 3B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

// field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (3A).
	Term    int
	Success bool

	// from https://pdos.csail.mit.edu/6.824/labs/lab-raft.html
	XTerm  int // conflicting entry term
	XIndex int // first index of conflicting entry term
	XLen   int // log length
}

// Assumes rf is NOT locked
// If heartbeat: broadcastingNewEntry = false, entryIndex = -1
// If broadcasting a single entry: broadcastingNewEntry = true, entryIndex = log index of entry to broadcast
func (rf *Raft) sendAppendEntries(followerIndex int, termWhenSendingRpc int) {
	rf.mu.Lock()
	// printLog(DEBUG_LOCKS, "%d sendAppendEntries took lock (start)", rf.me)

	// sendingMissingEntries := true
	// initLogLength := len(rf.log)

	// invariant: rf.mu.Lock is held at start and end of each loop iteration
	// If last log index >= nextIndex for a follower:
	// for len(rf.log)-1 >= rf.nextIndex[followerIndex] {
	// for {
	// prevLogTerm, prevLogIndex := rf.getPrevLogTermAndIndex(followerIndex)
	prevLogIndex := rf.nextIndex[followerIndex] - 1
	prevLogTerm := -1
	if prevLogIndex >= 0 {
		prevLogTerm = rf.log[prevLogIndex].Term
	}

	// Initialize new AppendEntriesArgs for heartbeat
	args := AppendEntriesArgs{
		Term:         termWhenSendingRpc,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
	}

	// If we are sending missing entries, add each to args.Entries
	// if sendingMissingEntries {
	args.Entries = rf.log[rf.nextIndex[followerIndex]:] //:initLogLength]
	// lenEntries = len(args.Entries)
	// entriesToAdd += initLogLength - rf.nextIndex[followerIndex]
	// for n := rf.nextIndex[followerIndex]; n < initLogLength; n++ {
	// 	args.Entries = append(args.Entries, rf.log[n])
	// }
	// }

	// // If we are broadcasting a new entry, add to args.Entries
	// if broadcastingNewEntry {
	// 	// args.Entries = rf.log[entryIndex : entryIndex+1]
	// 	args.Entries = append(args.Entries, rf.log[entryIndex])
	// }

	lenEntries := len(args.Entries)

	// Initialize new blank AppendEntries reply
	reply := AppendEntriesReply{}

	rf.mu.Unlock()
	// printLog(DEBUG_APPEND_ENTRIES_LEADER, "(%d,%d) sending AppendEntries (hb %t)...", rf.me, followerIndex, lenEntries == 0)
	// printLog(DEBUG_LOCKS, "%d sendAppendEntries released lock (sending...)", rf.me)

	// Send AppendEntries RPC with log entries starting at nextIndex
	rpcSuccess := rf.peers[followerIndex].Call("Raft.AppendEntries", &args, &reply)

	if rf.killed() {
		// rf.mu.Lock()
		return
	}

	// printLog(DEBUG_APPEND_ENTRIES_LEADER, "(%d,%d) AppendEntries (hb %t) response: success %t", rf.me, followerIndex, lenEntries == 0, reply.Success)
	// printLog(DEBUG_LOCKS, "%d sendAppendEntries taking lock (receiving...)", rf.me)
	rf.mu.Lock()
	// printLog(DEBUG_LOCKS, "%d sendAppendEntries took lock (receiving...)", rf.me)

	if rpcSuccess {
		// log.Printf("%d z", rf.me)
		// printLog(DEBUG_APPEND_ENTRIES_LEADER, "(%d,%d) appendEntries rpcSuccess", rf.me, followerIndex)
		// log.Printf("%d currentTerm, %d reply.Term", rf.currentTerm, reply.Term)

		if reply.Term > termWhenSendingRpc {
			// printLog(DEBUG_APPEND_ENTRIES_LEADER, "%d appendEntries RPC outdated (termWhenSendingRpc: %d) vs. reply (reply.Term: %d)", rf.me, termWhenSendingRpc, reply.Term)
			// log.Printf("%d a", rf.me)
			rf.checkTerm(reply.Term)

			// log.Printf("%d a2", rf.me)
			// printLog(DEBUG_LOCKS, "%d sendAppendEntries released lock (reply.Term > termWhenSendingRpc)", rf.me)
			rf.mu.Unlock()
			return
		}

		// not sure why first check is necessary
		if reply.Term != termWhenSendingRpc || rf.state != STATE_LEADER {
			// printLog(DEBUG_APPEND_ENTRIES_LEADER, "%d reply.Term (%d) != termWhenSendingRpc (%d) || rf.state != STATE_LEADER (%d)", rf.me, reply.Term, termWhenSendingRpc, rf.state)

			// log.Printf("%d b", rf.me)
			// printLog(DEBUG_LOCKS, "%d sendAppendEntries released lock (reply.Term != termWhenSendingRpc || rf.state != STATE_LEADER)", rf.me)
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			// log.Printf("%d c", rf.me)
			// If successful: update nextIndex and matchIndex for follower

			// If we don't need to send missing entries or we just sent missing entries, and the RPC was a success,
			// the follower is now up to date. We can update nextIndex/matchIndex and kick out of the loop
			rf.nextIndex[followerIndex] = intMax(prevLogIndex+lenEntries+1, rf.nextIndex[followerIndex])
			rf.setMatchIndex(followerIndex, intMax(prevLogIndex+lenEntries, rf.matchIndex[followerIndex]))
			// printLog(DEBUG_APPEND_ENTRIES_LEADER, "(%d,%d) follower now up to date: nextIndex: %v, matchIndex: %v", rf.me, followerIndex, rf.nextIndex, rf.matchIndex)

			// printLog(DEBUG_LOCKS, "%d sendAppendEntries released lock", rf.me)
			rf.mu.Unlock()
			return

			// // If we need to send missing entries but we haven't sent them yet, and the RPC was a success, then
			// // we just found the index where the logs most recently match. Retry the RPC, sending the missing entries
			// if !sendingMissingEntries {
			// 	rf.setMatchIndex(followerIndex, intMax(prevLogIndex, rf.matchIndex[followerIndex]))
			// 	sendingMissingEntries = true
			// }
		} else {
			// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
			// sendingMissingEntries = false
			// log.Printf("%d d", rf.me)

			if reply.XTerm == -1 {
				// follower's log is too short (case 3)
				rf.nextIndex[followerIndex] = reply.XLen
			} else if reply.XTerm > termWhenSendingRpc {
				// leader doesn't have XTerm (case 1)
				rf.nextIndex[followerIndex] = reply.XIndex
			} else {
				// leader has XTerm (case 2)
				var xTermLastIndex int
				for i := len(rf.log) - 1; i >= 0; i-- {
					if rf.log[i].Term == reply.XTerm {
						xTermLastIndex = i
						break
					}
				}
				rf.nextIndex[followerIndex] = xTermLastIndex
			}

			// rf.nextIndex[followerIndex]--
			// printLog(DEBUG_LOCKS, "%d sendAppendEntries released lock (changed nextIndex, retrying)", rf.me)
			rf.mu.Unlock()

			rf.sendAppendEntries(followerIndex, termWhenSendingRpc)
			return
			// printLog(DEBUG_APPEND_ENTRIES_LEADER, "decremented nextIndex to: %d, reply.Success: %t, prevLogIndex: %d, prevLogTerm: %d", rf.nextIndex[followerIndex], reply.Success, prevLogIndex, prevLogTerm)
		}
	} else {
		// printLog(DEBUG_APPEND_ENTRIES_LEADER, "rpc fail")
		// log.Printf("%d e", rf.me)

		rf.mu.Unlock()
	}
	// }
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// printLog(DEBUG_LOCKS, "%d receiving heartbeat before lock", rf.me)

	rf.mu.Lock()
	// printLog(DEBUG_LOCKS, "%d AppendEntries took lock", rf.me)
	defer rf.mu.Unlock()
	// defer printLog(DEBUG_LOCKS, "%d AppendEntries released lock", rf.me)

	isHeartbeat := len(args.Entries) == 0
	// if isHeartbeat {
	// printLog(DEBUG_APPEND_ENTRIES_FOLLOWER, "%d receiving heartbeat, len(entries) = %d...", rf.me, len(args.Entries))
	if len(args.Entries) > 0 {
		// printLog(DEBUG_APPEND_ENTRIES_FOLLOWER, "%d entries[0] = %d", rf.me, args.Entries[0])
	}
	// }

	// log.Printf("appendEntries handler: args wants to add at prevLogIndex: %d", args.PrevLogIndex)

	// update rf.currentTerm if behind
	rf.checkTerm(args.Term)

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		// printLog(true, "%d args.Term (%d) < rf.currentTerm (%d)", rf.me, args.Term, rf.currentTerm)

		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// Set state to follower and reset election timeout
	rf.updateState(STATE_FOLLOWER)
	rf.resetElectionTimeout()

	if args.PrevLogIndex >= 0 && len(rf.log)-1 >= args.PrevLogIndex {
		// printLog(DEBUG_APPEND_ENTRIES_FOLLOWER, "%d rf.log[args.PrevLogIndex].Term (%d) == args.PrevLogTerm (%d)", rf.me, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
	}
	// Reply false if log doesn't contain an entry at PrevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex != -1 &&
		(len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Success = false
		reply.Term = rf.currentTerm

		if len(rf.log) <= args.PrevLogIndex {
			reply.XTerm = -1
			reply.XIndex = -1
		} else {
			reply.XTerm = rf.log[args.PrevLogIndex].Term
			for i := 0; i < len(rf.log); i++ {
				if rf.log[i].Term == reply.XTerm {
					reply.XIndex = i
					break
				}
			}
		}
		reply.XLen = len(rf.log)

		// printLog(DEBUG_APPEND_ENTRIES_FOLLOWER, "%d prevLogIndex (%d) entry term does not match prevLogTerm (%d)", rf.me, args.PrevLogIndex, args.PrevLogTerm)
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	lastMatchingIndex := args.PrevLogIndex
	// printLog(DEBUG_APPEND_ENTRIES_FOLLOWER, "%d lastMatchingIndex = %d", rf.me, lastMatchingIndex)
	entryIndex := 0

	// while lastMatchingIndex is still in bounds of rf.log, and entryIndex is still in bounds of args.Entries
	for !isHeartbeat && lastMatchingIndex+1 < len(rf.log) && entryIndex < len(args.Entries) {
		// if next entry's term does not match, truncate log
		if rf.log[lastMatchingIndex+1].Term != args.Entries[entryIndex].Term {
			// printLog(DEBUG_APPEND_ENTRIES_FOLLOWER, "%d rf.log[lastMatchingIndex+1].Term (%d) != args.Entries[entryIndex].Term (%d)", rf.me, rf.log[lastMatchingIndex+1].Term, args.Entries[entryIndex].Term)
			rf.log = rf.log[0 : lastMatchingIndex+1]
			rf.persist()
			break
		}

		lastMatchingIndex++
		// printLog(DEBUG_APPEND_ENTRIES_FOLLOWER, "%d lastMatchingIndex++", rf.me)
		entryIndex++
	}

	// Append any new entries not in the log
	for ; entryIndex < len(args.Entries); entryIndex++ {
		// printLog(DEBUG_APPEND_ENTRIES_FOLLOWER, "%d adding entry at index %d, term %d", rf.me, len(rf.log), args.Entries[entryIndex].Term)
		rf.log = append(rf.log, args.Entries[entryIndex])
		rf.persist()
		lastMatchingIndex++
	}

	// If LeaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < len(rf.log)-1 {
			// rf.setCommitIndex(args.LeaderCommit)
			rf.commitIndex = args.LeaderCommit
			// printLog(DEBUG_APPEND_ENTRIES_FOLLOWER, "setCommitIndex to args.LeaderCommit (new CI: %d) (leader %d) for server %d", rf.commitIndex, args.LeaderId, rf.me)
		} else {
			// printLog(DEBUG_APPEND_ENTRIES_FOLLOWER, "args.LeaderCommit: %d, old rf.commitIndex: %d for server %d", args.LeaderCommit, rf.commitIndex, rf.me)
			// rf.setCommitIndex(len(rf.log) - 1)
			rf.commitIndex = len(rf.log) - 1
			// printLog(DEBUG_APPEND_ENTRIES_FOLLOWER, "setCommitIndex to len(rf.log)-1 (new CI: %d) (leader %d) for server %d", rf.commitIndex, args.LeaderId, rf.me)
			// log.Printf("args.LeaderCommit: %d, rf.commitIndex: %d", args.LeaderCommit, rf.commitIndex)
		}
	}

	// log.Printf("heartbeat has reset election timeout for %d to %v, now: %v", rf.me, rf.electionTimeout, time.Now())

	// printLog(DEBUG_APPEND_ENTRIES_FOLLOWER, "%d log is now %v", rf.me, rf.log)

	reply.Term = rf.currentTerm
	reply.Success = true
	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = -1
}

func (rf *Raft) heartbeatTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		// printLog(DEBUG_LOCKS, "%d heartbeatTicker took lock", rf.me)

		// if rf.state == STATE_LEADER {
		// 	log.Printf("got lock at time %v", time.Now())
		// }

		if rf.state == STATE_LEADER && time.Now().After(rf.heartbeatTimeout) {
			// Send heartbeats out to all followers and reset heartbeat timeout
			// log.Printf("hb timeout expire")
			// log.Printf("%d leader state, timeout has expired", rf.me)
			rf.leaderBroadcastHeartbeat()
		}

		// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++

			// log.Printf("%d sendCommittedLogEntry: %v, commandIndex = %d", rf.me, rf.log[rf.lastApplied].Cmd, rf.lastApplied)

			rf.applyChan <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Cmd,
				CommandIndex: rf.lastApplied + 1,
			}
		}

		rf.mu.Unlock()
		// printLog(DEBUG_LOCKS, "%d heartbeatTicker released lock", rf.me)

		time.Sleep(TICKER_SLEEPING_TIME)
	}
}

// Assumes rf is locked.
func (rf *Raft) leaderBroadcastHeartbeat() {
	rf.resetHeartbeatTimeout()

	for i := range rf.peers {
		if i != rf.me {
			go rf.sendAppendEntries(i, rf.currentTerm)
		}
	}
}

// Assumes rf is locked.
func (rf *Raft) resetHeartbeatTimeout() {
	rf.heartbeatTimeout = time.Now().Add(HEARTBEAT_TIMEOUT)
	// printLog(DEBUG_APPEND_ENTRIES_LEADER, "heartbeat timeout for %d reset to %v, now: %v", rf.me, rf.heartbeatTimeout, time.Now())
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
	// printLog(DEBUG_LOCKS, "%d Start took lock", rf.me)
	// defer printLog(DEBUG_LOCKS, "%d Start released lock", rf.me)

	// log.Printf("%d cp a", rf.me)

	if rf.state != STATE_LEADER {
		return -1, -1, false
	}

	index := len(rf.log)
	term := rf.currentTerm
	isLeader := true

	e := Entry{
		Cmd:  command,
		Term: term,
	}
	// log.Printf("%d cp b", rf.me)

	// add entry to leader log and update each follower log if necessary
	rf.log = append(rf.log, e)
	rf.persist()
	// printLog(DEBUG_APPEND_ENTRIES_LEADER, "")
	// printLog(DEBUG_APPEND_ENTRIES_LEADER, "%d BROADCASTING NEW LOG ENTRY (index %d) %v....", rf.me, index, e)

	// log.Printf("%d cp c", rf.me)

	rf.leaderBroadcastHeartbeat()

	time.Sleep(TICKER_SLEEPING_TIME / 10)

	// log.Printf("%d cp d", rf.me)

	// Your code here (3B).

	return index + 1, term, isLeader
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
	// rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// // Assumes rf is NOT locked
// func (rf *Raft) processSingleHeartbeat(serverIndex int) {
// 	rf.mu.Lock()

// 	argsTerm := rf.currentTerm
// 	// prevLogIndex, prevLogTerm := rf.getPrevLogTermAndIndex(serverIndex)

// 	// Initialize new AppendEntriesArgs and AppendEntriesReply
// 	args := AppendEntriesArgs{
// 		Term: argsTerm,
// 		// LeaderId:     rf.me,
// 		// PrevLogIndex: prevLogIndex,
// 		// PrevLogTerm:  prevLogTerm,
// 		// Entries:      rf.log[rf.nextIndex[serverIndex]:],
// 		LeaderCommit: rf.commitIndex,
// 	}
// 	reply := AppendEntriesReply{}
// 	rf.mu.Unlock()

// 	if rf.sendAppendEntries(serverIndex, &args, &reply) {
// 		// check to make sure rf's term is up to date
// 		rf.mu.Lock()
// 		rf.checkTerm(reply.Term)
// 		rf.mu.Unlock()
// 	}

// 	// If sendAppendEntries returns false, ignore
// }

func (rf *Raft) sendCommittedLogEntries() {
	rf.sendApplyMsgCond.L.Lock()

	for {
		rf.sendApplyMsgCond.Wait()

		rf.mu.Lock()
		// printLog(DEBUG_LOCKS, "%d sendCommittedLogEntries took lock", rf.me)

		// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++

			// log.Printf("%d sendCommittedLogEntry: CommandIndex = %d", rf.me, rf.lastApplied)

			rf.applyChan <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Cmd,
				CommandIndex: rf.lastApplied + 1,
			}
		}

		rf.mu.Unlock()
		// printLog(DEBUG_LOCKS, "%d sendCommittedLogEntries released lock", rf.me)
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
	rf.applyChan = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.state = STATE_FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.commitIndex = -1
	rf.lastApplied = -1

	mu := sync.Mutex{}
	rf.sendApplyMsgCond = sync.NewCond(&mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.resetHeartbeatTimeout()
	go rf.heartbeatTicker()
	rf.resetElectionTimeout()
	go rf.electionTicker()

	// go rf.sendCommittedLogEntries()

	// log.Printf("new raft online: %d", rf.me)

	return rf
}
