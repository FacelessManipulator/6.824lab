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

import "sync"
import "labrpc"
import "time"
import "math/rand"
// import "fmt"
import "bytes"
import "labgob"



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

type Log struct {
	Command 	interface{}
	Term 		int
	Index 		int
}

type State int8
const (
	Leader 		State = iota
	Candidate 	
	Follower	
	Killed
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	step	  int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm	int
	logs		[]Log

	committedIndex	int
	lastApplied	int
	voteFor		int

	state 				State
	// the temperary state of the term process in main loop, may out-of-date
	temp				*TempState
	// if a handle function want to restart the main loop, send a signal to this ch
	stateReset			chan int
	heartBeatTimerReset	chan int

	applyCh 	chan ApplyMsg
	replyCh		chan *ReplyContext
}

type TempState struct {
// write field
	votes 		int
	newIndex	[]int
	matchIndex	[]int

// read only field
	term 		int
	state 		State
	lastLog 	Log
}

func (ts *TempState) reset(rf *Raft) *TempState {
	ts.term = rf.currentTerm
	ts.votes = 1
	ts.state = rf.state
	ts.newIndex = make([]int, len(rf.peers))
	ts.matchIndex = make([]int, len(rf.peers))
	if len(rf.logs) > 0 {
		ts.lastLog = rf.logs[len(rf.logs) - 1]
	} else {
		ts.lastLog = Log{-rf.me, -1, -1}
	}
	for i:=0; i<len(rf.peers); i++ {
		ts.newIndex[i] = len(rf.logs)
		ts.matchIndex[i] = -1
	}
	ts.matchIndex[rf.me] = len(rf.logs)-1
	rf.temp = ts
	return ts
}

func checkConflict(a []Log, b []Log) int {
	for i,v := range a {
		if len(b)-1 < i {
			return -1
		} else if v!=b[i]{
				return i
		}
	}
	return -1
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
	isleader = rf.state == Leader
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// full write to files
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.committedIndex)
	e.Encode(rf.lastApplied)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

	// apply to state machine
	for i := rf.lastApplied+1; i <= rf.committedIndex; i++ {
		rf.applyCh <- ApplyMsg{true, rf.logs[i].Command, rf.logs[i].Index+1}
		rf.lastApplied++
	}
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var logs []Log
	var term, commit, apply int
	if d.Decode(&term) != nil ||
	   d.Decode(&commit) != nil ||
	   d.Decode(&apply) != nil ||
	   d.Decode(&logs) != nil {
	  return
	} else {
	  rf.currentTerm = term
	  rf.committedIndex = commit
	  rf.lastApplied = apply
	  rf.logs = logs
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

type ReplyContext interface {
	handle(term int, from int) bool
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 		int
	VoteGranted	bool
}

func (reply *RequestVoteReply) handle(rf *Raft, requestTerm int, from int) bool {
	// event handled at main loop's concurrent stage
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > requestTerm || from == rf.me || rf.state != Candidate {
		return false
	}
	if reply.VoteGranted {
		// fmt.Printf("%d get vote in term %d with ok; %v\n", rf.me, term, *reply)
		rf.temp.votes++
		if rf.temp.votes > len(rf.peers)/2 {
			// the only entry to be leader
			if len(rf.logs) > 0 {
				// fmt.Printf("%d -> Leader in %d with log %v\n", rf.me, rf.currentTerm, rf.logs[len(rf.logs)-1])
			}
			rf.state = Leader
			signalTimeout(rf.stateReset, time.Millisecond*5)
		}
	} else if rf.currentTerm < reply.Term {
		rf.currentTerm = reply.Term
		rf.voteFor = -1
	}
	return true
}

type AppendEntries struct {
	Term	int
	LeaderId	int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries			[]Log
	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term			int
	Success			int
}

func (reply *AppendEntriesReply) handle(rf *Raft, requestTerm int, from int) bool {
	// event handled at main loop's concurrent stage
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > requestTerm || from == rf.me || rf.state != Leader {
		return false
	}
	if reply.Term == rf.currentTerm {
		rf.temp.newIndex[from] = min(reply.Success + rf.temp.newIndex[from], len(rf.logs))
		rf.temp.newIndex[from] = max(rf.temp.newIndex[from], 0)
		if reply.Success >= 0 {
			rf.temp.matchIndex[from] = rf.temp.newIndex[from] - 1
		}
	} else if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.voteFor = -1
		signalTimeout(rf.stateReset, time.Millisecond*5)
	}
	return true
}


func (rf *Raft) RequestAppendEntries(args *AppendEntries, reply *AppendEntriesReply){
	// handle heartbeat or log replica
	// fmt.Printf("%d receivce sync msg\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = args.LeaderId
		if rf.state != Follower {
			rf.state = Follower
			signalTimeout(rf.stateReset, time.Millisecond*5)
			*reply = AppendEntriesReply{rf.currentTerm, 0} // call me next time
			return
		} else {
			signalTimeout(rf.heartBeatTimerReset, time.Millisecond*5)
		}
	} else {
		*reply = AppendEntriesReply{rf.currentTerm, 0} // call me next time
		return
	}
	// fmt.Printf("%d receive before leader{%v@%v:%v, %v} self{@%d, %v, %v}\n", rf.me, args.LeaderId,args.Term, args.PrevLogIndex, len(args.Entries),rf.currentTerm, rf.committedIndex, len(rf.logs))
	confirmed := 0
	if args.PrevLogIndex == -1 {
		rf.logs = args.Entries
		confirmed = len(args.Entries)
	} else {
		var i int
		for i = len(rf.logs) - 1; i >= 0; i-- {
			if args.PrevLogIndex > rf.logs[i].Index {
				// move to last term index
				j, cursorTerm := i, rf.logs[i].Term
				for ; j >= 0; j-- {
					if cursorTerm > rf.logs[j].Term {
						break
					}
				}
				// new index = last index + confirmed = prevlogindex + 1 + confirmed = j + 1
				confirmed = j - args.PrevLogIndex
				break
			} else if args.PrevLogIndex == rf.logs[i].Index {
				if args.PrevLogTerm == rf.logs[i].Term {
					conflict := checkConflict(rf.logs[:i+1], args.Entries)
					rf.logs = rf.logs[:i+1+conflict]
					if i+1+len(args.Entries) > len(rf.logs) {
						rf.logs = append(rf.logs[:i+1], args.Entries...)
					}
					confirmed = len(args.Entries)
					break
				} else {
					// move to last term index
					j, cursorTerm := i, rf.logs[i].Term
					for ; j >= 0; j-- {
						if cursorTerm > rf.logs[j].Term {
							break
						}
					}
					confirmed = j - args.PrevLogIndex
					// conflict and delete
					rf.logs = rf.logs[:j+1]
					// new index = last index + confirmed = prevlogindex + 1 + confirmed = j + 1
					break
				}
			}
		}
		if i == -1 {
			// unfound, move cur to head
			confirmed = -args.PrevLogIndex - 1
		}
	}
	*reply = AppendEntriesReply{rf.currentTerm, confirmed}
	if confirmed >= 0 {
		rf.committedIndex = min(args.LeaderCommit, len(rf.logs)-1)
	}
	// fmt.Printf("%d receive after self{%v, %v} confirm=%d\n", rf.me, rf.committedIndex, len(rf.logs), confirmed)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		*reply = RequestVoteReply{rf.currentTerm, false}
		return
	} else if rf.currentTerm == args.Term{
		if rf.voteFor != -1 {
			rf.voteFor = args.CandidateId
			*reply = RequestVoteReply{rf.currentTerm, false}
			return
		}
	} else if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.voteFor = -1
		signalTimeout(rf.stateReset, time.Millisecond*5)
	}
	if len(rf.logs) == 0 || (rf.logs[len(rf.logs)-1].Term < args.LastLogTerm ||
		(rf.logs[len(rf.logs)-1].Term == args.LastLogTerm &&
		 rf.logs[len(rf.logs)-1].Index <= args.LastLogIndex)) {
		if len(rf.logs) > 0{
		// fmt.Printf("%d vote %d as leader, candidate{%d, %d}, self{%v}\n", rf.me, args.CandidateId, args.LastLogIndex, args.LastLogTerm, rf.logs[len(rf.logs) - 1])
		}
		*reply = RequestVoteReply{rf.currentTerm, true}
		rf.voteFor = args.CandidateId
	} else {
		*reply = RequestVoteReply{rf.currentTerm, false}
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
	index := len(rf.logs)
	term := rf.currentTerm
	isLeader := rf.state == Leader
	if isLeader {
		// append immediately
		log := Log{command, term, index}
		rf.logs = append(rf.logs, log)
		rf.temp.matchIndex[rf.me] = index
		// fmt.Printf("[%d start command %v total: %d]\n", rf.me, rf.logs[index], len(rf.logs))
	}
	rf.mu.Unlock()

	return index+1, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Killed
	signalTimeout(rf.stateReset, time.Millisecond*5)
}

// we use this func to maintain consistence among logs
func (rf *Raft) SyncLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i :=0; i < len(rf.peers); i++ {
		var syncLog []Log
		var prevLogIndex, prevLogTerm int
		if rf.temp.newIndex[i] > len(rf.logs) || i == rf.me {
			// this node may not be leader anymore
			continue
		}
		if rf.temp.newIndex[i] == len(rf.logs) {
			// heartbeat
			syncLog = nil
		} else if rf.temp.newIndex[i] + rf.step > len(rf.logs) {
			syncLog = rf.logs[rf.temp.newIndex[i]:]
		} else {
			syncLog = rf.logs[rf.temp.newIndex[i]:rf.temp.newIndex[i]+rf.step]
		}
		if rf.temp.newIndex[i] <= 0 {
			prevLogTerm = -1
			prevLogIndex = -1
		} else {
			prevLog := rf.logs[rf.temp.newIndex[i] - 1]
			prevLogTerm = prevLog.Term
			prevLogIndex = prevLog.Index
		}
		args := AppendEntries{rf.temp.term, rf.me, prevLogIndex, prevLogTerm, syncLog, rf.committedIndex}
		go func(to int, args *AppendEntries) {
			reply := AppendEntriesReply{}
			if rf.peers[to].Call("Raft.RequestAppendEntries", args, &reply) {
				reply.handle(rf, rf.temp.term, to)
			}
		}(i, &args)
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
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.step = 100
	rf.voteFor = -1

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.applyCh = applyCh
	rf.heartBeatTimerReset = make(chan int)
	rf.stateReset = make(chan int)
	// rf.voteTimerReset = make(chan int)
	rf.currentTerm = 0
	rf.committedIndex = -1
	rf.lastApplied = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// fmt.Println("make new raft ", rf.me)

	go rf.MainLoop()

	return rf
}

func (rf *Raft) doFollower() int {
	rf.mu.Unlock()
	defer rf.mu.Lock()
	// CONCURRENT START
	for {
		// it awkword that stop and reset may cause trouble, I have no choice but create a new timer
		select {
				case <- time.After(time.Millisecond * time.Duration(rand.Intn(150) + 150)):
					return FOLLOWER_TIMEOUT
				case <- rf.heartBeatTimerReset:
				case <- rf.stateReset:
					return RESET
		}
		rf.persist()
	}
	// CONOCURRENT OVER
}

func (rf *Raft) doLeader() int {
	rf.mu.Unlock()
	defer rf.mu.Lock()
	// CONCURRENT START
	for {
		rf.SyncLog()
		select {
			case <- time.After(time.Millisecond * 50):
			case <- rf.stateReset:
				return RESET
		}
		rf.mu.Lock()
		// fmt.Printf("\nmin match: [%d - %v, %d]\n", minMatch, rf.matchIndex, rf.committedIndex)
		for i := rf.committedIndex+1; i <= rf.temp.matchIndex[rf.me]; i++ {
			if rf.logs[i].Term < rf.temp.term {
				continue
			} else if counts(rf.temp.matchIndex, func(x int) bool {return x>=i}) * 2 > len(rf.peers) {
				rf.committedIndex++
			} else {
				break
			}
		}
		// fmt.Printf("%d commit %v-%v at %d\n", rf.me, rf.committedIndex, rf.temp.matchIndex, rf.temp.term)
		rf.mu.Unlock()
		rf.persist()
	}
	// CONCURRENT OVER
}

func (rf *Raft) doCandidate() int {
	rf.mu.Unlock()
	defer rf.mu.Lock()
	// CONCURRENT START
	args := RequestVoteArgs{rf.temp.term, rf.me, rf.temp.lastLog.Index, rf.temp.lastLog.Term}
	// request votes
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(to int) {
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(to, &args, reply) {
				reply.handle(rf, args.Term, to)
			}
		}(i)
	}
	select {
		// wait until election timeout or new leader come up
		case <- rf.stateReset:
			return RESET
		case <- time.After(time.Millisecond * time.Duration(rand.Intn(150) + 150)):
			return CANDIDATE_TIMEOUT
	}
	// CONCURRENT OVER
}

func (rf *Raft) MainLoop() {
	var tempState *TempState
	var ret int
	for {
		// term before work
		tempState = &TempState{}
		tempState.reset(rf)
		// process term
		switch tempState.state {
		case Follower:
			ret = rf.doFollower()
		case Leader:
			ret = rf.doLeader()
		case Candidate:
			ret = rf.doCandidate()
		case Killed:
			rf.mu.Unlock()
			return
		}
		// term after work
		switch ret {
		case CANDIDATE_TIMEOUT:
			if tempState.term < rf.currentTerm {
				// leader choosen and timeout come at the same time
				// leader choosen event was already handled
			} else {
				// increase the current term to avoid out-of-date arrived event
				rf.currentTerm++
				rf.voteFor = rf.me
				rf.state = Candidate
			}
		case FOLLOWER_TIMEOUT:
			rf.currentTerm++
			rf.voteFor = rf.me
			rf.state = Candidate
		case LEADER_TIMEOUT:
		case SUCCESS:
		case RESET:
		}
	}
}