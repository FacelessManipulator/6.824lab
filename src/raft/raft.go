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
import "fmt"
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm	int
	votedFor	int
	logs		[]Log

	committedIndex	int
	lastApplied	int
	newIndex	[]int
	matchIndex	[]int

	state 		State
	heartTimer	*time.Timer
	heartTimerReset	chan int
	voteTimer	*time.Timer
	// voteTimerReset	chan int

	voteM		sync.Mutex
	applyCh 	chan ApplyMsg
}

func (rf *Raft) TryVote(term int, candidateId int, haveto bool) (int, int, bool) {
	rf.voteM.Lock()
	defer rf.voteM.Unlock()
	if haveto {
		rf.currentTerm = term
		rf.votedFor = candidateId
		if rf.state != Follower {
			rf.state = Follower
			fmt.Printf("%d --(force)-> follower\n", rf.me)
		}
		return term, candidateId, true
	}
	var voted bool
	if term > rf.currentTerm {
		rf.votedFor = candidateId
		rf.currentTerm = term
		voted = true
	} else if term == rf.currentTerm &&
			(rf.votedFor == -1 || rf.votedFor == candidateId) {
		rf.votedFor = candidateId
		voted = true
	} else {
		term = rf.currentTerm
		candidateId = rf.votedFor
		voted = false
	}

	if voted && candidateId == rf.me {
		// if vote self to be leader, transfer self to candidate
		if rf.state == Follower {
			rf.state = Candidate
			fmt.Printf("%d -> candidate\n", rf.me)
		}
	} else if voted && candidateId != -1 {
		// if vote else to be leader, transfer self to follower
		if rf.state == Leader || rf.state == Candidate {
			rf.state = Follower
			fmt.Printf("%d -> follower\n", rf.me)
		}
	}

	return term, candidateId, voted
}

// we use this func to maintain consistence among logs
// feel free to pass the slice cuz it's only the header of continues buffer
func (rf *Raft) SyncLog(step int, replyC chan AppendEntriesReply) {
	syncLog := make([][]Log, len(rf.peers))
	prevLogIndex := make([]int, len(rf.peers))
	prevLogTerm := make([]int, len(rf.peers))

	// get the consistent information of Raft
	rf.mu.Lock()
	term := rf.currentTerm
	committed := rf.committedIndex
	for i :=0; i < len(rf.peers); i++ {
		var prevLog Log
		if rf.newIndex[i] >= len(rf.logs) {
			syncLog[i] = nil
		} else if rf.newIndex[i] + step > len(rf.logs) {
			syncLog[i] = rf.logs[rf.newIndex[i]:]
		} else {
			syncLog[i] = rf.logs[rf.newIndex[i]:rf.newIndex[i]+step]
		}
		if len(rf.logs) == 0 || rf.newIndex[i] == 0 {
			prevLogTerm[i] = -1
			prevLogIndex[i] = -1
		} else {
			prevLog = rf.logs[rf.newIndex[i] - 1]
			prevLogTerm[i] = prevLog.Term
			prevLogIndex[i] = prevLog.Index
		}
	}
	rf.mu.Unlock()

	// send logs
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(i int, replyC chan AppendEntriesReply) {
				defer func(){
					if err := recover(); err != nil {
						// fmt.Println("AppendEntries Reply timeout. [Closed channel]\t", err)
					}
				}() // handle panic
				reply := AppendEntriesReply{}
				args := AppendEntries{term, rf.me, prevLogIndex[i], prevLogTerm[i], syncLog[i], committed}
				ok := rf.peers[i].Call("Raft.RequestAppendEntries", &args, &reply)
				if ok {
					replyC <- reply
				} else {
					// rpc call failed, we assume it a heartbeat reply and do nothing
					replyC <- AppendEntriesReply{-1, 0, i}
				}
			}(i, replyC)
		}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term, _, _ = rf.TryVote(-1, -1, false)
	isleader = rf.state == Leader
	return int(term), isleader
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var logs []Log
	var term, votedFor int
	if d.Decode(&term) != nil ||
	   d.Decode(&votedFor) != nil ||
	   d.Decode(&logs) != nil {
	  return
	} else {
	  rf.currentTerm = term
	  rf.votedFor = votedFor
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

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 		int
	VoteGranted	bool
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
	Id 				int
}

func (rf *Raft) RequestAppendEntries(args *AppendEntries, reply *AppendEntriesReply){
	// handle heartbeat or log replica
	// fmt.Printf("%d receivce sync msg\n", rf.me)
	rf.mu.Lock()
	var log Log
	if len(rf.logs) > 0 {
		log = rf.logs[len(rf.logs)-1]
	} else {
		log = Log{1, -1, -1}
	}
	rf.mu.Unlock()
	if log.Term > args.Term {
		// invaild entries, we treat it as a heartbeat packet and do nothing
		*reply = AppendEntriesReply{rf.currentTerm, 0, rf.me}
		return
	}
	term, _, _ := rf.TryVote(args.Term, args.LeaderId, true)//maybe this is too heavy
	// reset heart timer for follower or leader
	rf.heartTimerReset <- 1
	rf.mu.Lock()
	defer rf.mu.Unlock()

	i, found := len(rf.logs)-1, false
	// linear search for convenience
	// buckets or skip list may be a better structure to store Logs
	for ; i >= 0; i-- {
		if args.PrevLogIndex > rf.logs[i].Index {
			// fmt.Printf("Lost %d log.\n", args.PrevLogIndex)
			i++
			break
		} else if args.PrevLogIndex == rf.logs[i].Index {
			if args.PrevLogTerm == rf.logs[i].Term {
				found = true
			}
			break
		}
	}
	// after the loop, i should be the very position which we should delete or append after
	confirmed := -1
	if !found && i == -1 {
		// unfound cuz prev empty
		// fmt.Printf("%d empty at %d\t", rf.me, args.PrevLogIndex)
		if args.PrevLogIndex == -1 {
			rf.logs = args.Entries
			confirmed = len(args.Entries)
		} else {
			rf.logs = nil
		}
	} else if !found {
		// fmt.Printf("%d conflict at %d\t", rf.me, args.PrevLogIndex)
		// unfound cuz conflic or lost log
		rf.logs = rf.logs[:i]
	} else {
		// mostly there shouldn't be logs after rf.logs[i]. However, since we have step to send
		// multi logs which means the entris in args may replace the exsist logs. We simply ignore
		// this replacement.
		if args.Entries != nil {
			// fmt.Printf("%d append at %d\t", rf.me, args.PrevLogIndex)
			rf.logs = append(rf.logs[:i+1], args.Entries...)
		}
		confirmed = len(args.Entries)
	}
	*reply = AppendEntriesReply{term, confirmed, rf.me}

	// fresh the commitedIndex
	if args.LeaderCommit > rf.committedIndex && confirmed >= 0 && len(rf.logs) > 0 {
		toCommit := min(args.LeaderCommit, rf.logs[len(rf.logs) - 1].Index)
		for i:=rf.committedIndex+1; i <= toCommit; i++ {
			rf.applyCh <- ApplyMsg{true, rf.logs[i].Command, rf.logs[i].Index+1}
			fmt.Printf("%d commit index %v\n", rf.me, rf.logs[i])
			rf.committedIndex = i
		}
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	var log Log
	if len(rf.logs) > 0 {
		log = rf.logs[len(rf.logs)-1]
	} else {
		log = Log{1, -1, -1}
	}
	rf.mu.Unlock()
	if log.Term > args.LastLogTerm ||
		(log.Term == args.LastLogTerm && log.Index > args.LastLogIndex) {
		return
	}
	term, _, ok := rf.TryVote(args.Term, args.CandidateId, false)
	*reply = RequestVoteReply{term, ok}
	// fmt.Printf("%d: RequestVote %d as leader in term %d result %t\n", rf.me, args.CandidateId, args.Term, ok)
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
		rf.matchIndex[rf.me] = index
		fmt.Printf("[%d start command %v total: %d]\n", rf.me, rf.logs[index], len(rf.logs))
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
	rf.state = Killed
	fmt.Printf("%d killed\n", rf.me)
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
	rf.state = Follower
	rf.applyCh = applyCh
	rf.heartTimerReset = make(chan int)
	// rf.voteTimerReset = make(chan int)
	rf.logs = []Log{}
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.committedIndex = -1
	rf.newIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i:=0; i<len(rf.peers);i++{
		rf.matchIndex[i] = -1
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	fmt.Println("make new raft ", rf.me)

	go func(){
		for nextTerm := rf.currentTerm; true; {
			switch rf.state {
			case Follower:
				nextTerm = rf.doFollower()
			case Leader:
				rf.doLeader(nextTerm)
			case Candidate:
				nextTerm = rf.doCandidate(nextTerm)
			case Killed:
				return
			}
		}
	}()

	return rf
}

func (rf *Raft) doFollower() int {
	for loops:=0; rf.state == Follower; loops++ {
		// it awkword that stop and reset may cause trouble, I have no choice but create a new timer
		rf.heartTimer = time.NewTimer(time.Millisecond * time.Duration(rand.Intn(150) + 150))
		select {
				case <- rf.heartTimer.C:
					// fmt.Printf("%d heartbeat timeout: %v\n", rf.me, rf.state)
					// if timeout, try vote self
					nextTerm := rf.currentTerm + 1
					for ok := false; !ok; nextTerm++ {
						// the only entry transfer to candidate
						nextTerm, _, ok = rf.TryVote(nextTerm, rf.me, false)
					}
					return nextTerm - 1
				case <- rf.heartTimerReset:
					// fmt.Printf("%d heart timer reset\n", rf.me)
					continue
		}
		if loops % 20 == 0 {
			// a raft node won't get persist if it tranfers state frequently
			rf.persist()
		}
	}
	return -1
}

func (rf *Raft) doLeader(term int) bool {
	// transfer to initial state of leader
	rf.mu.Lock()
	for i:=0; i<len(rf.peers); i++ {
		rf.newIndex[i] = len(rf.logs)
	}
	rf.mu.Unlock()
	step := 1
	for loops:=0; rf.state == Leader; loops++ {
		replyC := make(chan AppendEntriesReply, 32)
		go rf.SyncLog(step, replyC)

		rf.heartTimer.Reset(time.Millisecond * 50)
		ForEnd:
		for {
			// handle reply until timeout
			select {
				case <- rf.heartTimer.C:
					break ForEnd
				case reply := <- replyC:
					// the reply must come after sent request.
					// in this period, none go routine would r/w nextNew-Slice
					// so there is no need to require the mutex
					if reply.Success > 0 {
						// fmt.Printf("[%d reply success at %d/%d]", reply.Id, rf.newIndex[reply.Id], len(rf.logs))
						rf.newIndex[reply.Id] = min(rf.newIndex[reply.Id] + step, len(rf.logs))
						rf.matchIndex[reply.Id] = rf.newIndex[reply.Id] - 1
					} else if reply.Success < 0 {
						// fmt.Printf("[%d reply failed at %d]", reply.Id, rf.newIndex[reply.Id])
						rf.newIndex[reply.Id] = max(rf.newIndex[reply.Id] - step, 0)
					}
				case <- rf.heartTimerReset:
					close(replyC)
					return false
			}
		}
		// close the account
		close(replyC)
		rf.mu.Lock()
		// fmt.Printf("\nmin match: [%d - %v, %d]\n", minMatch, rf.matchIndex, rf.committedIndex)
		for i:=rf.committedIndex+1; i <= rf.matchIndex[rf.me]; i++ {
			if counts(rf.matchIndex, func(x int) bool {return x>=i}) * 2 < len(rf.peers) {
				break
			}
			rf.applyCh <- ApplyMsg{true, rf.logs[i].Command, rf.logs[i].Index+1}
			// cuz we could reapply the log, we don't care if raft crash at this point
			rf.committedIndex = i
			fmt.Printf("%d commit index %v for %d/%v\n", rf.me, rf.logs[i], counts(rf.matchIndex, func(x int) bool {return x>=i}), rf.matchIndex)
		}
		rf.mu.Unlock()
		if loops % 20 == 0 {
			rf.persist()
		}
	}
	return true
}

func (rf *Raft) doCandidate(term int) int {
	// fmt.Printf("%d perfrom candidate in term %d \n", rf.me, term)
	// it awkword that stop and reset may cause trouble, I have no choice but create a new timer
	rf.voteTimer = time.NewTimer(time.Millisecond * time.Duration(rand.Intn(150) + 150))
	var lastLogIndex, lastLogTerm int
	if len(rf.logs) > 0 {
		lastLogIndex = rf.logs[len(rf.logs)-1].Index
		lastLogTerm = rf.logs[len(rf.logs)-1].Term
	} else {
		lastLogIndex = -1
		lastLogTerm = -1
	}
	args := RequestVoteArgs{term, rf.me, lastLogIndex, lastLogTerm}
	votec := make(chan *RequestVoteReply, len(rf.peers))
	defer close(votec)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int, votec chan *RequestVoteReply) {
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(i, &args, reply) {
				defer func(){
					if err := recover(); err != nil {
						// fmt.Println("Expired vote", err)
					}
				}() // handle panic
				// cuz the handle of reply is simple enough
				// we handle it synchronously in raft routine to get rid of mutex
				votec <- reply
			}
		}(i, votec)
	}
	for elected := 1; true; {
		select {
			// if election timeout, raise a new election
		case <- rf.voteTimer.C:
			if rf.state != Candidate {
				return -1
			}
			// reelection
			nextTerm := rf.currentTerm + 1
			for ok := false; !ok; nextTerm++{
				nextTerm, _, ok = rf.TryVote(nextTerm, rf.me, false)
			}
			// fmt.Printf("%d election timeout, start nextTerm %d, %d\n", rf.me, nextTerm-1, rf.state)
			return nextTerm - 1
		// case <- rf.voteTimerReset:
		// 	return -1
		case reply, ok := <- votec:
			// exit one lose the election
			if rf.state != Candidate {
				return -1
			}
			if ok && reply.VoteGranted {
				// fmt.Printf("%d get vote in term %d with ok; %v\n", rf.me, term, *reply)
				elected += 1
				if elected > len(rf.peers)/2 {
					rf.mu.Lock()
					// the only entry to be leader
					if rf.state == Candidate && rf.currentTerm == term {
						rf.state = Leader
						fmt.Printf("%d -> leader in term %d\n", rf.me, term)
						rf.mu.Unlock()
						// exit once won the election
						return term
					}
					rf.mu.Unlock()
				}
			} else if ok && !reply.VoteGranted {
				rf.TryVote(reply.Term, -1, false)
			}
		}
	}
	fmt.Println("Shall not come there")
	return -1
}