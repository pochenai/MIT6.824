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
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
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
	HeartBeatInterval = 100 * time.Millisecond
	// The tester generously allows solutions to complete elections in one second
	// (much more than the paper's range of timeouts).
	RaftElectionTimeout = 1000 * time.Millisecond
)

type Role int

const (
	Follower Role = iota + 1
	Candicate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candicate:
		return "Candicate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

type Log struct {
	Term  int
	Index int
	Cmd   interface{}
}

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
	CurrentTerm int
	VotedFor    int // We should only reset votedFor when receiving a RPC req/resp with a higher term, or we might vote more than once in the same term
	role        Role
	lastLive    time.Time
	// for 2B
	Log         []Log
	commitIndex int
	applyCh     chan ApplyMsg
	nextIndex   []int
	matchIndex  []int
	// for 2D
	LastIncludedIndex int // start from -1
	LastIncludedTerm  int
	SnapshotState     []byte
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = rf.role == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Be cautious to use defer rf.persis() in other methods, cause the applyCh is blocking, we might haven't persisted the log, then suddenly the server is killed
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) persistSnapShot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	state := w.Bytes()

	w = new(bytes.Buffer)
	e = labgob.NewEncoder(w)
	e.Encode(rf.LastIncludedTerm)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.SnapshotState)
	snapshot := w.Bytes()

	rf.persister.SaveStateAndSnapshot(state, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(raftstate []byte, snapstate []byte) {
	if raftstate == nil || len(raftstate) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(raftstate)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Log
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		panic("readPersist decode error")
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Log = log
	}

	// snapshot
	rs := bytes.NewBuffer(snapstate)
	ds := labgob.NewDecoder(rs)
	var lastIncludedTerm int
	var lastIncludedIndex int
	var snapshot []byte
	if ds.Decode(&lastIncludedTerm) == nil && ds.Decode(&lastIncludedIndex) == nil && ds.Decode(&snapshot) == nil {
		rf.LastIncludedTerm = lastIncludedTerm
		rf.LastIncludedIndex = lastIncludedIndex
		rf.SnapshotState = snapshot
		// commit after snapshot will use commitIndex
		rf.commitIndex = lastIncludedIndex
	} else {
		DPrintf("snapshot doesn't exist or decoded failure\n")
	}

}

type RequestInstallSnapshot struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendInstallSnapShot(server int, args *RequestInstallSnapshot, reply *InstallSnapshotReply) {
	ok := rf.peers[server].Call("Raft.RequestInstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok && reply.Term > rf.CurrentTerm {
		rf.CurrentTerm = reply.Term
		rf.role = Follower
		rf.VotedFor = -1

		rf.persist()
	}

	// Leader can only get this response after the follower successfully processes condInstallSnapshot. So, we don't need to worry that RequestInstallSnapshot is successfully, but condInstallSnapshot failed.
	// But condInstallSnapshot is called by another go routine and maybe called between another appendEntries RPC, so we need to make sure the condInstallSnapshot is Idempotent to handle old condInstallSnapshot RPCs.
	if reply.Success && rf.role == Leader {
		DPrintf("leader:%v, got applySnapshot success from peer:%v, lastIncludedIndex: %v, lastIncludedTerm: %v", rf.me, server, rf.LastIncludedIndex, rf.LastIncludedTerm)

		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
	}
}

func (rf *Raft) RequestInstallSnapshot(args *RequestInstallSnapshot, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("leader:%v, RequestInstallSnapshot to peer: %v, lastIncludedIndex:%v, lastIncludedTerm:%v, myLastIncIdx:%v, myLastIncTerm:%v", args.LeaderID, rf.me, args.LastIncludedIndex, args.LastIncludedTerm, rf.LastIncludedIndex, rf.LastIncludedTerm)
	reply.Term = rf.CurrentTerm
	reply.Success = false

	// If we are the leader, we should reject old RPC received when we were the follower.
	if args.Term < rf.CurrentTerm || args.LastIncludedTerm < rf.LastIncludedTerm || args.LastIncludedIndex < rf.commitIndex {
		return
	}

	// Allservers: transition to follower if term >= currentTerm
	rf.CurrentTerm = args.Term
	rf.role = Follower
	rf.lastLive = time.Now()
	// ensure only vote once for each term
	if args.Term > rf.CurrentTerm {
		rf.VotedFor = -1
	}

	// Only deal with this RPC when we're the follower or candicate, otherwise ignore it. Cause we might receive old RPC request.
	// This msg will insert a log at cmdIndex to ensure checklog pass (the previous log is the same with other servers) when applying the next entry in the future.
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: logIndexToApplyChIndex(args.LastIncludedIndex),
	}
	DPrintf("peer %v, applySnapshot success, lastIncludedIndex: %v, lastIncludedTerm: %v", rf.me, rf.LastIncludedIndex, rf.LastIncludedTerm)

	// update commitIndex immediately rather than set it later in CondInstallSnapshot for requestAppendEntries to drop old RPCs.
	rf.commitIndex = args.LastIncludedIndex
	reply.Success = true
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedCmdIndex int, snapshot []byte) bool {

	// Your code here (2D).
	// Mustn't use lock, cause this function is called during RequestInstallSnapshot by another goroutine in config.go/applierSnap,
	// which will cause deadlock.
	// 	- leader requestInstallSnapshot to a follower (holding lock rf.mu)
	//		- follower applySnapshot msg
	// 			- tester's applierSnap call CondInstallSnapshot
	//				- acquired cfg.mu in tester
	// 				- CondInstallSnapshot, try to acquire lock until it acuqired(deadlock rf.mu)
	// - leader receive requestInstallSnapshot success and applyCommit
	// 		- leader in tester's applierSnap try to acquire cfg.mu until it acquired(deadlock cfg.mu)
	// - Another follower received requestInstallSnapshot and applyCommit
	// 		- This follower in tester's applierSnap try to acquire cfg.mu until it acquired(deadlock cfg.mu)
	// Finally, all raft peers are blocked!

	// If no lock: this race condition can happen:
	// - CondInstallSnapshot: logIndex 58, rf.lastIncludedLogIndex = 48
	// 		- trim log to rf.Log = []
	//		- haven't update LastIncludedIndex
	// - RequestAppendEntries:, prevLogIndex 50, entries 11, rf.lastIncludedLogIndex = 48, leaderCommit=48
	// 		- appendLogs rf.Log=[51,52,53..61]
	// - update rf.LastIncludedIndex = 58 in CondInstallSnapshot
	// - RequestAppendEntries: prevLogIndex: 50, leaderCommit: 60, rf.LastIncludedIndex = 58
	//	 	- We'll commit logIndex 51 (real) with false index: 59
	// Solution: set commitIndex in RequestInstallSnapshot, before CondInstallSnapshot. So, old requestAppendEntriesRPC would be rejected.
	DPrintf("server %v, condInstall snapshot start, lastIncludedIndex: %v, lastIncludedTerm: %v", rf.me, rf.LastIncludedIndex, rf.LastIncludedTerm)

	lastIncludedLogIndex := cmdIndexToLogIndex(lastIncludedCmdIndex)

	// We might receive old RPC(when peer is follower), if we don't check if we're the leader. It'll trim all logs, but the nextIndex is larger than rf.LastIncludedIndex. So, it'll panic out of range for rf.log when we're sending new heartbeats.
	if rf.role == Leader || rf.LastIncludedTerm > lastIncludedTerm || rf.commitIndex > lastIncludedLogIndex {
		return false
	}

	rf.SnapshotState = snapshot
	// Better to trim its log rather than set it to nil, because we might receive old RPC request. If we set it to nil, then the len(rf.log) could be less than nextIndex[server]
	logIndex := max(min(lastIncludedLogIndex-rf.LastIncludedIndex, len(rf.Log)), 0)
	rf.Log = rf.Log[logIndex:] // min is len(rf.Log) not len(rf.Log) -1 !, cause we'll trim all the log when lastIncludedLogIndex is larger; else we'll leave some log. Cause requestAppendEntries can check the conflicting log.
	rf.LastIncludedIndex = lastIncludedLogIndex
	rf.LastIncludedTerm = lastIncludedTerm
	DPrintf("server %v, condInstall snapshot success, lastIncludedIndex: %v, lastIncludedTerm: %v", rf.me, rf.LastIncludedIndex, rf.LastIncludedTerm)
	rf.persistSnapShot()

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// Because it'll be called during appling msgs, so the function called the applyCh has acquired the lock, so we mustn't use lock here, or deadlock will happen.
	// So, it also means trace conditions might happen：
	//  - setup: rf.Log = [49, 50], rf.lastIncludedIndex=48
	//  	case 1. Snapshot first: set lastIncludedIndex: 58 (lastCommitIndex >=lastIncludedIndex due to applyCh mechanism)=> appendEntries: prevLogIndex=50 < lastCommitIndex, appendEntries [51,52,53] (this RPC will be rejected cause)=> Snapshot trim Log, nothing bad happens.
	//  	case 2. Snapshot first: set lastIncludedIndex: 58 => appendEntries: prevLogIndex=58=prevLogIndex, appendEntries [59,60,61], leaderCommit=61  => Snapshot trim Log to [59:] => appendEntries, set commitIndex 59,60,61; nothing bad happens (shouldn't set rf.commitIndex here).
	//  	case 3.appendEntries first: prevLogIndex=50, entries [51,52,53], leaderCommit 55, rf.lastIncludedIndex=48
	// 			=> case 3.1 another RPC trigger Snapshot: set lastIncludedIndex: 58 => nothing bad happens, because all works has been done.
	// 			=> case 3.2 appendEntries: commit 51 with 51 => Snapshot triggerd by another AppendRPC (when it's the last msg): set lastIncludedIndex: 58, trim rf.Log => appendEntries panics duo to trimed log out of range.
	// 				=> solution: reject appendEntries RPC's whose prevLog index is less than rf.commitIndex (because this must has been set by another RPC, because it acquired the lock before).
	DPrintf("server %v, snapshot index: %v, before,lastIncludedIndex: %v, lastIncludedTerm: %v", rf.me, index, rf.LastIncludedIndex, rf.LastIncludedTerm)

	// index is the lastCmd logIndex+1, so we have decrease it by 1 first
	lastIncludedLogIndex := cmdIndexToLogIndex(index)
	logIndex := lastIncludedLogIndex - rf.LastIncludedIndex - 1
	if logIndex > len(rf.Log)-1 {
		panic(fmt.Sprintf("snapshot index out of range: logIndex:%v, lenLog-1:%v", logIndex, len(rf.Log)-1))
	}
	log := rf.Log[logIndex]
	rf.LastIncludedTerm = log.Term
	rf.LastIncludedIndex = lastIncludedLogIndex
	rf.Log = rf.Log[logIndex+1:]
	rf.SnapshotState = snapshot
	DPrintf("server %v, snapshot index: %v, after,lastIncludedIndex: %v, lastIncludedTerm: %v", rf.me, index, rf.LastIncludedIndex, rf.LastIncludedTerm)

	rf.persistSnapShot()
}

type RequestAppendEntry struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

func (r RequestAppendEntry) String() string {
	return fmt.Sprintf("term %v, leaderId:%v, prevLogIndex:%v, prevLogTerm: %v, commit: %v, entries: %v", r.Term, r.LeaderId, r.PrevLogIndex, r.PrevLogTerm, r.LeaderCommit, len(r.Entries))
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// Quick append log
	Xterm int // the conflicting term
	// the conflicting index
	// If Xterm = -2, it means timeout or rejected, we just return and don't deal with it
	// if it's Xterm is -1, XIndex is the nextIndex to start append;
	// else: XIndex is the conflicting index
	Xindex int
}

func (rf *Raft) sendAppendEntries(server int, args *RequestAppendEntry, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	// DPrintf("leader %v, RequestAppendEntries reply from server: %v, success: %v", rf.me, server, reply.Success)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && reply.Term > rf.CurrentTerm {
		rf.CurrentTerm = reply.Term
		rf.role = Follower
		rf.VotedFor = -1

		rf.persist()
	}

	if rf.role != Leader || !ok {
		// DPrintf("reply timeout, from peer: %v, Xterm: %v, Xindex: %v", server, reply.Xterm, reply.Xindex)
		return
	}

	if reply.Success {
		// update nextIndex and matchIndex
		nextIndex := args.PrevLogIndex + 1 + len(args.Entries)
		if nextIndex < rf.nextIndex[server] { // drop old RPCs to avoid nextIndex roll back
			DPrintf("leader:%v, drop old reply success from peer: %v, prevNextIndex: %v, newNextIndex(old): %v", rf.me, server, rf.nextIndex[server], nextIndex)
			return
		}
		rf.nextIndex[server] = nextIndex
		rf.matchIndex[server] = nextIndex - 1
		DPrintf("leader:%v, got reply success from peer: %v, prevLogIndex: %v, newLogIndex: %v, entries: %v", rf.me, server, args.PrevLogIndex, rf.nextIndex[server], len(args.Entries))
		DPrintf("leader:%v,check commitIndex for peer: %v, commitIndex: %v, matchIndex: %v", rf.me, server, rf.commitIndex, rf.matchIndex)

		// update commitIndex
		for i := len(rf.Log) + rf.LastIncludedIndex; i > rf.commitIndex; i-- {
			relativeIndex := i - rf.LastIncludedIndex - 1
			if rf.Log[relativeIndex].Term == rf.CurrentTerm {
				count := 0
				for j := range rf.peers {
					if j == rf.me || rf.matchIndex[j] >= i {
						count += 1
					}
				}
				if count > len(rf.peers)/2 {
					i := i

					msgs := []ApplyMsg{}
					for commitedInd := rf.commitIndex + 1; commitedInd <= i; commitedInd++ {
						cmd := rf.Log[commitedInd-rf.LastIncludedIndex-1].Cmd
						commitedInd := logIndexToApplyChIndex(commitedInd)
						DPrintf("leader %v, applyCh: %v", rf.me, commitedInd)
						msgs = append(msgs, ApplyMsg{
							CommandValid: true,
							Command:      cmd,
							CommandIndex: commitedInd,
						})
					}

					// Add an invalid command to ensure Snapshot must has been applied before the next AppendEntries RPC.
					msgs = append(msgs, ApplyMsg{
						CommandValid: false,
						Command:      nil,
						CommandIndex: -2,
					})
					// send ApplyMsg to applyCh for tester
					// copy msgs first, then apply msgs to applyCh. Cause we might trigger snapshoting and trim the log when we're applying the msgs.
					// must process it during lock to avoid out of order apply by another RPC.
					for _, msg := range msgs {
						rf.applyCh <- msg
					}

					rf.commitIndex = i
					DPrintf("leader %v, set commit index: %v", rf.me, logIndexToApplyChIndex(i))
					// Allservers: update lastApplied and apply log[lastApplied] to state machine
					// todo
					break
				}
			}
		}
		DPrintf("check commitIndex done, leader:%v, matchIndex: %v", rf.me, rf.matchIndex)
	} else if reply.Xterm != -2 {
		DPrintf("reply mismatch, from peer: %v, Xterm: %v, Xindex: %v, prevIdx:%v, prevTerm:%v, myLastIncIdx:%v", server, reply.Xterm, reply.Xindex, args.PrevLogIndex, args.PrevLogTerm, rf.LastIncludedIndex)
		// Although, ok is true and reply.Success is false, we still have to check Xterm!=-2, cause RPC might early return our response before the reply is fully set in requestAppendEntries.
		// If AppendEntries fails because of log inconsistency, do quick recovery.
		// Must be ok, or it'll reset nextIndex if timeout due to the target peer is disconnected.
		if reply.Xterm == -1 {
			// skip to follower's index
			if rf.nextIndex[server] < reply.Xindex { // drop late rpc, failed requestAppendEntries RPC will only decrease the nextIndex, so we can safely ignore it
				return
			}
			rf.nextIndex[server] = reply.Xindex
			fmt.Println("peer:", server, ",skip to addIndex:", rf.nextIndex[server])
		} else { // conflicting, args.PrevLogIndex == reply.Xindex not always true.
			// TODO:Merge this case with reply.Xterm == -1
			if rf.nextIndex[server] < reply.Xindex { // drop late rpc
				return
			}
			if reply.Xindex <= rf.LastIncludedIndex { // conflicting with snapshot index;
				rf.nextIndex[server] = max(rf.LastIncludedIndex, 0) // if lastIncludedIndex is -1, then nextIndex should be 0
				fmt.Println("peer:", server, ",skip to LastIncludedIndex:", rf.nextIndex[server])
				return
			}
			// skip to leader's last index of previous term before snapshot index
			for i := min(args.PrevLogIndex, reply.Xindex); i >= rf.LastIncludedIndex; i-- {
				if i == rf.LastIncludedIndex || rf.Log[i-rf.LastIncludedIndex-1].Term < min(reply.Xterm, args.PrevLogTerm) {
					term := -1
					if i > rf.LastIncludedIndex {
						term = rf.Log[i-rf.LastIncludedIndex-1].Term
					}
					fmt.Println("peer:", server, ",skip to prevLogIndex:", i, "term:", term)
					rf.nextIndex[server] = i + 1
					return
				}
			}
		}
	} else {
		DPrintf("reply false, from peer: %v, Xterm: %v, Xindex: %v", server, reply.Xterm, reply.Xindex)
	}
	// else, timeout shoule be retried in sendHeartBeats, but don't retry here cause leader might has changed.
}

func logIndexToApplyChIndex(index int) int {
	// ApplyMsg.CommandIndex starts from 0, while here CommandIndex is initialized with -1, so we need to add 1
	return index + 1
}

func cmdIndexToLogIndex(index int) int {
	// ApplyMsg.CommandIndex is larger than cmdIndex by 1, so we need to minus 1
	return index - 1
}

func (rf *Raft) RequestAppendEntries(args *RequestAppendEntry, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("leader:%v, RequestAppendEntries to peer: %v,prevIndex:%v,prevTerm:%v, prevCommit:%v,leaderTerm:%v,entries:%v, under mycommitIndex: %v, myterm: %v, myIncIdx:%v", args.LeaderId, rf.me, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Term, len(args.Entries), rf.commitIndex, rf.CurrentTerm, rf.LastIncludedIndex)

	reply.Success = false
	reply.Term = rf.CurrentTerm
	reply.Xindex = -1
	reply.Xterm = -2

	// cond:1， args.PrevLogIndex < rf.commitIndex could happend when no snapshot. In snapshot case, we might receive old RPC. We can't use args.PrevLogIndex < rf.commitIndex to check, because in no snapshot case, we might rollback to a too old log to quickly sync the peer's log with leader.
	//  To avoid this race condition:
	// 			appendEnriesRPC A -> commit during A with index 58 => appendRPC B with LastIncludedIndex=50 => Snapshot called trigged by RPC A => commit during RPC B (LastIncludedIndex changed to 58)
	// We must ensure snapshot is done before the next AppendEntries RPC, so we add an invalid command to applyCh.
	if args.Term < rf.CurrentTerm || args.PrevLogIndex < rf.LastIncludedIndex {
		return
	}

	// Allservers: transition to follower if term >= currentTerm
	rf.CurrentTerm = args.Term
	rf.role = Follower

	rf.lastLive = time.Now()
	// ensure only vote once for each term
	if args.Term > rf.CurrentTerm {
		rf.VotedFor = -1
	}

	// cond:2
	if args.PrevLogIndex > -1 && args.PrevLogIndex != rf.LastIncludedIndex { // snapShoted lastIncludedIndex must be equal, because lastIncludedIndex can only be committed by the majority
		// check if prevLogIndex is in the log, not exist
		if args.PrevLogIndex > len(rf.Log)+rf.LastIncludedIndex {
			reply.Xterm = -1
			if len(rf.Log) > 0 {
				reply.Xterm = rf.Log[len(rf.Log)-1].Term // the last term in the log
			}
			reply.Xindex = len(rf.Log) + rf.LastIncludedIndex + 1

			rf.persist()
			return
		}
		// prevLogIndex > rf.LastIncludedIndex has been ensured before in cond:1;
		// prevLogIndex != rf.LastIncludedIndex has been ensured before in cond:2
		relativeLogIndex := args.PrevLogIndex - rf.LastIncludedIndex - 1
		if relativeLogIndex >= 0 && rf.Log[relativeLogIndex].Term != args.PrevLogTerm {
			// log mismatch, return false
			reply.Xterm = rf.Log[relativeLogIndex].Term
			reply.Xindex = args.PrevLogIndex // should be absolute index rather than relative index

			rf.persist()
			return
		}
	}

	// cond:3&4
	for i, entry := range args.Entries {
		relativeLogIndex := args.PrevLogIndex + i - rf.LastIncludedIndex
		var existing *Log
		if len(rf.Log) > 0 && relativeLogIndex >= 0 && len(rf.Log) > relativeLogIndex {
			existing = &rf.Log[relativeLogIndex]
		}
		if existing != nil {
			if existing.Term != entry.Term || existing.Cmd != entry.Cmd { // same index but different term or different command
				// cond:3 Delete the existing entry and all that follow it
				rf.Log = rf.Log[:relativeLogIndex]
				rf.Log = append(rf.Log, entry)
			}
			continue
		} else { // existing must be nil before appending to avoid applying the same entry twice, which happens when leader sends heartbeat
			// cond:4 Append any new entries not already in the log
			rf.Log = append(rf.Log, entry)
		}
	}
	rf.persist() // log changes in the following

	// cond:5
	if args.LeaderCommit > rf.commitIndex {
		// the right one shouldn't be len(rf.log) -1, because the old leader might has append new logs longer, so old leader (current follower) log lenght might be longer than new leader's log length. If we use len(rf.log)-1, then follower might commit some logs not belong to the new leader.
		commitIndex := min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		// Send ApplyMsg for each new committed log entry for tester
		msgs := []ApplyMsg{}
		// Must start from commitIndex, or we'll commit the same index multiple times.
		for i := rf.commitIndex + 1; i <= commitIndex; i++ {
			relativeIndex := i - rf.LastIncludedIndex - 1
			cmd := rf.Log[relativeIndex].Cmd
			commitedInd := logIndexToApplyChIndex(i)
			// copy msgs first, then apply msgs to applyCh. Cause we might trigger snapshoting and trim the log when we're applying the msgs.
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command:      cmd,
				CommandIndex: commitedInd,
			})
		}

		// Snapshot is async and we can't add lock to Snapshot func, because it'll cause deadlock when applyCh hasn't done. So, we add an invalid command to ensure Snapshot must has been applied before the next AppendEntries RPC.
		msgs = append(msgs, ApplyMsg{
			CommandValid: false,
			Command:      nil,
			CommandIndex: -2,
		})

		// must block to avoid out of order apply
		for _, msg := range msgs {
			DPrintf("follower:%v, apply msg, applyCmd: %v, cmdIndex: %v, valid:%v", rf.me, msg.Command, msg.CommandIndex, msg.CommandValid)
			rf.applyCh <- msg
		}

		DPrintf("follower:%v,  set commit index: %v", rf.me, logIndexToApplyChIndex(commitIndex))
		rf.commitIndex = commitIndex
	}
	// TODO: Allservers update lastApplied and apply log[lastApplied] to state machine
	DPrintf("leader:%v, RequestAppendEntries success in peer: %v,prevIndex:%v,prevTerm:%v, prevCommit:%v,leaderTerm:%v,entries:%v,under mycommitIndex: %v, myterm: %v, myLog: %v", args.LeaderId, rf.me, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Term, len(args.Entries), rf.commitIndex, rf.CurrentTerm, len(rf.Log))
	reply.Success = true
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogTerm  int
	LastLogIndex int
}

func (r RequestVoteArgs) String() string {
	return fmt.Sprintf("term:%v,id:%v,lastLogTerm:%v,lastLogIndex:%v", r.Term, r.CandidateId, r.LastLogTerm, r.LastLogIndex)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastTerm, lastIndex := rf.LastLog()
	DPrintf("server %v, get requestVote: %v, curTerm:%v, lastTerm:%v, lastIndex:%v,role:%v,votedFor:%v", rf.me, args, rf.CurrentTerm, lastTerm, lastIndex, rf.role, rf.VotedFor)
	reply.VoteGranted = false

	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		return
	}

	defer rf.persist()

	// transition to follower, reset currentTerm and votedFor. 缺一不可!
	// currentTerm must be updated to ensure one peer is only allowed to vote once for each term
	if args.Term > rf.CurrentTerm {
		rf.role = Follower
		// make sure kickoff next election use latest term
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
	}

	// lastLive should only be updated after granting vote, or the current server will issue leader election again
	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		// candidate's log must be at least update to date with the majority.
		// Should be lastlog's term rather than rf.currentTerm!

		lastTerm, lastIndex := rf.LastLog()
		if args.LastLogTerm > lastTerm {
			reply.VoteGranted = true
			rf.VotedFor = args.CandidateId
			rf.lastLive = time.Now()
			return
		}
		if args.LastLogTerm == lastTerm {
			if args.LastLogIndex >= lastIndex {
				reply.VoteGranted = true
				rf.VotedFor = args.CandidateId
				rf.lastLive = time.Now()
			}
		}
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
	DPrintf("send requestVote from: %v, to :%v, term: %v", rf.me, server, rf.CurrentTerm)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.CurrentTerm
	index = len(rf.Log) + rf.LastIncludedIndex + 1
	isLeader = rf.role == Leader

	if !isLeader {
		return rf.commitIndex, term, isLeader
	}

	defer rf.persist()

	newLog := Log{
		Term:  rf.CurrentTerm,
		Index: index,
		Cmd:   command,
	}

	DPrintf("leader:%v append log:%v", rf.me, newLog)
	// update log
	rf.Log = append(rf.Log, newLog)

	// send new entries to other peers immdiately so that even if leader request appendEntries rpc timeout, follower will have a chance to receive the new cmd.
	rf._sendHeartBeats()
	return logIndexToApplyChIndex(index), term, isLeader
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

func (rf *Raft) sendHeartBeats() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf._sendHeartBeats()
}

// not threadsafe, must use it under lock
func (rf *Raft) _sendHeartBeats() {
	if rf.role != Leader {
		return
	}
	rf.lastLive = time.Now()
	// also update the server's nextIndex for debugging purposes
	rf.matchIndex[rf.me] = len(rf.Log) + rf.LastIncludedIndex

	// send heartbeat
	emptyEntry := RequestAppendEntry{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		// use goroutine for each rpc to avoid blocking
		server := server
		var entries []Log
		nextIndex := rf.nextIndex[server]
		// 1. no snapshot, nextIndex =0 > lastIncludedIndex = -1, so rf.sendInstallSnapShot will not execute;
		// 2. snapshot enabled, nextIndex will be rf.lastIncludedIndex, so it will execute rf.sendInstallSnapShot.
		if nextIndex <= rf.LastIncludedIndex {
			go func() {
				reply := InstallSnapshotReply{}
				arg := RequestInstallSnapshot{
					Term:              rf.CurrentTerm,
					LeaderID:          rf.me,
					LastIncludedIndex: rf.LastIncludedIndex,
					LastIncludedTerm:  rf.LastIncludedTerm,
					Data:              rf.SnapshotState,
				}
				rf.sendInstallSnapShot(server, &arg, &reply)
			}()
			continue
		}
		// else: nextIndex > rf.lastIncludedIndex, sendAppendEntries
		if len(rf.Log)+rf.LastIncludedIndex > nextIndex-1 {
			entries = rf.Log[nextIndex-rf.LastIncludedIndex-1:]
		}
		prevLogIndex := nextIndex - 1
		prevLogTerm := rf.LastIncludedTerm
		if prevLogIndex > rf.LastIncludedIndex {
			prevLogTerm = rf.Log[prevLogIndex-rf.LastIncludedIndex-1].Term
		}
		go func() {
			reply := AppendEntriesReply{}
			entry := emptyEntry
			entry.Entries = entries
			entry.PrevLogIndex = prevLogIndex
			entry.PrevLogTerm = prevLogTerm

			rf.sendAppendEntries(server, &entry, &reply)
		}()
	}
}

// Mustn't use seed or it'll generate the same random number every time
func RandElectionTimeout() time.Duration {
	randVal := rand.Float32()*2.5 + 2.5 // use a larger timeouts, cause timeout is more frequent in lab2C, 2D
	duration := int64(float32(HeartBeatInterval) * randVal)
	return time.Duration(duration)
}

func (rf *Raft) LastLog() (int, int) {
	lastTerm := -1
	lastIndex := -1
	if len(rf.Log) > 0 {
		lastTerm = rf.Log[len(rf.Log)-1].Term
		lastIndex = len(rf.Log) + rf.LastIncludedIndex
	} else {
		lastTerm = rf.LastIncludedTerm
		lastIndex = rf.LastIncludedIndex
	}
	return lastTerm, lastIndex
}

func (rf *Raft) kickOffLeaderElection(electionTimeout time.Duration) {
	rf.mu.Lock()
	rf.role = Candicate
	rf.CurrentTerm += 1
	rf.VotedFor = rf.me
	rf.lastLive = time.Now()
	term := rf.CurrentTerm

	rf.persist()

	rf.mu.Unlock()
	start := time.Now()

	go func() {
		// send Rpc request to other servers
		var mu sync.Mutex
		// cond share the same lock with mu cause they control the same shared variables
		cond := sync.NewCond(&mu)
		totalVotes := 1
		finished := 1

		lastTerm, lastIndex := rf.LastLog()
		requestVotes := RequestVoteArgs{
			Term:         term,
			CandidateId:  rf.me,
			LastLogTerm:  lastTerm,
			LastLogIndex: lastIndex,
		}

		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			go func(server int) {
				var reply RequestVoteReply
				ok := rf.sendRequestVote(server, &requestVotes, &reply)
				mu.Lock()
				defer mu.Unlock()
				if ok && reply.VoteGranted {
					DPrintf("server %v got vote from %v, reply: %v", rf.me, server, reply)
					totalVotes += 1
				}
				finished += 1
				cond.Broadcast()
			}(server)
		}

		cond.L.Lock()
		for totalVotes < (len(rf.peers)/2+1) && finished != len(rf.peers) {
			DPrintf("candidate: %v, total votes: %v, finished: %v", rf.me, totalVotes, finished)
			cond.Wait()
		}

		if totalVotes > len(rf.peers)/2 {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.role == Candicate && rf.CurrentTerm == term && time.Since(start) < electionTimeout {
				DPrintf("peer: %v, selected as leader", rf.me)
				rf.role = Leader
				// shouldn't reset votedFor=-1 here, cause we don't allow for voting for other candidates in the same term.
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))

				for i := range rf.nextIndex {
					// initialize to leader last log index + 1
					rf.nextIndex[i] = len(rf.Log) + rf.LastIncludedIndex + 1
				}
				for i := range rf.nextIndex {
					// initialize to -1
					rf.matchIndex[i] = -1
				}

				// commit a new empty log entry to avoid liveness issue, when the last log's term isn't the current term, then according to raft's commit role, leader only commit the log entry with the same term as current term, then leader will never commit
				// It' better to be done after setting nextIndex, to send the newLog to other peers immediately.
				// The above comment is not neccessary in lab3b, because there is retry and lab2b's index will be wrong. But in real-world, it's better to do so.
				// rf.Log = append(rf.Log, Log{
				// 	Term:  rf.CurrentTerm,
				// 	Index: len(rf.Log),
				// 	Cmd:   -9999, // empty log entry
				// })

				rf.persist()

				rf._sendHeartBeats()
			}

		}
		cond.L.Unlock()

	}()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// Leader sendHeartBeats ticking
	go func() {
		for !rf.killed() {
			go rf.sendHeartBeats()
			time.Sleep(HeartBeatInterval)
		}
	}()

	// Follower starts leader election periodically if leader is stale
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		electionTimeout := RandElectionTimeout()
		start := time.Now()
		time.Sleep(electionTimeout)
		// start new election
		rf.mu.Lock()
		if rf.lastLive.Before(start) && rf.role != Leader {
			DPrintf("ticking, server: %v, role: %v, term: %v, start election", rf.me, rf.role, rf.CurrentTerm)
			go rf.kickOffLeaderElection(electionTimeout)

		}
		rf.mu.Unlock()
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

	// Your initialization code here (2A, 2B, 2C).
	// 2A
	rf.VotedFor = -1
	rf.role = Follower
	rf.lastLive = time.Now()
	rf.CurrentTerm = 0
	rf.commitIndex = -1

	// 2B
	rf.applyCh = applyCh

	// 2C
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// 2D
	rf.LastIncludedIndex = -1
	rf.LastIncludedTerm = -1

	// Initialize from state persisted before a crash. It shoulde be initialized before setting nextIndex.
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	for i := range rf.nextIndex {
		// initialize to leader last log index + 1
		rf.nextIndex[i] = len(rf.Log) + rf.LastIncludedIndex + 1
	}
	for i := range rf.nextIndex {
		// initialize to -1
		rf.matchIndex[i] = -1
	}

	DPrintf("follower: %v, Candicate: %v, leader:%v", Follower, Candicate, Leader)
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
