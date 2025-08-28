package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	done      chan int
	state     map[string]string
	msgSeq    map[int64]int64 // map from clientId to max requestId
	pendingCh map[int]chan Result
	persister *raft.Persister
}

func (kv *KVServer) applyCmds() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if !msg.CommandValid && !msg.SnapshotValid { // Must skip this due to invalid command used to ensure snapshot is processed during raft RPC.
				continue
			}

			DPrintf("peer %v receive applyCh: %v, isSnapshot:%v", kv.me, msg.CommandIndex, msg.SnapshotValid)
			kv.mu.Lock() // lock first to avoid concurrent map read/write and ensure lineariability
			if msg.SnapshotValid {
				if len(msg.Snapshot) < 1 {
					panic("snapshot is nil")
				}
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.restoreState(msg.Snapshot) // write kv.msgSeq, so it also should be locked
				}
				kv.mu.Unlock()
				continue
			}

			var result Result
			op := msg.Command.(ReqArg)
			// leader and follower both have state, don't forget to set follower's state
			if op.Seq > kv.msgSeq[op.Cid] {
				kv.msgSeq[op.Cid] = op.Seq
				switch op.Op {
				case OpPut:
					kv.state[op.Key] = op.Value
				case OpAppend:
					kv.state[op.Key] += op.Value
				}
			}

			// To avoid concurrent map read/write, we set result in lock.
			result.Value = kv.state[op.Key]
			result.Cid = op.Cid
			result.Seq = op.Seq
			kv.mu.Unlock()

			// save state snapshot
			s := kv.persister.RaftStateSize()
			if s > kv.maxraftstate && kv.maxraftstate != -1 {
				DPrintf("peer:%d, start persisting snapshot, size:%d, cmdInd:%d", kv.me, s, msg.CommandIndex)
				kv.rf.Snapshot(msg.CommandIndex, kv.snapshot())
			}

			DPrintf("peer:%d, start sending applyCh, cmdInd:%d, isLeader:%v", kv.me, msg.CommandIndex, msg.LeaderId == kv.me)
			kv.mu.RLock()
			// use msg.LeaderId == kv.me (or can use lastTerm + isleader to check if term has changed) to check if is leader rather than set kv.isLeader than check use kv.isLeader. Or if the leader is stale, it might not realized it's not leader anymore, so there will be many unhandled ch, which cause memory leaks.
			if msg.LeaderId == kv.me { // Be aware server and follower use the same code, so check exists to make sure only leader has this channel
				// Use lock to ensure pendingCh is created before applyCh or server would get stuck in <-kv.pendingCh[cmdIndex] during Get/PutAppend
				ch, exists := kv.pendingCh[msg.CommandIndex]
				// we only send result if there is already a channel, or we don't send because there might be no handlers (due to new leader commit old leader's msg) for this applyCh , which might cause memory leak or deadlock.s
				if exists {
					DPrintf("leader %v sending ch", kv.me)
					ch <- result
				}
			}
			kv.mu.RUnlock()

			// Or could use goroutine to avoid deadlock, but this way may face linearizability error(because this applyCh might be send to other Request handler):
			// case 1:kv.lock wait rf.lock
			// 			- PutAppendCmd => kv.lock wait rf.lock
			// 			- then applyCh came (it has acquired rf.lock) and wait kv.lock, but kv.lock is held by PutAppendCmd
			// 			- So deadlock happens.
			// case 2: rf.GetState trys to get lock during applyCh which still holds the lock Request handler
		case <-kv.done: // add this case to avoid memory leak, when kv is killed but the go routine is stucked at msg := <-kv.applyCh and can't return
			return
		}

	}
}

func (kv *KVServer) Request(args *ReqArg, reply *Reply) {
	// Your code here.
	kv.mu.RLock()
	// For lineariazability, we don't allow old PUT/Append execute twice. Get is ok.
	// Acturally some seq less than this might not be handled.
	if args.Seq <= kv.msgSeq[args.Cid] {
		reply.Value = kv.state[args.Key]
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	cmdIndex, term, isleader := kv.rf.Start(*args)
	reply.Term = term
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("leader:%v start apply cmd:%v", kv.me, cmdIndex)
	kv.mu.Lock()
	var ch chan Result
	// Use lock to ensure pendingCh is created first before applyCh or server might get stuck in select <-ch during Get/PutAppend
	if _, exists := kv.pendingCh[cmdIndex]; !exists {
		kv.pendingCh[cmdIndex] = make(chan Result)
	}
	ch = kv.pendingCh[cmdIndex]
	kv.mu.Unlock()
	DPrintf("leader:%v waiting for cmd:%v", kv.me, cmdIndex)

	select {
	case result := <-ch:
		DPrintf("leader %v start reply", kv.me)
		// Check this first, because the result might should send to other client's request which share the same cmdIndex.
		// If so, the leader certainly has changed,we should let the client retry.
		if result.Cid != args.Cid || result.Seq != args.Seq {
			reply.Err = ErrWrongLeader
		} else {
			reply.Value = result.Value
		}
		DPrintf("leader %v reply done", kv.me)
	case <-time.After(raft.RaftElectionTimeout):
		DPrintf("leader %v timeout retry", kv.me)
		reply.Err = ErrRetry
	}

	kv.mu.Lock()
	// This opearation is idempotent, we don't need to worry about it like close(ch).
	// We put it in channel to make it faster and don't block the reply.
	delete(kv.pendingCh, cmdIndex)
	// We must first check map exists then close(ch) if use go routine, to avoid async close/sendto closed channel in case timeouts+leader change cause multiple handlers came on the same cmdIndex.
	close(ch)
	kv.mu.Unlock()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.done)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) snapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.state)
	e.Encode(kv.msgSeq)
	return w.Bytes()
}

func (kv *KVServer) restoreState(snapshot []byte) {
	if len(snapshot) < 1 {
		kv.state = make(map[string]string)
		kv.msgSeq = make(map[int64]int64)
		return
	}
	rs := bytes.NewBuffer(snapshot)
	ds := labgob.NewDecoder(rs)
	if ds.Decode(&kv.state) != nil || ds.Decode(&kv.msgSeq) != nil {
		panic("Restore from snapshot failed")
	}
	DPrintf("Restored state from snapshot, state len:%d", len(snapshot))
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(ReqArg{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.restoreState(persister.ReadSnapshot())
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.pendingCh = map[int]chan Result{}
	kv.done = make(chan int)

	go kv.applyCmds()
	return kv
}
