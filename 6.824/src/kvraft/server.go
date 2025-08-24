package kvraft

import (
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
	state     map[string]string
	msgSeq    map[int64]int64 // map from command index to sequence
	pendingCh map[int]chan int
}

func (kv *KVServer) applyCmds() {
	for msg := range kv.applyCh {
		if !msg.CommandValid { // Must skip this due to invalid command used to ensure snapshot is processed during raft RPC.
			continue
		}
		msg := msg

		kv.mu.Lock() // lock first to avoid concurrent map read/write and ensure lineariability
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
		kv.mu.Unlock()

		// Must use goroutine to avoid deadlock:
		// case 1:kv.lock wait rf.lock
		// 			- PutAppendCmd => kv.lock wait rf.lock
		// 			- then applyCh came (it has acquired rf.lock) and wait kv.lock, but kv.lock is held by PutAppendCmd
		// 			- So deadlock happens.
		// case 2: rf.GetState trys to get lock during applyCh which still holds the lock
		go func() {
			// Be aware server and follower use the same code, so check exists to make sure only leader has this channel
			if _, isLeader := kv.rf.GetState(); isLeader {
				kv.mu.Lock()
				// Use lock to ensure pendingCh is created before applyCh or server would get stuck in <-kv.pendingCh[cmdIndex] during Get/PutAppend
				if _, exists := kv.pendingCh[msg.CommandIndex]; !exists {
					kv.pendingCh[msg.CommandIndex] = make(chan int, 1)
				}
				// send ch in lock to avoid concurrent map read/write
				kv.pendingCh[msg.CommandIndex] <- 0
				kv.mu.Unlock()
			}
		}()

	}
}

func (kv *KVServer) Request(args *ReqArg, reply *Reply) {
	// Your code here.
	kv.mu.RLock()
	if args.Seq <= kv.msgSeq[args.Cid] {
		if args.Op == OpGet {
			reply.Value = kv.state[args.Key]
		}
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	cmdIndex, term, isleader := kv.rf.Start(*args)
	// fmt.Println("peer:", kv.me, "Receive Get:", "key:", len(args.Key))
	reply.Term = term
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	var ch chan int
	// Use lock to ensure pendingCh is created first before applyCh or server might get stuck in select <-ch during Get/PutAppend
	if _, exists := kv.pendingCh[cmdIndex]; !exists {
		kv.pendingCh[cmdIndex] = make(chan int, 1)
	}
	ch = kv.pendingCh[cmdIndex]
	kv.mu.Unlock()

	select {
	case <-ch:
		kv.mu.Lock()
		close(ch)
		delete(kv.pendingCh, cmdIndex)

		if args.Op == OpGet {
			if val, exists := kv.state[args.Key]; exists {
				reply.Value = val
			} else {
				reply.Err = ErrNoKey
			}
		}
		kv.mu.Unlock()
	case <-time.After(raft.RaftElectionTimeout):
		reply.Err = ErrRetry
	}
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
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.pendingCh = map[int]chan int{}
	kv.state = make(map[string]string)
	kv.msgSeq = make(map[int64]int64)
	go kv.applyCmds()
	return kv
}
