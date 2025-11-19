package shardctrler

import (
	"bytes"
	"encoding/json"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	configs []Config // indexed by config num

	// Your data here.
	dead         int32 // set by Kill()
	done         chan int
	msgSeq       map[int64]int64 // map from clientId to max requestId
	pendingCh    map[int]chan Result
	persister    *raft.Persister
	maxraftstate int // snapshot if log grows this big

}

type Op struct {
	// Your data here.
	Cid    int64 // client id
	Seq    int64 // sequence number
	OpType Operation
	Args   []byte
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	data, _ := json.Marshal(args)
	op := Op{
		Cid:    args.Cid,
		Seq:    args.Seq,
		OpType: OpJoin,
		Args:   data,
	}
	_reply := Reply{}
	sc.Request(&op, &_reply)
	reply.Err = _reply.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	data, _ := json.Marshal(args)
	op := Op{
		Cid:    args.Cid,
		Seq:    args.Seq,
		OpType: OpLeave,
		Args:   data,
	}
	_reply := Reply{}
	sc.Request(&op, &_reply)
	reply.Err = _reply.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	data, _ := json.Marshal(args)
	op := Op{
		Cid:    args.Cid,
		Seq:    args.Seq,
		OpType: OpMove,
		Args:   data,
	}
	_reply := Reply{}
	sc.Request(&op, &_reply)
	reply.Err = _reply.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	data, _ := json.Marshal(args)
	op := Op{
		Cid:    args.Cid,
		Seq:    args.Seq,
		OpType: OpQuery,
		Args:   data,
	}
	_reply := Reply{}
	sc.Request(&op, &_reply)
	reply.Err = _reply.Err
	reply.Config = _reply.Config
}

func (sc *ShardCtrler) applyCmds() {
	for !sc.killed() {
		select {
		case msg := <-sc.applyCh:
			if !msg.CommandValid && !msg.SnapshotValid { // Must skip this due to invalid command used to ensure snapshot is processed during raft RPC.
				continue
			}

			DPrintf("peer %v receive applyCh: %v, isSnapshot:%v", sc.me, msg.CommandIndex, msg.SnapshotValid)
			sc.mu.Lock() // lock first to avoid concurrent map read/write and ensure lineariability
			if msg.SnapshotValid {
				if len(msg.Snapshot) < 1 {
					panic("snapshot is nil")
				}
				if sc.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					sc.restoreState(msg.Snapshot) // write kv.msgSeq, so it also should be locked
				}
				sc.mu.Unlock()
				continue
			}

			var result Result
			op := msg.Command.(Op)
			result.Cid = op.Cid
			result.Seq = op.Seq
			// leader and follower both have state, don't forget to set follower's state
			if op.Seq > sc.msgSeq[op.Cid] {
				sc.msgSeq[op.Cid] = op.Seq
				switch op.OpType {
				case OpJoin:
					//  The new configuration should divide the shards as evenly as possible among the full set of groups, and should move as few shards as possible to achieve that goal. The shardctrler should allow re-use of a GID if it's not part of the current configuration (i.e. a GID should be allowed to Join, then Leave, then Join again).
					var args JoinArgs
					json.Unmarshal(op.Args, &args)
					config := sc.configs[len(sc.configs)-1].Clone()
					gids := make([]int, 0)
					for k := range args.Servers {
						gids = append(gids, k)
					}

					newShards := rebalanceShards(config.Shards, OpJoin, gids)
					maps.Copy(args.Servers, config.Groups)
					newConfig := Config{
						Num:    config.Num + 1,
						Shards: newShards,
						Groups: args.Servers,
					}
					DPrintf("OpJoin args:%v", args)
					DPrintf("OpJoin previous config:%v", sc.configs[len(sc.configs)-1])
					DPrintf("OpJoin new config:%v", newConfig)
					sc.configs = append(sc.configs, newConfig)

				case OpLeave:
					// create a new configuration that does not include those groups, and that assigns those groups' shards to the remaining groups. The new configuration should divide the shards as evenly as possible among the groups, and should move as few shards as possible to achieve that goal.
					// sc.state[op.Key] += op.Value
					var args LeaveArgs
					json.Unmarshal(op.Args, &args)
					config := sc.configs[len(sc.configs)-1].Clone()

					newShards := rebalanceShards(config.Shards, OpLeave, args.GIDs)
					newGroups := make(map[int][]string)
					for k, v := range config.Groups {
						newGroups[k] = v
					}
					for _, gid := range args.GIDs {
						delete(newGroups, gid)
					}
					newConfig := Config{
						Num:    config.Num + 1,
						Shards: newShards,
						Groups: newGroups,
					}
					DPrintf("OpLeave args:%v", args)
					DPrintf("OpLeave previous config:%v", sc.configs[len(sc.configs)-1])
					DPrintf("OpLeave new config:%v", newConfig)
					sc.configs = append(sc.configs, newConfig)
				case OpMove:
					// The Move RPC's arguments are a shard number and a GID. The shardctrler should create a new configuration in which the shard is assigned to the group. The purpose of Move is to allow us to test your software.
					var args MoveArgs
					json.Unmarshal(op.Args, &args)
					newConfig := sc.configs[len(sc.configs)-1].Clone()
					newConfig.Shards[args.Shard] = args.GID
					newConfig.Num += 1
					DPrintf("OpMove args:%v", args)
					DPrintf("OpMove previous config:%v", sc.configs[len(sc.configs)-1])
					DPrintf("OpMove new config:%v", newConfig)
					sc.configs = append(sc.configs, newConfig)
				}
			}

			// we should return correct results for OpQuery incase we receive late query results before OpJoin, OpLeave and OpMove. Or the result's for OpQuery will be config0, which is incorrect.
			if op.OpType == OpQuery {
				var args QueryArgs
				json.Unmarshal(op.Args, &args)
				if args.Num >= 0 && args.Num < len(sc.configs) {
					result.Config = sc.configs[args.Num]
				} else {
					result.Config = sc.configs[len(sc.configs)-1]
				}
				DPrintf("OpQuery args:%v", args)
				DPrintf("OpQuery result:%v", result.Config)
			}

			sc.mu.Unlock()

			// save state snapshot
			s := sc.persister.RaftStateSize()
			if s > sc.maxraftstate && sc.maxraftstate != -1 {
				DPrintf("peer:%d, start persisting snapshot, size:%d, cmdInd:%d", sc.me, s, msg.CommandIndex)
				sc.rf.Snapshot(msg.CommandIndex, sc.snapshot())
			}

			DPrintf("peer:%d, start sending applyCh, cmdInd:%d, isLeader:%v", sc.me, msg.CommandIndex, msg.LeaderId == sc.me)
			sc.mu.RLock()
			// use msg.LeaderId == kv.me (or can use lastTerm + isleader to check if term has changed) to check if is leader rather than set kv.isLeader than check use kv.isLeader. Or if the leader is stale, it might not realized it's not leader anymore, so there will be many unhandled ch, which cause memory leaks.
			if msg.LeaderId == sc.me { // Be aware server and follower use the same code, so check exists to make sure only leader has this channel
				// Use lock to ensure pendingCh is created before applyCh or server would get stuck in <-kv.pendingCh[cmdIndex] during Get/PutAppend
				ch, exists := sc.pendingCh[msg.CommandIndex]
				// we only send result if there is already a channel, or we don't send because there might be no handlers (due to new leader commit old leader's msg) for this applyCh , which might cause memory leak or deadlock.s
				if exists {
					DPrintf("leader %v sending ch", sc.me)
					ch <- result
				}
			}
			sc.mu.RUnlock()

			// Or could use goroutine to avoid deadlock, but this way may face linearizability error(because this applyCh might be send to other Request handler):
			// case 1:kv.lock wait rf.lock
			// 			- PutAppendCmd => kv.lock wait rf.lock
			// 			- then applyCh came (it has acquired rf.lock) and wait kv.lock, but kv.lock is held by PutAppendCmd
			// 			- So deadlock happens.
			// case 2: rf.GetState trys to get lock during applyCh which still holds the lock Request handler
		case <-sc.done: // add this case to avoid memory leak, when kv is killed but the go routine is stucked at msg := <-kv.applyCh and can't return
			return
		}

	}
}

func (sc *ShardCtrler) Request(op *Op, reply *Reply) {
	// Your code here.
	sc.mu.RLock()
	// For lineariazability, we don't allow old PUT/Append execute twice. Get is ok.
	// Acturally some seq less than this might not be handled.
	DPrintf("Request op:%v, seq:%v, svSeq:%v, bool:%v", op.OpType, op.Seq, sc.msgSeq[op.Cid], op.Seq <= sc.msgSeq[op.Cid])
	if op.Seq <= sc.msgSeq[op.Cid] {
		if op.OpType == OpQuery {
			var args QueryArgs
			json.Unmarshal(op.Args, &args)
			if args.Num >= 0 && args.Num < len(sc.configs) {
				reply.Config = sc.configs[args.Num]
			} else {
				reply.Config = sc.configs[len(sc.configs)-1]
			}
			DPrintf("Return query directly")
		}
		sc.mu.RUnlock()
		return
	}
	sc.mu.RUnlock()

	cmdIndex, _, isleader := sc.rf.Start(*op)
	if !isleader {
		DPrintf("Return WrongLeader")
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("leader:%v start apply cmd:%v", sc.me, cmdIndex)
	sc.mu.Lock()
	var ch chan Result
	// Use lock to ensure pendingCh is created first before applyCh or server might get stuck in select <-ch during Get/PutAppend
	if _, exists := sc.pendingCh[cmdIndex]; !exists {
		sc.pendingCh[cmdIndex] = make(chan Result, 1)
	}
	ch = sc.pendingCh[cmdIndex]
	sc.mu.Unlock()
	DPrintf("leader:%v waiting for cmd:%v", sc.me, cmdIndex)

	select {
	case result := <-ch:
		DPrintf("leader %v start reply", sc.me)
		// Check this first, because the result might should send to other client's request which share the same cmdIndex.
		// If so, the leader certainly has changed,we should let the client retry.
		if result.Cid != op.Cid || result.Seq != op.Seq {
			reply.Err = ErrWrongLeader
		} else {
			reply.Config = result.Config
		}
		DPrintf("leader %v reply done", sc.me)
	case <-time.After(raft.RaftElectionTimeout):
		DPrintf("leader %v timeout retry", sc.me)
		reply.Err = ErrRetry
	}

	sc.mu.Lock()
	// This opearation is idempotent, we don't need to worry about it like close(ch).
	// We put it in channel to make it faster and don't block the reply.
	delete(sc.pendingCh, cmdIndex)
	// We must first check map exists then close(ch) if use go routine, to avoid async close/sendto closed channel in case timeouts+leader change cause multiple handlers came on the same cmdIndex.
	close(ch)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
	close(sc.done)
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) snapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sc.configs)
	e.Encode(sc.msgSeq)
	return w.Bytes()
}

func (sc *ShardCtrler) restoreState(snapshot []byte) {
	sc.configs = make([]Config, 1)
	sc.msgSeq = make(map[int64]int64)
	if len(snapshot) < 1 {
		sc.configs[0].Groups = map[int][]string{}
		return
	}
	// If we don't initialize the kv state, the previous state will also be decoded into the new state instead if overwriting all fields!
	rs := bytes.NewBuffer(snapshot)
	ds := labgob.NewDecoder(rs)
	if ds.Decode(&sc.configs) != nil || ds.Decode(&sc.msgSeq) != nil {
		panic("Restore from snapshot failed")
	}
	DPrintf("Restored state from snapshot, state len:%d", len(snapshot))
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh, false)

	// Your code here.
	sc.persister = persister
	sc.pendingCh = map[int]chan Result{}
	sc.done = make(chan int)
	sc.msgSeq = make(map[int64]int64)
	sc.maxraftstate = -1
	go sc.applyCmds()

	return sc
}
