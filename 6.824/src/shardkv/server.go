package shardkv

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type ShardKV struct {
	mu       sync.RWMutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	make_end func(string) *labrpc.ClientEnd
	gid      int // it'll never change for the server
	// ctrlers      []*labrpc.ClientEnd
	sm           *shardctrler.Clerk
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead            int32 // set by Kill()
	done            chan int
	config          shardctrler.Config
	avaliableShards map[int]bool
	state           map[int]ShardState // shard -> (state: key-value map, msgSeq: clientId -> max requestId)
	shardMoveSeq    map[int64]int64    // clientId -> max requestId
	pendingCh       map[int]chan Result
	persister       *raft.Persister
	// for sync shards RPC requests
	cid int64
	seq int64 // sequence number for each client
}

// 非幂等操作，因为有可能shards开始接收request，state已经变了；然后又把state设置为之前过期的重复请求
func (kv *ShardKV) updateConfigAndState(op ReqArg) {
	kv.config = op.NextConfig
	for shard, state := range op.State {
		if !kv.avaliableShards[shard] {
			kv.state[shard] = state
			kv.avaliableShards[shard] = true
		}
	}
}

func (kv *ShardKV) applyCmds() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if !msg.CommandValid && !msg.SnapshotValid { // Must skip this due to invalid command used to ensure snapshot is processed during raft RPC.
				continue
			}

			// DPrintf("group:%v, peer %v receive applyCh: %v, isSnapshot:%v", kv.gid, kv.me, msg.CommandIndex, msg.SnapshotValid)
			kv.mu.Lock() // lock first to avoid concurrent map read/write and ensure lineariability
			if msg.SnapshotValid {
				if len(msg.Snapshot) < 1 {
					panic("snapshot is nil")
				}
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					DPrintf("Conda install snapshot, group:%v, leader:%v, peer:%v,term:%v,snapTerm:%v,snapIndex:%v", kv.gid, msg.LeaderId, kv.me, msg.Term, msg.SnapshotTerm, msg.SnapshotIndex)
					kv.restoreState(msg.Snapshot) // write kv.msgSeq, so it also should be locked
				}
				kv.mu.Unlock()
				continue
			}

			op := msg.Command.(ReqArg)

			// 单纯为了leader推进commitIndex，不做任何操作
			if op.Op == OpEmpty {
				continue
			}
			// leader and follower both have state, don't forget to set follower's state
			var result Result
			var shard int

			// 所有的操作是幂等的(因为除了config从0升级外，不涉及状态update)，不用过滤重复请求，以减少client重试次数
			// only update shardMoveSeq when moveShards success, or we'll let the client retry and process it again.
			// if we update shardMoveSeq even if not success, the command will not be processed again, which caused the server never updated to new config.
			if op.Op == OpMoveShards {
				prevConfig := op.PrevNextConfig
				nextConfig := op.NextConfig
				prevNewNum := prevConfig.Num
				currentNum := kv.config.Num

				if msg.LeaderId == kv.me {
					DPrintf("=====syncing group:%v,fromG:%v,peer:%v,leader:%v,term:%v,cmdIndex:%v,moveshards:%v, latest:%v, prevLastest:%v, current:%v,synced:%v, processed:%v", kv.gid, op.FromGid, kv.me, msg.LeaderId, msg.Term, msg.CommandIndex, op.Shards, nextConfig.Num, prevNewNum, currentNum, kv.synced(&kv.config), op.Seq > kv.shardMoveSeq[op.Cid])
				}

				if op.Seq > kv.shardMoveSeq[op.Cid] {
					kv.shardMoveSeq[op.Cid] = op.Seq
				}

				if prevNewNum < currentNum {
					result.ShardsStatus = StateSynced
				}

				if prevNewNum == currentNum {
					if kv.synced(&kv.config) {
						// 由于升级了config，所以如下操作的前置条件以后就不成立了，因此不会被重试，是幂等操作
						// update to new config, reset avaliable and update previous owned to be avaliable
						kv.avaliableShards = map[int]bool{}
						if currentNum == 0 { // 说明latestConfig为1,是server自己Config更新请求
							for _, s := range nextConfig.ShardsForGID(kv.gid) {
								kv.state[s] = NewShardState()
								kv.avaliableShards[s] = true
							}
						} else {
							for _, s := range nextConfig.ShardsForGID(kv.gid) {
								// only set previous owned shards to be avaliable
								if slices.Contains(prevConfig.ShardsForGID(kv.gid), s) {
									kv.avaliableShards[s] = true
								}
							}
						}
						kv.config = nextConfig
						result.ShardsStatus = StateSynced
					} else {
						result.ShardsStatus = StateSyncing
					}
				}

				if prevNewNum > currentNum { // we lag at least 1 config from latest config
					result.ShardsStatus = StateSyncing
				}

				// op.msgSeq must synced when moving shards, or when client's append/put op got !ok, it'll send the req again to the new shards, which make the system execute the same command multiple times, results in non-lineariable.
				// If the server is MoveShards from itself, because we've updated it to new config, we don't need to send state and updateShards again to avoid overriding new state to old state if leader changes.
				if result.ShardsStatus == StateSynced && op.FromGid != kv.gid {
					shards := op.Shards
					shardsState := make(map[int]ShardState)
					for _, s := range shards {
						shardsState[s] = kv.state[s].Clone()
					}
					shardsStateJson, err := json.Marshal(shardsState)
					if err != nil {
						panic("fail to pack shardsState")
					}
					result.ShardsState = shardsStateJson
				}
				// if there is no pendingCh, we don't send. So, we don't need to worry about blocking the corresponding request handler.
				if msg.LeaderId == kv.me {
					DPrintf("-----syncing group:%v,fromG:%v,peer:%v,leader:%v,term:%v,cmdIndex:%v,applyCh op:%v, shards:%v, state:%v, success:%v", kv.gid, op.FromGid, kv.me, msg.LeaderId, msg.Term, msg.CommandIndex, op.Op, op.Shards, op.State, result.ShardsStatus == StateSynced)
				}
				goto Return_result
			}

			if op.Op == OpUpdateShardsState {
				nextConfig := op.NextConfig
				prevConfig := op.PrevNextConfig

				if msg.LeaderId == kv.me {
					DPrintf("=====syncing group:%v,fromG:%v,peer:%v,leader:%v,term:%v,cmdIndex:%v,updateshards, latest:%v, prevLastest:%v,current:%v,opState:%v,synced:%v, processed:%v", kv.gid, op.FromGid, kv.me, msg.LeaderId, msg.Term, msg.CommandIndex, nextConfig.Num, prevConfig.Num, kv.config.Num, op.State, kv.synced(&kv.config), op.Seq > kv.shardMoveSeq[op.Cid])
				}
				// update的操作是非幂等的，因为可能会导致旧状态覆盖新状态，所以需要过滤重复请求
				if op.Seq <= kv.shardMoveSeq[op.Cid] {
					// do nothing because we've updated
					goto Return_result
				}
				kv.shardMoveSeq[op.Cid] = op.Seq

				if nextConfig.Num < kv.config.Num {
					// do nothing, because we've updated
					result.ShardsStatus = StateUpdated
				}

				if nextConfig.Num == kv.config.Num {
					// only update config for the unavailiable shards, or the state will be overwritten by the duplicated requests
					kv.updateConfigAndState(op)

					result.ShardsStatus = StateUpdated
				}

				if nextConfig.Num == kv.config.Num+1 {
					if kv.synced(&kv.config) {
						// must reset avaliable then update it, cause we are upgrading to new config
						kv.avaliableShards = map[int]bool{}
						prevOwnedShards := kv.config.ShardsForGID(kv.gid)
						for _, shard := range nextConfig.ShardsForGID(kv.gid) {
							// If it's current is initial config or owned some shards for nextconfig, we should set it avaliable.
							// Or, the server will get stuck and can't move to the next config.
							if slices.Contains(prevOwnedShards, shard) || kv.config.Num == 0 {
								kv.avaliableShards[shard] = true
								// state is unchanged for prev owned shards
							}
						}
						// only update config for the unavailiable shards, or the state will be overwritten by the duplicated requests
						kv.updateConfigAndState(op)

						result.ShardsStatus = StateUpdated
					} else { // else reject, because we haven't synced yet
						result.ShardsStatus = StateSyncing
					}
				}

				if nextConfig.Num > kv.config.Num+1 {
					result.ShardsStatus = StateSyncing
				}
				// if there is no pendingCh, we don't send. So, we don't need to worry about blocking the corresponding request handler.
				if msg.LeaderId == kv.me {
					DPrintf("-----syncing group:%v,fromG:%v,peer:%v,leader:%v, term:%v,cmdIndex:%v,cfg:%v, peer:%d, applyCh op:%v, shards:%v, state:%v, success:%v", kv.gid, op.FromGid, kv.me, msg.LeaderId, msg.Term, msg.CommandIndex, kv.config.Num, kv.me, op.Op, op.Shards, kv.state, result.ShardsStatus == StateUpdated)
				}

				DPrintf("group:%v,peer:%v,leader:%v,updated config:num:%v,shards:%v, \n availiable:%v, synced:%v \n", kv.gid, kv.me, msg.LeaderId, kv.config.Num, kv.config.Shards, kv.avaliableShards, kv.synced(&kv.config))
				goto Return_result
			}

			// for OpGet, OpPut, OpAppend
			shard = key2shard(op.Key)
			result.Gid = kv.gid
			// don't process shards that don't belongs to current owned.
			if shardGid := kv.config.Shards[shard]; shardGid != kv.gid {
				result.Gid = shardGid
				goto Return_result
			}
			// don't process shards that are not avaliable.
			if avaliable := kv.avaliableShards[shard]; !avaliable {
				result.ShardsStatus = StateSyncing
				goto Return_result
			}

			if v, exist := kv.state[shard]; !exist || v.State == nil {
				kv.state[shard] = NewShardState()
			}
			// op.msgSeq must synced when moving shards, or when client's append/put op got !ok, it'll send the req again to the new shards, which make the system execute the same command multiple times, results in non-lineariable.
			if op.Seq > kv.state[shard].MsgSeq[op.Cid] {
				kv.state[shard].MsgSeq[op.Cid] = op.Seq
				switch op.Op {
				case OpPut:
					kv.state[shard].State[op.Key] = string(op.Value)
				case OpAppend:
					kv.state[shard].State[op.Key] += string(op.Value)
				}
				DPrintf("=====group:%v, peer:%v,term:%v, cmdIndex:%v,cfg:%v, applyCh op:%v, key:%v,value:%v, updatedV:%s, shard:%d,", kv.gid, kv.me, msg.Term, msg.CommandIndex, kv.config.Num, op.Op, op.Key, string(op.Value), kv.state[shard].State[op.Key], shard)
			}
			result.Value = kv.state[shard].State[op.Key]

		Return_result:
			// To avoid concurrent map read/write, we set result in lock.
			result.Cid = op.Cid
			result.Seq = op.Seq

			kv.mu.Unlock()

			// save state snapshot
			s := kv.persister.RaftStateSize()
			if s > kv.maxraftstate && kv.maxraftstate != -1 {
				DPrintf("group:%v, peer:%d, start persisting snapshot, size:%d, cmdInd:%d, cfg:%v", kv.gid, kv.me, s, msg.CommandIndex, kv.config.Num)
				snapshot := kv.snapshot()
				kv.rf.Snapshot(msg.CommandIndex, snapshot)
			}

			// DPrintf("group:%v, peer:%d, start sending applyCh, cmdInd:%d, isLeader:%v", kv.gid, kv.me, msg.CommandIndex, msg.LeaderId == kv.me)
			kv.mu.RLock()
			// use msg.LeaderId == kv.me (or can use lastTerm + isleader to check if term has changed) to check if is leader rather than set kv.isLeader than check use kv.isLeader. Or if the leader is stale, it might not realized it's not leader anymore, so there will be many unhandled ch, which cause memory leaks.
			if msg.LeaderId == kv.me { // Be aware server and follower use the same code, so check exists to make sure only leader has this channel
				// Use lock to ensure pendingCh is created before applyCh or server would get stuck in <-kv.pendingCh[cmdIndex] during Get/PutAppend
				ch, exists := kv.pendingCh[msg.CommandIndex]
				// we only send result if there is already a channel, or we don't send because there might be no handlers (due to new leader commit old leader's msg) for this applyCh , which might cause memory leak or deadlock.s
				if exists {
					// DPrintf("group:%v leader %v sending ch", kv.gid, kv.me)
					ch <- result
				}
			}
			kv.mu.RUnlock()
			// DPrintf("group:%v, peer:%d, sending applyCh done, cmdInd:%d, isLeader:%v", kv.gid, kv.me, msg.CommandIndex, msg.LeaderId == kv.me)

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

func (kv *ShardKV) Request(args *ReqArg, reply *Reply) {
	// Your code here.
	kv.mu.RLock()
	// For lineariazability, we don't allow old PUT/Append execute twice. Get is ok.
	// Acturally some seq less than this might not be handled.
	if args.Op == OpGet || args.Op == OpPut || args.Op == OpAppend {
		shard := key2shard(args.Key)
		if shardGid := kv.config.Shards[shard]; shardGid != kv.gid {
			reply.Err = ErrWrongGroup
			kv.mu.RUnlock()
			return
		}
		if avaliable := kv.avaliableShards[shard]; !avaliable {
			reply.Err = ErrWaitingSyncShard
			kv.mu.RUnlock()
			return
		}
		if args.Seq <= kv.state[shard].MsgSeq[args.Cid] {
			if shardState, exists := kv.state[shard]; exists {
				reply.Value = shardState.State[args.Key]
			}
			kv.mu.RUnlock()
			return
		}
	}

	if args.Op == OpMoveShards || args.Op == OpUpdateShardsState {
		// for simple: we just let the client retry with newid;
		// TODO optimization: calculate the result and reply
		if args.Seq <= kv.shardMoveSeq[args.Cid] {
			if args.Op == OpMoveShards {
				// it means the request succeed for OpUpdateShardsState, so we don't need to set err. But OpMoveShards requires reply.ShardState, so it needs to retry
				reply.Err = ErrReqOutdated
			}
			kv.mu.RUnlock()
			return
		}
	}
	// for OpFetchShards, we directly send the cmd to raft
	kv.mu.RUnlock()

	cmdIndex, _, isleader := kv.rf.Start(*args)
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	// DPrintf("group:%v, leader:%v start apply cmd:%v, op:%v", kv.gid, kv.me, cmdIndex, args.Op)
	kv.mu.Lock()
	var ch chan Result
	// Use lock to ensure pendingCh is created first before applyCh or server might get stuck in select <-ch during Get/PutAppend
	if _, exists := kv.pendingCh[cmdIndex]; !exists {
		// Here channel length is set to 1, to avoid the scenario that the channel is created, then the result selector timeout, but the channel hasn't been removed, then applyCh try to send to the channel, but there is no consumer, it will be blocked for ever.
		kv.pendingCh[cmdIndex] = make(chan Result, 1)
	}
	ch = kv.pendingCh[cmdIndex]
	kv.mu.Unlock()
	// DPrintf("group:%v, leader:%v waiting for cmd:%v", kv.gid, kv.me, cmdIndex)

	select {
	case result := <-ch:
		// DPrintf("group:%v, leader %v start reply", kv.gid, kv.me)
		// Check this first, because the result might should send to other client's request which share the same cmdIndex.
		// If so, the leader certainly has changed,we should let the client retry.
		if result.Cid != args.Cid || result.Seq != args.Seq {
			reply.Err = ErrWrongLeader
		} else {
			// for OpGet, OpPut and OpAppend: check the config hasn't changed which cause shards don't belongs to the current server.
			if args.Op == OpGet || args.Op == OpPut || args.Op == OpAppend {
				if result.Gid != kv.gid {
					reply.Err = ErrWrongGroup
				}
			}
			// for OpMoveShards, OpUpdateShardState: check whether the server is synced or not.
			if result.ShardsStatus == StateSyncing {
				reply.Err = ErrWaitingSyncShard
			}

			// for OpGet, OpPut and OpAppend
			reply.Value = result.Value
			// for OpMoveShards
			reply.ShardsState = result.ShardsState
		}
		// DPrintf("group:%v, leader %v reply done", kv.gid, kv.me)
	case <-time.After(raft.RaftElectionTimeout):
		// DPrintf("group:%v, leader %v timeout retry", kv.gid, kv.me)
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

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.done)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) snapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.state)
	e.Encode(kv.avaliableShards)
	e.Encode(kv.config)
	e.Encode(kv.shardMoveSeq)
	return w.Bytes()
}

func (kv *ShardKV) restoreState(snapshot []byte) {
	kv.state = make(map[int]ShardState)
	kv.avaliableShards = map[int]bool{}
	kv.config = shardctrler.Config{}
	kv.shardMoveSeq = make(map[int64]int64)
	if len(snapshot) < 1 {
		return
	}

	// If we don't initialize the kv state, the previous state will also be decoded into the new state instead if overwriting all fields!
	rs := bytes.NewBuffer(snapshot)
	ds := labgob.NewDecoder(rs)
	var tmpConfig shardctrler.Config
	if ds.Decode(&kv.state) != nil || ds.Decode(&kv.avaliableShards) != nil || ds.Decode(&tmpConfig) != nil || ds.Decode(&kv.shardMoveSeq) != nil {
		panic("Restore from snapshot failed")
	}
	kv.config = tmpConfig
	DPrintf("group:%v, peer:%v, Restored state from snapshot, state len:%d, config:%v, shards:%v,avaliable:%v, synced:%v", kv.gid, kv.me, len(snapshot), kv.config.Num, kv.config.Shards, kv.avaliableShards, kv.synced(&kv.config))
}

// synced if any of the following conditions are met:
// 1.gid doesn't have shards in previous config (the config parameter)
// 2.gid have shards in prev config and these shards are all avaliable
func (kv *ShardKV) synced(config *shardctrler.Config) bool {
	synced := true
	for _, s := range config.ShardsForGID(kv.gid) {
		synced = synced && kv.avaliableShards[s]
	}
	return synced
}

// 定义: 找到nextConig 对应的 gid和shards
func (kv *ShardKV) shardsToSyncForNextCfg(nextNum int) (map[int]ReqArg, shardctrler.Config, shardctrler.Config) {
	nextCfg := kv.sm.Query(nextNum)
	if nextNum == 1 {
		return nil, nextCfg, shardctrler.Config{}
	}
	prevNextConfig := kv.sm.Query(nextNum - 1)
	// sync shards from others (计算nextCfg所需的shards，各自在上一轮config所在的groupID)
	newOwnedShards := nextCfg.ShardsForGID(kv.gid)
	syncGrpShards := make(map[int][]int) // prevLatest gid=> next config required shards

	for _, s := range newOwnedShards {
		gid := prevNextConfig.Shards[s]
		_, ok := syncGrpShards[gid]
		if !ok {
			syncGrpShards[gid] = make([]int, 0)
		}
		syncGrpShards[gid] = append(syncGrpShards[gid], s)
	}

	reqArgs := make(map[int]ReqArg, len(syncGrpShards))
	for gid, shards := range syncGrpShards {
		reqArgs[gid] = ReqArg{
			Cid:            kv.cid,
			Seq:            atomic.AddInt64(&kv.seq, 1),
			Op:             OpMoveShards,
			FromGid:        kv.gid,
			NextConfig:     nextCfg.Clone(),
			PrevNextConfig: prevNextConfig.Clone(),
			Shards:         shards,
		}
	}

	return reqArgs, nextCfg, prevNextConfig
}

// pull shards required by the nextconfig from the groups owned the shareds in the previous config.
func (kv *ShardKV) pullShards(nextNum int) error {
	movePrevShardsArgs, nextCfg, prevNextConfig := kv.shardsToSyncForNextCfg(nextNum)

	updateShardsFn := func(updateArg *ReqArg) Reply {
		var reply Reply
		for {
			reply = Reply{}
			kv.Request(updateArg, &reply)

			if reply.Err == "" {
				break
			}
			// TODO: this comment might not need. we must update the seq, because some larger seq might be processed before the currenct seq is processed, which caused the client never get the required shards.
			if reply.Err == ErrWaitingSyncShard || reply.Err == ErrRetry {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if reply.Err == ErrWrongLeader { // leader changed, we should return
				break
			}
		}
		return reply
	}
	// If there is no owned shards for nextConfig, we directly updateShardsState for nextConfig with empty config.
	// Must do this, or if no owned shards, the server will get stuck due to no moveShards RPC will be sent.
	if len(movePrevShardsArgs) == 0 {
		DPrintf("=====syncing group:%v,peer:%v,pullShardsReq nothing, next:%v, prevNext:%v, current%v", kv.gid, kv.me, nextCfg, prevNextConfig, kv.config.Num)
		updateShardsStateCmd := ReqArg{
			Cid:            kv.cid,
			Seq:            atomic.AddInt64(&kv.seq, 1),
			Op:             OpUpdateShardsState,
			FromGid:        kv.gid,
			PrevNextConfig: prevNextConfig,
			NextConfig:     nextCfg,
			State:          make(map[int]ShardState),
		}

		reply := updateShardsFn(&updateShardsStateCmd)
		if reply.Err == ErrWrongLeader {
			return errors.New(ErrWrongLeader)
		} else {
			return nil
		}
	}

	// TODO: pull shards concurrently for each group
	for gid, mvArg := range movePrevShardsArgs {
		servers := mvArg.PrevNextConfig.Groups[gid]
		leader := 0
		srv := kv.make_end(servers[leader])
		DPrintf("=====syncing group:%v,peer:%v,pullShardsReq:%v toGid:%v, next:%v, prevNext:%v, current%v", kv.gid, kv.me, mvArg.Shards, gid, nextCfg, prevNextConfig, kv.config.Num)
		var reply Reply
		for {
			reply = Reply{}
			ok := srv.Call("ShardKV.Request", &mvArg, &reply)

			if ok && reply.Err == "" {
				break
			}

			// TODO: this common might is not needed. we must update the seq, because some larger seq might be processed before the currenct seq is processed, which caused the client never get the required shards.
			if reply.Err == ErrWaitingSyncShard || reply.Err == ErrRetry {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if reply.Err == ErrReqOutdated {
				time.Sleep(100 * time.Millisecond)
				mvArg.Seq = atomic.AddInt64(&kv.seq, 1)
				continue
			}
			if !ok || reply.Err == ErrWrongLeader {
				time.Sleep(50 * time.Millisecond)
				leader = (leader + 1) % len(servers)
				srv = kv.make_end(servers[leader])
			}
		}

		// for the server previous owned shards, we've already moved it in the above MoveShards request, just skip it.
		if gid == kv.gid {
			DPrintf("=====syncing group:%v,peer:%v,pullShardsReqReturn:%v toGid:%v, next:%v, prevNext:%v, current%v, skipedGid", kv.gid, kv.me, mvArg.Shards, gid, nextCfg, prevNextConfig, kv.config.Num)
			continue
		}

		jsonState := reply.ShardsState
		shardsState := make(map[int]ShardState)
		err := json.Unmarshal(jsonState, &shardsState)
		if err != nil {
			panic("failed to parse shardsState")
		}

		DPrintf("=====syncing group:%v,peer:%v,pullShardsReqReturn:%v toGid:%v, next:%v, prevNext:%v, current%v, state:%v", kv.gid, kv.me, mvArg.Shards, gid, nextCfg, prevNextConfig, kv.config.Num, shardsState)

		updateShardsStateCmd := ReqArg{
			Cid:            kv.cid,
			Seq:            atomic.AddInt64(&kv.seq, 1),
			Op:             OpUpdateShardsState,
			FromGid:        kv.gid,
			PrevNextConfig: mvArg.PrevNextConfig,
			NextConfig:     mvArg.NextConfig,
			State:          shardsState,
		}
		updateReply := updateShardsFn(&updateShardsStateCmd)

		DPrintf("=====syncing group:%v,peer:%v,updateShardsReqReturn:%v toGid:%v, next:%v, prevNext:%v, current%v, state:%v err:%v", kv.gid, kv.me, mvArg.Shards, kv.gid, nextCfg, prevNextConfig, kv.config.Num, shardsState, updateReply.Err)

		// must handler this error, or the following dead loop might happen:
		/*	the latest config to sync is C4, leader 1 current is C2 unsynced (required s1 from other group)
			leader1: got shards state for C2 from other groups, when leader1 request itself updateShardsState
				- leader changed to leader2, so, update shards fail
				- leader1 continue to update to C3, it will defintely fail, because leader1/2 isn't synced
				- then leader1 is back and leader2 is still unsynced, leader1 will loop endlessly failing update to C3 due to C2 sync is skipped and it'll never be tried again.
		*/
		if updateReply.Err == ErrWrongLeader {
			return errors.New(ErrWrongLeader)
		}
	}
	return nil
}

func (kv *ShardKV) syncToLatestCfg(currentCfg, latestCfg *shardctrler.Config, synced bool) {
	nextStartNum := currentCfg.Num
	if synced {
		nextStartNum = currentCfg.Num + 1
	}

	DPrintf("=====syncing group:%v,peer:%v, tolatest,from:%v, latest:%v, current%v", kv.gid, kv.me, nextStartNum, latestCfg, currentCfg)

	for nextNum := nextStartNum; nextNum <= latestCfg.Num; nextNum++ {
		err := kv.pullShards(nextNum)
		if err != nil {
			fmt.Printf("=====syncing group:%v,pullShards,peer:%v,num:%v, error:%v\n", kv.gid, kv.me, nextNum, err)
			// let the new leader to re-pull the required failed shards;
			// must skip following updates to avoid endless failure on later update due to the previous is unsynced.
			return
		}
	}
}

// 类似于2C的活锁问题: 前一个leader刚发送startcmd，其他节点也收到了但是还没有提交就挂了；然后新leader被选举出来，但是在新term没有新的command，而raft只允许提交自己term的log，所以新leader会一直提交不了日志，导致无法推进config更新和shard同步。
func (kv *ShardKV) keepRaftLeaderToLatest() {
	for !kv.killed() {
		isleader, leaderAppliedNewLog := kv.rf.AppliedNewLogInCurrentTerm()
		if isleader && !leaderAppliedNewLog {
			emptyReq := ReqArg{
				Cid: kv.cid,
				Seq: atomic.AddInt64(&kv.seq, 1),
				Op:  OpEmpty,
			}
			kv.rf.Start(emptyReq)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(ReqArg{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	// kv.ctrlers = ctrlers
	kv.sm = shardctrler.MakeClerk(ctrlers)

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.restoreState(persister.ReadSnapshot())
	kv.rf = raft.MakeWithGid(servers, me, persister, kv.applyCh, gid, false)
	kv.pendingCh = map[int]chan Result{}
	kv.done = make(chan int)

	kv.cid = nrand()
	kv.seq = 0

	// initialize config
	go func() {
		for !kv.killed() {
			_, isleader := kv.rf.GetState()
			if isleader {
				latestConfig := kv.sm.Query(-1)

				kv.mu.RLock()
				currentNum := kv.config.Num
				currentConfig := kv.config
				synced := kv.synced(&currentConfig)
				kv.mu.RUnlock()
				// if latestConfig.Num == currentNum, we should also pullShards, or the server will stuck in some shards always unavaliable
				if latestConfig.Num > currentNum || (latestConfig.Num == currentNum && !synced) {
					if latestConfig.Num == 1 { // 初始配置，不需要从其他节点同步shards，直接更新到新shards
						moveShardsArgs := ReqArg{
							Cid:            kv.cid,
							Seq:            atomic.AddInt64(&kv.seq, 1),
							Op:             OpMoveShards,
							FromGid:        kv.gid,
							NextConfig:     latestConfig,
							PrevNextConfig: currentConfig, // 必须在lock中读取，否则可能导致错误
						}
						kv.rf.Start(moveShardsArgs)
					} else {
						kv.syncToLatestCfg(&currentConfig, &latestConfig, synced)
					}
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	go kv.applyCmds()

	// go kv.keepRaftLeaderToLatest()

	return kv
}
