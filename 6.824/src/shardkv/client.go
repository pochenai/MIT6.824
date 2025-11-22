package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	cid         int64
	seq         int64       // sequence number for each client
	groupLeader map[int]int // gid -> leader index
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.groupLeader = make(map[int]int)
	ck.cid = nrand()
	ck.seq = 0
	return ck
}

func (ck *Clerk) Request(key string, value string, act Operation) string {
	args := ReqArg{
		Key:   key,
		Value: []byte(value),
		Op:    act,
		Cid:   ck.cid,
		Seq:   atomic.AddInt64(&ck.seq, 1),
	}

	// ask controler for the latest configuration.
	ck.config = ck.sm.Query(-1)

	var reply Reply
	shard := key2shard(key)
	fmt.Printf("Client start:%v, Request, key:%v, act:%v, shard:%v, leader:%v, gid:%v, config:%v \n", ck.cid, key, act, shard, ck.groupLeader[shard], ck.config.Shards[shard], ck.config.Num)

	start := time.Now()

	// use timeouts to handle killed shardctrler (e.g static testing), or this cmd will loop infinitely and never return, which causes OOM if run it multiple rounds.
	for time.Since(start) < 10*time.Second {
		reply = Reply{}

		gid := ck.config.Shards[shard]
		servers, exists := ck.config.Groups[gid]
		if !exists {
			time.Sleep(50 * time.Millisecond)
			// ask controler for the latest configuration.
			ck.config = ck.sm.Query(-1)
			continue
		}
		leader := ck.groupLeader[gid]

		srv := ck.make_end(servers[leader])
		ok := srv.Call("ShardKV.Request", &args, &reply)
		if ok && reply.Err == "" {
			break
		}

		if reply.Err == ErrWrongGroup {
			time.Sleep(50 * time.Millisecond)
			// ask controler for the latest configuration.
			ck.config = ck.sm.Query(-1)
			goto Continue
		}
		if reply.Err == ErrWaitingSyncShard {
			// don't need to increase seq, because we haven't update seq if it's syncing.
			time.Sleep(50 * time.Millisecond)
			goto Continue
		}
		if reply.Err == ErrReqOutdated { // it'll never happend for OpGet, OpPut and OpAppend, but we put it here for completeness
			time.Sleep(50 * time.Millisecond)
			args.Seq = atomic.AddInt64(&ck.seq, 1)
			goto Continue
		}
		// when not ok, we must change leader, because it mean the chosen leader is down.
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrRetry {
			time.Sleep(50 * time.Millisecond)
			ck.groupLeader[gid] = (ck.groupLeader[gid] + 1) % len(servers)
			// DPrintf("Client:%v, Seq:%v, Request shard %v gid %v try next leader %v", ck.cid, ck.seq, shard, gid, ck.shardsLeader[shard])
		}

	Continue:
		// time.Sleep(time.Millisecond * 500)
		// fmt.Printf("Client error:%v, key:%v, act:%v, shard:%v, gid:%v,config:%v, got replyErr:%v, ok:%v \n", ck.cid, key, act, shard, gid, ck.config.Num, reply.Err, ok)

		continue
	}
	gid := ck.config.Shards[shard]
	fmt.Printf("Client:%v, Request done, key:%v, act:%v, shard:%v,group:%v, leader:%v \n", ck.cid, key, act, shard, gid, ck.groupLeader[gid])
	return reply.Value
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	return ck.Request(key, "", OpGet)
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op Operation) {
	ck.Request(key, value, op)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}
