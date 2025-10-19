package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	cid    int64
	seq    int64 // sequence number for each client
	leader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.cid = nrand()
	ck.seq = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		Cid: ck.cid,
		Seq: atomic.AddInt64(&ck.seq, 1),
	}
	// Your code here.
	args.Num = num
	for {
		var reply QueryReply
		ok := ck.servers[ck.leader].Call("ShardCtrler.Query", args, &reply)
		if ok && reply.Err == "" {
			return reply.Config
		}
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrRetry {
			time.Sleep(1 * time.Millisecond)
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		Cid: ck.cid,
		Seq: atomic.AddInt64(&ck.seq, 1),
	}
	// Your code here.
	args.Servers = servers

	for {
		// try each known server.
		var reply JoinReply
		ok := ck.servers[ck.leader].Call("ShardCtrler.Join", args, &reply)
		if ok && reply.Err == "" {
			return
		}
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrRetry {
			time.Sleep(1 * time.Millisecond)
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		Cid: ck.cid,
		Seq: atomic.AddInt64(&ck.seq, 1),
	}
	// Your code here.
	args.GIDs = gids

	for {
		var reply LeaveReply
		ok := ck.servers[ck.leader].Call("ShardCtrler.Leave", args, &reply)
		if ok && reply.Err == "" {
			return
		}
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrRetry {
			time.Sleep(1 * time.Millisecond)
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		Cid: ck.cid,
		Seq: atomic.AddInt64(&ck.seq, 1),
	}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	for {
		var reply MoveReply
		ok := ck.servers[ck.leader].Call("ShardCtrler.Move", args, &reply)
		if ok && reply.Err == "" {
			return
		}
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrRetry {
			time.Sleep(1 * time.Millisecond)
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
	}
}
