package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	// You'll have to add code here.
	ck.cid = nrand()
	ck.seq = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	return ck.Request(key, "", OpGet)
}

func (ck *Clerk) Request(key string, value string, act Operation) string {
	args := ReqArg{
		Key:   key,
		Value: value,
		Op:    act,
		Cid:   ck.cid,
		Seq:   atomic.AddInt64(&ck.seq, 1),
	}
	var reply Reply

	for {
		reply = Reply{}
		ok := ck.servers[ck.leader].Call("KVServer.Request", &args, &reply)
		if ok && reply.Err == "" {
			break
		}
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrRetry {
			time.Sleep(1 * time.Millisecond)
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
	}
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op Operation) {
	// You will have to modify this function.
	ck.Request(key, value, op)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}
