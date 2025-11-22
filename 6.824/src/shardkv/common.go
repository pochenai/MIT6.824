package shardkv

import (
	"log"
	"maps"

	"6.824/shardctrler"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Operation int

const (
	OpGet Operation = iota
	OpPut
	OpAppend
	OpMoveShards
	OpUpdateShardsState
	OpEmpty
)

func (op Operation) String() string {
	switch op {
	case OpGet:
		return "Get"
	case OpPut:
		return "Put"
	case OpAppend:
		return "Append"
	case OpMoveShards:
		return "MoveShards"
	case OpUpdateShardsState:
		return "UpdateShardsState"
	default:
		return "UnknownOperation"
	}
}

const (
	OK = "OK"
	// ErrNoKey       = "ErrNoKey"  // no need due to it'll return "" for Get request of non-existing key
	ErrWrongGroup       = "ErrWrongGroup"
	ErrWrongLeader      = "ErrWrongLeader"
	ErrWaitingSyncShard = "ErrWaitingSyncShard"
	ErrRetry            = "ErrRetry"
	ErrReqOutdated      = "ErrReqOutdated"
	ErrAlreadyUpdated   = "ErrAlreadyUpdated"
	ErrKilled           = "ErrKilled"
	ErrCtrlerKilled     = "ErrCtrlerKilled"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type ReqArg struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// common fields
	Cid int64 // client id
	Seq int64 // sequence number

	// for OpGet, OpPut, OpAppend
	Key   string
	Value []byte
	Op    Operation

	// for OpMoveShards, OpUpdateShardsState
	FromGid    int
	NextConfig shardctrler.Config // for OpMoveShards and OpUpdateShardsState
	Shards     []int              // only for OpMoveShards
	State      map[int]ShardState // only for OpUpdateShardsState
}

type Reply struct {
	Err   Err
	Value string
	// for sync shards
	ShardsState []byte // use bytes instead of map to avoid state change after network transfer
}

type ShardState struct {
	MsgSeq map[int64]int64
	State  map[string]string
}

func NewShardState() ShardState {
	return ShardState{
		MsgSeq: make(map[int64]int64),
		State:  make(map[string]string),
	}
}

func (s ShardState) Clone() ShardState {
	return ShardState{
		MsgSeq: maps.Clone(s.MsgSeq),
		State:  maps.Clone(s.State),
	}
}

type ShardsStatus int

const (
	StateSynced ShardsStatus = iota
	StateSyncing
	StateUpdated
)

type Result struct {
	Cid          int64 // client id
	Seq          int64 // sequence number
	Value        string
	Gid          int
	ShardsState  []byte
	ShardsStatus ShardsStatus
}
