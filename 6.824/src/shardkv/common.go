package shardkv

import "log"

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
	OpFetchShards
	OpUpdateCfg
)

func (op Operation) String() string {
	switch op {
	case OpGet:
		return "Get"
	case OpPut:
		return "Put"
	case OpAppend:
		return "Append"
	case OpFetchShards:
		return "FetchShards"
	case OpUpdateCfg:
		return "UpdateCfg"
	default:
		return "UnknownOperation"
	}
}

const (
	OK = "OK"
	// ErrNoKey       = "ErrNoKey"  // no need due to it'll return "" for Get request of non-existing key
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrRetry       = "ErrRetry"
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
	Key   string
	Value string
	Op    Operation // "Get", "Put", or "Append"
	Cid   int64     // client id
	Seq   int64     // sequence number
}

type Reply struct {
	Err   Err
	Value string
}

type Result struct {
	Cid   int64 // client id
	Seq   int64 // sequence number
	Value string
	Gid   int
}

type MvShardsReq struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Shards map[int]map[string]string // shard -> key-value map
	Value  string
	Op     Operation // "Get", "Put", or "Append"
	Cid    int64     // client id
	Seq    int64     // sequence number
}
