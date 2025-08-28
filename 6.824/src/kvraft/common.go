package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrRetry       = "ErrRetry"
)

type Err string
type Operation int

const (
	OpGet Operation = iota
	OpPut
	OpAppend
)

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
	Term  int
}

type Result struct {
	Cid   int64 // client id
	Seq   int64 // sequence number
	Value string
}
