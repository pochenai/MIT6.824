package shardctrler

import (
	"log"
	"sort"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (c *Config) Clone() Config {
	newG := make(map[int][]string, len(c.Groups))
	for k, v := range c.Groups {
		newSlice := make([]string, len(v))
		copy(newSlice, v)
		newG[k] = newSlice
	}
	return Config{
		Num:    c.Num,
		Shards: c.Shards,
		Groups: newG,
	}
}

func (c *Config) ShardsForGID(gid int) []int {
	var shards []int
	for shard, g := range c.Shards {
		if g == gid {
			shards = append(shards, shard)
		}
	}
	return shards
}

const (
	OK             = "OK"
	ErrRetry       = "ErrRetry"
	ErrWrongLeader = "ErrWrongLeader"
)

type Operation int

const (
	OpJoin Operation = iota
	OpLeave
	OpQuery
	OpMove
)

func (op Operation) String() string {
	switch op {
	case OpJoin:
		return "Join"
	case OpLeave:
		return "Leave"
	case OpQuery:
		return "Query"
	case OpMove:
		return "Move"
	default:
		return "UnknownOperation"
	}
}

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	Cid     int64            // client id
	Seq     int64            // sequence number
}

type JoinReply struct {
	Err Err
}

type LeaveArgs struct {
	GIDs []int
	Cid  int64 // client id
	Seq  int64 // sequence number
}

type LeaveReply struct {
	Err Err
}

type MoveArgs struct {
	Shard int
	GID   int
	Cid   int64 // client id
	Seq   int64 // sequence number
}

type MoveReply struct {
	Err Err
}

type QueryArgs struct {
	Num int   // desired config number
	Cid int64 // client id
	Seq int64 // sequence number
}

type QueryReply struct {
	Err    Err
	Config Config
}

type Reply struct {
	Err    Err
	Config Config
}

type Result struct {
	Cid    int64 // client id
	Seq    int64 // sequence number
	Config Config
}

func rebalanceShards(shards [NShards]int, op Operation, gids []int) [NShards]int {
	// Step 1: collect current active groups
	active := make(map[int]bool)
	for _, gid := range shards {
		if gid != 0 {
			active[gid] = true
		}
	}

	switch op {
	case OpJoin:
		for _, g := range gids {
			active[g] = true
		}
	case OpLeave:
		for _, g := range gids {
			delete(active, g)
		}
	}

	// No groups left → all shards unassigned
	if len(active) == 0 {
		var empty [NShards]int
		return empty
	}

	// Step 2: sorted list of active gids
	groups := make([]int, 0, len(active))
	for g := range active {
		groups = append(groups, g)
	}
	sort.Ints(groups)

	// Step 3: target load per group
	targetLow := NShards / len(groups)
	targetHigh := targetLow + 1
	numHigh := NShards % len(groups)

	// Build a map of gid → desired load
	desired := make(map[int]int)
	for i, g := range groups {
		if i < numHigh {
			desired[g] = targetHigh
		} else {
			desired[g] = targetLow
		}
	}

	// Step 4: count current assignment
	count := make(map[int]int)
	for _, g := range shards {
		if g != 0 && active[g] {
			count[g]++
		}
	}

	// Step 5: reassign shards from removed groups first
	for i, g := range shards {
		if !active[g] {
			for _, target := range groups {
				if count[target] < desired[target] {
					shards[i] = target
					count[target]++
					break
				}
			}
		}
	}

	// Step 6: rebalance overfull → underfull, deterministic order
	for i, g := range shards {
		if count[g] > desired[g] {
			for _, target := range groups {
				if count[target] < desired[target] {
					// move shard i from g → target
					shards[i] = target
					count[g]--
					count[target]++
					break
				}
			}
		}
	}

	return shards
}
