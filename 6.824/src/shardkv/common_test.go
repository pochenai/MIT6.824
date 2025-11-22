package shardkv

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"
	"time"

	"6.824/labgob"
	"6.824/shardctrler"
)

func TestStructAssign(t *testing.T) {
	s := NewShardState()
	println(s.MsgSeq == nil)

	state := make(map[int]ShardState)
	state[0] = s
	fmt.Println(state[0].MsgSeq == nil)
	state[0].MsgSeq[1] = 2
	fmt.Println(state)
}

func TestSLice(t *testing.T) {
	a := make([]int, 0)
	a = append(a, 1)
	for i := range a {
		println(i)
	}
}

func TestNestedLoop(t *testing.T) {
	j := 0
	for j < 10 {
		i := 0
		for i < 5 {
			if i == 1 {
				break
			}
			if i == 2 {
				return
			}
			i += 1
		}
		j++

		if j == 3 {
			return
		}

		println("outer loop", j)
		time.Sleep(time.Millisecond * 100)
	}
}

func TestDecode(t *testing.T) {
	type TestKV struct {
		avaliableShards map[int]bool
		state           map[int]ShardState // shard -> (state: key-value map,
		config          shardctrler.Config
	}

	snapshot := func(kv *TestKV) []byte {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.state)
		e.Encode(kv.avaliableShards)
		e.Encode(kv.config)
		return w.Bytes()
	}

	kv := TestKV{
		avaliableShards: make(map[int]bool),
		state:           make(map[int]ShardState),
		config: shardctrler.Config{
			Num: 1,
		},
	}

	for i := 0; i < 3; i++ {
		kv.avaliableShards[i] = true
		kv.state[i] = ShardState{
			MsgSeq: make(map[int64]int64),
			State:  make(map[string]string),
		}
		kv.state[i].State["i"] = fmt.Sprintf("%d", i)
	}

	newKv := TestKV{
		avaliableShards: map[int]bool{},
		state:           map[int]ShardState{},
		config:          shardctrler.Config{Num: 2},
	}

	for i := 0; i < 1; i++ {
		newKv.avaliableShards[i] = true
		newKv.state[i] = ShardState{
			MsgSeq: make(map[int64]int64),
			State:  make(map[string]string),
		}
		newKv.state[i].State["i"] = fmt.Sprintf("%d", i+1)
	}

	// If we don't initialize the kv state, the previous state will also be decoded into the new state instead if overwriting all fields!
	// kv = TestKV{}
	rs := bytes.NewBuffer(snapshot(&newKv))
	ds := labgob.NewDecoder(rs)
	tempConfig := shardctrler.Config{}
	if ds.Decode(&kv.state) != nil || ds.Decode(&kv.avaliableShards) != nil || ds.Decode(&tempConfig) != nil {
		panic("failed to decode testkv")
	}
	kv.config = tempConfig
	fmt.Println(kv)
}

func TestKey2Shards(t *testing.T) {
	n := 10
	ka := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards

		shard := key2shard(ka[i])
		fmt.Println("i:", i, "key:", ka[i], "shard:", shard)
	}
}
