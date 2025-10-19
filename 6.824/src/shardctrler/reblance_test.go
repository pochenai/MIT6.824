package shardctrler

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReBalance(t *testing.T) {
	shards := [NShards]int{}
	// Test join
	joinResults := [][NShards]int{
		{1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
		{2, 2, 2, 2, 2, 1, 1, 1, 1, 1},
		{3, 3, 2, 2, 2, 3, 1, 1, 1, 1},
		{4, 3, 2, 2, 2, 3, 4, 1, 1, 1},
		{4, 3, 5, 2, 2, 3, 4, 5, 1, 1},
		{4, 3, 6, 2, 2, 3, 4, 5, 1, 1},
		{7, 3, 6, 2, 2, 3, 4, 5, 1, 1},
		{7, 8, 6, 2, 2, 3, 4, 5, 1, 1},
		{7, 8, 6, 9, 2, 3, 4, 5, 1, 1},
		{7, 8, 6, 9, 2, 3, 4, 5, 10, 1},
	}
	for i := 1; i <= NShards; i++ {
		shards = rebalanceShards(shards, OpJoin, []int{i})
		assert.Equal(t, joinResults[i-1], shards)
	}

	// Test leave
	leaveResults := [][NShards]int{
		{7, 8, 6, 9, 2, 3, 4, 5, 10, 2},
		{7, 8, 6, 9, 3, 3, 4, 5, 10, 4},
		{7, 8, 6, 9, 5, 6, 4, 5, 10, 4},
		{7, 8, 6, 9, 5, 6, 7, 5, 10, 8},
		{7, 8, 6, 9, 9, 6, 7, 10, 10, 8},
		{7, 8, 7, 9, 9, 8, 7, 10, 10, 8},
		{8, 8, 9, 9, 9, 8, 10, 10, 10, 8},
		{9, 9, 9, 9, 9, 10, 10, 10, 10, 10},
		{10, 10, 10, 10, 10, 10, 10, 10, 10, 10},
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
	}
	for i := 1; i <= NShards; i++ {
		shards = rebalanceShards(shards, OpLeave, []int{i})
		assert.Equal(t, leaveResults[i-1], shards)
	}

	// Test join twice and multiple gids
	shards = rebalanceShards(shards, OpJoin, []int{1})
	assert.Equal(t, [NShards]int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, shards)
	shards = rebalanceShards(shards, OpJoin, []int{2})
	assert.Equal(t, [NShards]int{2, 2, 2, 2, 2, 1, 1, 1, 1, 1}, shards)
	shards = rebalanceShards(shards, OpJoin, []int{2})
	assert.Equal(t, [NShards]int{2, 2, 2, 2, 2, 1, 1, 1, 1, 1}, shards)
	shards = rebalanceShards(shards, OpJoin, []int{1, 2})
	assert.Equal(t, [NShards]int{2, 2, 2, 2, 2, 1, 1, 1, 1, 1}, shards)

	// Test leave twice and multiple gids
	shards = rebalanceShards(shards, OpLeave, []int{1})
	assert.Equal(t, [NShards]int{2, 2, 2, 2, 2, 2, 2, 2, 2, 2}, shards)
	shards = rebalanceShards(shards, OpLeave, []int{1})
	assert.Equal(t, [NShards]int{2, 2, 2, 2, 2, 2, 2, 2, 2, 2}, shards)
	shards = rebalanceShards(shards, OpJoin, []int{1})
	shards = rebalanceShards(shards, OpLeave, []int{1, 2})
	assert.Equal(t, [NShards]int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, shards)
	shards = rebalanceShards(shards, OpLeave, []int{1, 2})
	assert.Equal(t, [NShards]int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, shards)
}
