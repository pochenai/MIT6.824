package shardctrler

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfg(t *testing.T) {
	g := Config{Groups: make(map[int][]string)}
	g.Groups[0] = []string{"123", "456"}

	gCopy := g.Clone()
	assert.Equal(t, gCopy, g)

	gCopy.Groups[0][0] = "111"
	assert.Equal(t, gCopy.Groups[0][0], "111")
	assert.Equal(t, g.Groups[0][0], "123")
}
