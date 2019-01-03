package randutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Tests the length of the requested random text.
func TestTextLength(t *testing.T) {
	n := 25
	assert.Len(t, Text(n), n)
}

// Ensures that Text results do not collide.
func TestTextRandomness(t *testing.T) {
	n := 1000
	results := make(map[string]struct{})
	for i := 0; i < n; i++ {
		results[string(Text(10))] = struct{}{}
	}
	assert.Len(t, results, n)
}
