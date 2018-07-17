package stringset

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	testTask = "testTask"
)

func TestStringSet_New(t *testing.T) {
	testSet := New()
	assert.NotNil(t, testSet)
}

func TestStringSet_Add(t *testing.T) {
	// Create a new StringSet
	testSet := &stringSet{
		m: make(map[string]bool),
	}
	// Add test task to StringSet
	testSet.Add(testTask)
	assert.Equal(t, true, testSet.m[testTask])
}

func TestStringSet_Contains(t *testing.T) {
	// Create a new StringSet
	testSet := &stringSet{
		m: make(map[string]bool),
	}
	assert.Equal(t, false, testSet.Contains(testTask))

	// Add test task to StringSet
	testSet.m[testTask] = true
	assert.Equal(t, true, testSet.Contains(testTask))
}

func TestStringSet_Remove(t *testing.T) {
	// Create a new StringSet
	testSet := &stringSet{
		m: make(map[string]bool),
	}
	// Add test task to StringSet
	testSet.m[testTask] = true
	assert.Equal(t, true, testSet.m[testTask])

	testSet.Remove(testTask)
	assert.Equal(t, false, testSet.m[testTask])
}
