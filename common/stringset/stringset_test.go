package stringset

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	testItem = "testItem"
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
	testSet.Add(testItem)
	assert.Equal(t, true, testSet.m[testItem])
}

func TestStringSet_Contains(t *testing.T) {
	// Create a new StringSet
	testSet := &stringSet{
		m: make(map[string]bool),
	}
	assert.Equal(t, false, testSet.Contains(testItem))

	// Add test task to StringSet
	testSet.m[testItem] = true
	assert.Equal(t, true, testSet.Contains(testItem))
}

func TestStringSet_Remove(t *testing.T) {
	// Create a new StringSet
	testSet := &stringSet{
		m: make(map[string]bool),
	}
	// Add test task to StringSet
	testSet.m[testItem] = true
	assert.Equal(t, true, testSet.m[testItem])

	testSet.Remove(testItem)
	assert.Equal(t, false, testSet.m[testItem])
}

func TestStringSet_Clear(t *testing.T) {
	// Create a new StringSet
	testSet := &stringSet{
		m: make(map[string]bool),
	}
	// Add test task to StringSet
	testSet.m[testItem] = true
	assert.Equal(t, 1, len(testSet.m))

	testSet.Clear()
	assert.Equal(t, 0, len(testSet.m))
}

func TestStringSet_ToSlice(t *testing.T) {
	// Create a new StringSet
	testSet := &stringSet{
		m: make(map[string]bool),
	}

	testItems := []string{
		"testitem1",
		"testitem2",
	}
	// Add testItems to StringSet
	for _, item := range testItems {
		testSet.m[item] = true
	}
	items := testSet.ToSlice()
	assert.Len(t, items, len(testItems))
	for _, item := range items {
		assert.True(t, testSet.Contains(item))
	}
}
