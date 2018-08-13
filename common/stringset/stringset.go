package stringset

import (
	"sync"
)

// StringSet defines the interface for a set of strings
type StringSet interface {
	// Add adds 'key' to the set
	Add(key string)
	// Remove removes 'key' from the set
	Remove(key string)
	// Contains checks if the set contains 'key'
	Contains(key string) bool
	// Clear clears the contents of set
	Clear()
	// ToSlice returns a slice containing all elements in the set
	ToSlice() []string
}

// stringSet implements StringSet interface. It is thread safe
type stringSet struct {
	sync.RWMutex
	m map[string]bool
}

// New creates and initializes a new StringSet
func New() StringSet {
	s := &stringSet{
		m: make(map[string]bool),
	}
	return s
}

// Add adds 'key' to the set
func (s *stringSet) Add(key string) {
	defer s.Unlock()
	s.Lock()

	s.m[key] = true
}

// Contains checks if the set contains 'key'
func (s *stringSet) Contains(key string) bool {
	defer s.RUnlock()
	s.RLock()

	return s.m[key]
}

// Remove removes 'key' from the set
func (s *stringSet) Remove(key string) {
	defer s.Unlock()
	s.Lock()

	delete(s.m, key)
}

// Clear clears the contents of the set
func (s *stringSet) Clear() {
	defer s.Unlock()
	s.Lock()

	for k := range s.m {
		delete(s.m, k)
	}
}

// ToSlice returns a slice containing all elements in the set
// This method is required to range over the set (as we cannot range over the
// custom type StringSet)
func (s *stringSet) ToSlice() []string {
	defer s.RUnlock()
	s.RLock()

	keys := make([]string, 0, len(s.m))
	for k := range s.m {
		keys = append(keys, k)
	}
	return keys
}
