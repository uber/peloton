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
