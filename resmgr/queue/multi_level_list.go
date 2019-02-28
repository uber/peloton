// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package queue

import (
	"container/list"
	"fmt"
	"math"
	"sort"
	"sync"
)

// ErrorQueueEmpty represents the error that the queue is empty.
type ErrorQueueEmpty string

func (err ErrorQueueEmpty) Error() string {
	return string(err)
}

// MultiLevelList holds all the lists with different Level, and a limit
// on total size of the list. If the limit is negative there is no size limit
// on the list.
// The Multi level list is implemented using a map[Level]List.
// Which holds list per Level.
// Push operation is O(1)
// Pop Operation is O(1)
// Remove Operation is O(m) where m is the list size of specified level
// RemoveItems Operation is O(m) where m is the list size of specified level
type MultiLevelList interface {
	// Push method adds taskItem to MultiLevel List
	// it takes input as the level and value as interface{}
	Push(level int, element interface{}) error
	// PushList method adds list to MultiLevel List
	// it takes input as the level and list
	PushList(level int, newlist *list.List) error
	// Pop method removes the Front Item for the given Level
	Pop(level int) (interface{}, error)
	// PeekItem method returns the the Front Item for the given Level
	// It will not remove the item from the list
	PeekItem(level int) (interface{}, error)
	// PeekItems returns a list of items from a given level.
	// The number of items returns is min(limit, number of items in a given level).
	// Its returns an error if there are no items in the level.
	// It does not remove the items from the list.
	PeekItems(level int, limit int) ([]interface{}, error)
	// Remove method removes the specified item from multilevel list
	// It takes level and the value as the input parameter
	// If we have duplicate entry added we will be removing the first entry in the list
	// return value is error if there is any error in removing
	Remove(level int, value interface{}) error
	// RemoveItems method removes the specified items from multilevel list
	// First return value is true/false -> Success/failure
	// Second return value is item list not able to be deleted
	// error is third return value
	// This function returns false if only subset of the items being deleted
	RemoveItems(
		values map[interface{}]bool,
		level int) (bool, map[interface{}]bool, error)
	// IsEmpty method checks if the list for specified level is empty in multilevel list
	IsEmpty(level int) bool
	// Levels returns all the current levels in the list.
	Levels() []int
	// Len returns the number of items in multilevel list for specified level.
	Len(level int) int
	// Size returns the total number of items in multilevel list for all levels
	Size() int
	// GetHighestLevel returns the highest level from the list
	GetHighestLevel() int
}

// multiLevelList struct is implementing MultiLevelList
// which holds all the lists with different Level, and a limit
// on total size of the list. If the limit is negative there is no size limit
// on the list.
// The Multi level list is implemented using a map[Level]List.
// Which holds list per Level.
// Push operation is O(1)
// Pop Operation is O(1)
// Remove Operation is O(m) where m is the list size of specified level
// RemoveItems Operation is O(m) where m is the list size of specified level
type multiLevelList struct {
	// mutex for the multiLevelList
	sync.RWMutex
	// name of the list
	name string
	// map of lists for each level
	mapLists map[int]*list.List
	// highest level at any given time
	highestLevel int
	// max list size for any level
	limit int64
}

// NewMultiLevelList initializes the multi level list with a name and a max
// size, if the max size is negative, then the list will have no size bounds.
func NewMultiLevelList(name string, maxListSize int64) MultiLevelList {
	return &multiLevelList{
		name:         name,
		mapLists:     make(map[int]*list.List),
		highestLevel: math.MinInt32,
		limit:        maxListSize,
	}
}

// Push method adds taskItem to MultiLevel List
// it takes input as the level and value as interface{}
func (p *multiLevelList) Push(level int, element interface{}) error {
	// TODO: We need to optimize the locking
	// TODO: Need to take RLock on Map and Exclusive lock on individual list

	p.Lock()
	defer p.Unlock()
	if p.limit >= 0 && p.limit <= int64(p.size()) {
		return fmt.Errorf("list size limit reached")
	}
	if val, ok := p.mapLists[level]; ok {
		val.PushBack(element)
	} else {
		pList := list.New()
		pList.PushBack(element)
		p.mapLists[level] = pList
	}
	if level > p.highestLevel {
		p.highestLevel = level
	}
	return nil
}

// PushList method adds list to MultiLevel List
// it takes input as the level and list
func (p *multiLevelList) PushList(level int, newlist *list.List) error {
	// TODO: We need to optimize the locking
	// TODO: Need to take RLock on Map and Excusive lock on individual list
	p.Lock()
	defer p.Unlock()
	if p.limit >= 0 && p.limit < int64(p.size()+newlist.Len()) {
		return fmt.Errorf("list size limit reached")
	}
	if val, ok := p.mapLists[level]; ok {
		val.PushBackList(newlist)
	} else {
		pList := list.New()
		pList.PushBackList(newlist)
		p.mapLists[level] = pList
	}
	if level > p.highestLevel {
		p.highestLevel = level

	}
	return nil
}

// Pop method removes the Front Item for the given Level
func (p *multiLevelList) Pop(level int) (interface{}, error) {
	p.Lock()
	defer p.Unlock()
	if val, ok := p.mapLists[level]; ok {
		e := val.Front().Value
		val.Remove(val.Front())
		if val.Len() == 0 {
			delete(p.mapLists, level)
			p.highestLevel = p.calculateHighestLevel()
		}
		return e, nil
	}
	err := ErrorQueueEmpty(fmt.Sprintf("No items found in queue for priority %d", level))
	return nil, err
}

// PeekItem method returns the the Front Item for the given Level
// It will not remove the item from the list
func (p *multiLevelList) PeekItem(level int) (interface{}, error) {
	p.RLock()
	defer p.RUnlock()
	if val, ok := p.mapLists[level]; ok {
		e := val.Front().Value
		return e, nil
	}
	err := ErrorQueueEmpty(fmt.Sprintf("No items found in queue for priority %d", level))
	return nil, err
}

// PeekItems returns a list of items from a given level.
// The number of items returns is min(limit, number of items in a given level).
// Its returns an error if there are no items in the level.
// It does not remove the items from the list.
func (p *multiLevelList) PeekItems(level int, limit int) ([]interface{}, error) {
	p.RLock()
	defer p.RUnlock()
	var elements []interface{}

	val, ok := p.mapLists[level]
	if !ok {
		return elements, ErrorQueueEmpty(fmt.Sprintf("No items found in queue for priority %d", level))
	}

	for e := val.Front(); e != nil; e = e.Next() {
		if e == nil {
			continue
		}
		elements = append(elements, e.Value)
		if len(elements) == limit {
			// we've reached as much as we wanted to
			return elements, nil
		}
	}
	return elements, nil
}

// Remove method removes the specified item from multilevel list
// It takes level and the value as the input parameter
// If we have duplicate entry added we will be removing the first entry in the list
// return value is error if there is any error in removing
func (p *multiLevelList) Remove(
	level int,
	value interface{}) error {
	p.Lock()
	defer p.Unlock()

	var itemRemoved = false
	if l, ok := p.mapLists[level]; ok {
		for e := l.Front(); e != nil; e = e.Next() {
			if e.Value == value {
				l.Remove(e)
				itemRemoved = true
				break
			}
		}
	}

	if itemRemoved == false {
		err := ErrorQueueEmpty(fmt.Sprintf("No items found in queue %s", value))
		return err
	}
	if p.mapLists[level].Len() == 0 {
		delete(p.mapLists, level)
		p.highestLevel = p.calculateHighestLevel()
	}
	return nil
}

// RemoveItems method removes the specified items from multilevel list
// First return value is true/false -> Success/failure
// Second return value is item list not able to be deleted
// error is third return value
// This function returns false if only subset of the items being deleted
func (p *multiLevelList) RemoveItems(
	values map[interface{}]bool,
	level int) (bool, map[interface{}]bool, error) {
	newValuesMap := make(map[interface{}]bool)
	for k, v := range values {
		newValuesMap[k] = v
	}
	p.Lock()
	defer p.Unlock()

	if l, ok := p.mapLists[level]; ok {
		var next *list.Element
		for e := l.Front(); e != nil; e = next {
			eValue := e.Value
			next = e.Next()
			if _, ok := values[eValue]; ok {
				l.Remove(e)
				delete(newValuesMap, eValue)
			}
		}
	}

	if len(newValuesMap) != 0 {
		err := ErrorQueueEmpty("No items found in queue for given level")
		return false, newValuesMap, err
	}
	if p.mapLists[level].Len() == 0 {
		delete(p.mapLists, level)
		p.highestLevel = p.calculateHighestLevel()
	}
	return true, newValuesMap, nil
}

// IsEmpty method checks if the list for specified level is empty in multilevel list
func (p *multiLevelList) IsEmpty(level int) bool {
	p.RLock()
	defer p.RUnlock()
	if val, ok := p.mapLists[level]; ok {
		if val.Len() != 0 {
			return false
		}
	}
	return true
}

// Levels returns all the current levels in the list.
func (p *multiLevelList) Levels() []int {
	p.RLock()
	defer p.RUnlock()
	levels := make([]int, 0, len(p.mapLists))
	for level := range p.mapLists {
		levels = append(levels, level)
	}
	sort.Ints(levels)
	return levels
}

// Len returns the number of items in multilevel list for specified level.
func (p *multiLevelList) Len(level int) int {
	p.RLock()
	defer p.RUnlock()
	if val, ok := p.mapLists[level]; ok {
		return val.Len()
	}
	return 0
}

func (p *multiLevelList) size() int {
	len := 0
	for _, val := range p.mapLists {
		len += val.Len()
	}
	return len
}

// Size returns the total number of items in multilevel list for all levels
func (p *multiLevelList) Size() int {
	p.RLock()
	defer p.RUnlock()
	return p.size()
}

// calculateHighestLevel returns highest level in the multilevel list
func (p *multiLevelList) calculateHighestLevel() int {
	// TODO: we also can use heap for index to scan it Which can help getting the highest level in in O(1)
	// We will try to find if there is any regression then we will move with heap approach
	level := math.MinInt32
	for key := range p.mapLists {
		if key > level {
			level = key
		}
	}
	return level
}

// GetHighestLevel returns the highest level from the list
func (p *multiLevelList) GetHighestLevel() int {
	p.RLock()
	defer p.RUnlock()

	return p.highestLevel
}
