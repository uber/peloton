package queue

import (
	"container/list"
	"fmt"
	"math"
	"sort"
	"sync"
)

// MultiLevelList struct holds all the lists with different Level, and a limit
// on total size of the list. If the limit is negative there is no size limit
// on the list.
// The Multi level list is implemented using a map[Level]List.
// Which holds list per Level.
// Push operation is O(1)
// Pop Operation is O(1)
// Remove Operation is O(m) where m is the list size of specified level
// RemoveItems Operation is O(m) where m is the list size of specified level
type MultiLevelList struct {
	sync.RWMutex
	name         string
	mapLists     map[int]*list.List
	highestLevel int
	limit        int64
}

// NewMultiLevelList initializes the multi level list with a name and a max
// size, if the max size is negative, then the list will have no size bounds.
func NewMultiLevelList(name string, maxListSize int64) *MultiLevelList {
	pm := MultiLevelList{
		name:         name,
		mapLists:     make(map[int]*list.List),
		highestLevel: math.MinInt32,
		limit:        maxListSize,
	}
	return &pm
}

// GetName returns the name of the multi level list.
func (p *MultiLevelList) GetName() string {
	return p.name
}

// Push method adds taskItem to MultiLevel List
// it takes input as the level and value as interface{}
func (p *MultiLevelList) Push(level int, element interface{}) error {
	// TODO: We need to optimize the locking
	// TODO: Need to take RLock on Map and Excusive lock on individual list

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
func (p *MultiLevelList) PushList(level int, newlist *list.List) error {
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
func (p *MultiLevelList) Pop(level int) (interface{}, error) {
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
	err := fmt.Errorf("No items found in queue for priority %d", level)
	return nil, err
}

// PeekItem method returns the the Front Item for the given Level
// It will not remove the item from the list
func (p *MultiLevelList) PeekItem(level int) (interface{}, error) {
	p.Lock()
	defer p.Unlock()
	if val, ok := p.mapLists[level]; ok {
		e := val.Front().Value
		return e, nil
	}
	err := fmt.Errorf("No items found in queue for priority %d", level)
	return nil, err
}

// Remove method removes the specified item from multilevel list
// It takes level and the value as the input parameter
// If we have duplicate entry added we will be removing the first entry in the list
// return value is error if there is any error in removing
func (p *MultiLevelList) Remove(
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
		err := fmt.Errorf("No items found in queue %s", value)
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
func (p *MultiLevelList) RemoveItems(
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
		err := fmt.Errorf("No items found in queue for given level")
		return false, newValuesMap, err
	}
	if p.mapLists[level].Len() == 0 {
		delete(p.mapLists, level)
		p.highestLevel = p.calculateHighestLevel()
	}
	return true, newValuesMap, nil
}

// IsEmpty method checks if the list for specified level is empty in multilevel list
func (p *MultiLevelList) IsEmpty(level int) bool {
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
func (p *MultiLevelList) Levels() []int {
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
func (p *MultiLevelList) Len(level int) int {
	p.RLock()
	defer p.RUnlock()
	if val, ok := p.mapLists[level]; ok {
		return val.Len()
	}
	return 0
}

func (p *MultiLevelList) size() int {
	len := 0
	for _, val := range p.mapLists {
		len += val.Len()
	}
	return len
}

// Size returns the total number of items in multilevel list for all levels
func (p *MultiLevelList) Size() int {
	p.RLock()
	defer p.RUnlock()
	return p.size()
}

// calculateHighestLevel returns highest level in the multilevel list
func (p *MultiLevelList) calculateHighestLevel() int {
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
func (p *MultiLevelList) GetHighestLevel() int {
	p.RLock()
	defer p.RUnlock()

	return p.highestLevel
}
