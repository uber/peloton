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

package cirbuf

import (
	"errors"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
)

// CircularBuffer is a circular buffer implementation
type CircularBuffer struct {
	sync.RWMutex
	buffer []*CircularBufferItem
	// head points to the next newest item to be added
	head uint64
	// tail points to the last available item
	tail uint64
}

// NewCircularBuffer creates an circular buffer with size bufferSize
func NewCircularBuffer(bufferSize int) *CircularBuffer {
	buffer := make([]*CircularBufferItem, bufferSize)
	return &CircularBuffer{
		buffer: buffer,
	}
}

// Capacity returns the total capacity of the circular buffer
func (c *CircularBuffer) Capacity() int {
	c.RLock()
	defer c.RUnlock()
	return len(c.buffer)
}

// Size returns the size used in the circular buffer
func (c *CircularBuffer) Size() int {
	c.RLock()
	defer c.RUnlock()
	return int(c.head - c.tail)
}

// GetItem returns the CircularBufferItem located by sequence id
func (c *CircularBuffer) GetItem(sequence uint64) (*CircularBufferItem, error) {
	c.RLock()
	defer c.RUnlock()
	if sequence < c.tail || sequence >= c.head {
		log.WithFields(log.Fields{
			"position": sequence,
			"head":     c.head,
			"tail":     c.tail}).
			Error("GetItem Index out of bound")
		return nil, errors.New("GetItem Index out of bound")
	}
	return c.buffer[int(sequence%uint64(len(c.buffer)))], nil
}

// GetItemsByRange returns the CircularBufferItems located by sequence id. Both from and to are inclusive
// This function is best effort, it returns partial content if the specified range overlap with the actual
// buffer range
func (c *CircularBuffer) GetItemsByRange(from uint64, to uint64) ([]*CircularBufferItem, error) {
	c.RLock()
	defer c.RUnlock()
	if from > c.head || to < c.tail {
		log.WithFields(log.Fields{
			"from": from,
			"to":   to,
			"head": c.head,
			"tail": c.tail}).
			Error("Index out of bound")
		return nil, errors.New("GetItemsByRange Index out of bound")
	}
	var items []*CircularBufferItem
	if from == c.head {
		return items, nil
	}
	if c.head == 0 {
		return items, nil
	}

	if from < c.tail {
		from = c.tail
	}
	if to >= c.head {
		to = c.head - 1
	}

	for i := from; i <= to; i++ {
		items = append(items, c.buffer[int(i%uint64(len(c.buffer)))])
	}
	return items, nil
}

// AddItem add a data record into the circular buffer, returns the CircularBufferItem created
func (c *CircularBuffer) AddItem(data interface{}) (*CircularBufferItem, error) {
	c.Lock()
	defer c.Unlock()
	if c.isFull() {
		log.WithFields(log.Fields{
			"head": c.head,
			"tail": c.tail}).
			Error("Buffer is full")
		return nil, fmt.Errorf("AddItem buffer is full")
	}
	item := &CircularBufferItem{
		SequenceID: c.head,
		Value:      data,
	}
	c.buffer[int((c.head)%uint64(len(c.buffer)))] = item
	c.head++
	return item, nil
}

// MoveTail increases the tail position, and return the list of CircularBufferItem moved out
func (c *CircularBuffer) MoveTail(newTail uint64) ([]*CircularBufferItem, error) {
	c.Lock()
	defer c.Unlock()
	if newTail > c.head || newTail < c.tail {
		log.WithFields(log.Fields{
			"head": c.head,
			"tail": c.tail}).
			Error("New tail value is out of range")
		return nil, fmt.Errorf("MoveTail new tail value is out of range")
	}
	var removedItems []*CircularBufferItem
	for i := c.tail; i < newTail; i++ {
		removedItems = append(removedItems, c.buffer[int(i%uint64(len(c.buffer)))])
	}
	c.tail = newTail
	return removedItems, nil
}

// Note: not thread safe and need to be called with lock
func (c *CircularBuffer) isFull() bool {
	return int(c.head-c.tail) >= len(c.buffer)
}

// GetRange returns the head / tail of the circular buffer
func (c *CircularBuffer) GetRange() (uint64, uint64) {
	c.RLock()
	defer c.RUnlock()
	return c.head, c.tail
}

// CircularBufferItem is the item stored in the circular buffer
type CircularBufferItem struct {
	SequenceID uint64
	Value      interface{}
}
