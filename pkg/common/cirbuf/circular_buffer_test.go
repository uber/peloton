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
	"github.com/stretchr/testify/assert"
	"testing"
)

type event struct {
	value int
}

func TestCBBasic(t *testing.T) {
	cbSize := 5
	cb := NewCircularBuffer(cbSize)
	for i := 0; i < cb.Capacity(); i++ {
		item, err := cb.AddItem(event{value: i})
		assert.Nil(t, err)
		assert.Equal(t, i, int(item.SequenceID))
		assert.Equal(t, i+1, int(cb.head))
	}
	head := cb.head
	tail := cb.tail
	// If buffer is full, cannot add more items
	assert.Equal(t, cb.Capacity(), int(cb.head))
	_, err := cb.AddItem(event{value: -1})
	assert.NotNil(t, err)
	assert.Equal(t, head, cb.head)
	assert.Equal(t, tail, cb.tail)

	// remove some items by move tail
	newTailPos := uint64(3)
	removedItems, err := cb.MoveTail(newTailPos)
	assert.Equal(t, int(newTailPos), len(removedItems))
	for i := 0; i < int(newTailPos); i++ {
		assert.Equal(t, i, int(removedItems[i].SequenceID))
	}
	// add more items
	items := 3
	for i := 0; i < items; i++ {
		item, err := cb.AddItem(event{value: i})
		assert.Nil(t, err)
		assert.Equal(t, i+cb.Capacity(), int(item.SequenceID))
		assert.Equal(t, i+cb.Capacity()+1, int(cb.head))
	}
}

// Continuesly adding a lot of events into the circular buffer, also trimming data from it
// Make sure that all events can be read correctly even through the buffer has been roll over
// many times
func TestCBContinueAddItems(t *testing.T) {
	cbSize := 37
	items := 1000000
	cb := NewCircularBuffer(cbSize)
	var removedItems []*CircularBufferItem
	for i := 0; i < items; i++ {
		item, err := cb.AddItem(event{value: i})
		assert.Nil(t, err)
		assert.Equal(t, i, int(item.SequenceID))
		assert.Equal(t, i+1, int(cb.head))

		// Check the new added item and its previous item
		previousItem, err := cb.GetItem(uint64(i))
		assert.Nil(t, err)
		assert.Equal(t, i, int(previousItem.SequenceID))
		assert.Equal(t, i, int(previousItem.Value.(event).value))
		if i > 0 {
			previousItem, err = cb.GetItem(uint64(i - 1))
			assert.Nil(t, err)
			assert.Equal(t, i-1, int(previousItem.SequenceID))
			assert.Equal(t, i-1, int(previousItem.Value.(event).value))
		}
		// If the buffer is close to full, remove part of its content by MoveTail
		if cb.Capacity()-cb.Size() < 10 {
			removed, err := cb.MoveTail(cb.head - 10)
			assert.Nil(t, err)
			removedItems = append(removedItems, removed...)
		}
	}
	// remove and track
	removed, err := cb.MoveTail(cb.head)
	assert.Nil(t, err)
	assert.Equal(t, 0, cb.Size())
	removedItems = append(removedItems, removed...)

	// All removed items should match all the items
	assert.Equal(t, items, len(removedItems))
	for i := 0; i < items; i++ {
		assert.Equal(t, i, int(removedItems[i].SequenceID))
		assert.Equal(t, i, int(removedItems[i].Value.(event).value))
	}
}

func TestCBGetItemsByRange(t *testing.T) {
	cb := NewCircularBuffer(8)
	items, err := cb.GetItemsByRange(uint64(0), uint64(0))
	assert.Nil(t, err)
	assert.Equal(t, 0, len(items))
	items, err = cb.GetItemsByRange(uint64(0), uint64(100))
	assert.Nil(t, err)
	assert.Equal(t, 0, len(items))

	// fill all buffer
	for i := 0; i < cb.Capacity(); i++ {
		_, err := cb.AddItem(event{value: i})
		assert.Nil(t, err)
	}
	// Normal case that from and to are both within buffer range
	from := 2
	to := 7
	items, err = cb.GetItemsByRange(uint64(from), uint64(to))
	assert.Nil(t, err)
	assert.Equal(t, len(items), to-from+1)
	for i := from; i <= to; i++ {
		assert.Equal(t, i, int(items[i-from].SequenceID))
		assert.Equal(t, i, int(items[i-from].Value.(event).value))
	}

	// to is larger than tail
	from = 4
	to = 9
	items, err = cb.GetItemsByRange(uint64(from), uint64(to))
	assert.Nil(t, err)
	assert.Equal(t, 4, len(items))
	for i := 0; i < len(items); i++ {
		assert.Equal(t, i+4, int(items[i].SequenceID))
		assert.Equal(t, i+4, int(items[i].Value.(event).value))
	}

	// from is less than tail
	cb.MoveTail(3)
	from = 0
	to = 6
	items, err = cb.GetItemsByRange(uint64(from), uint64(to))
	assert.Nil(t, err)
	assert.Equal(t, 4, len(items))
	for i := 0; i < len(items); i++ {
		assert.Equal(t, i+3, int(items[i].SequenceID))
		assert.Equal(t, i+3, int(items[i].Value.(event).value))
	}
	// Add more data and make circular buffer rollover
	cb.MoveTail(6)
	head := cb.head
	for i := 0; i < 5; i++ {
		_, err := cb.AddItem(event{value: i + int(head)})
		assert.Nil(t, err)
	}
	head, tail := cb.GetRange()
	assert.Equal(t, 13, int(head))
	assert.Equal(t, 6, int(tail))

	from = 6
	to = 11
	items, err = cb.GetItemsByRange(uint64(from), uint64(to))
	assert.Nil(t, err)
	assert.Equal(t, len(items), to-from+1)
	for i := from; i <= to; i++ {
		assert.Equal(t, i, int(items[i-from].SequenceID))
		assert.Equal(t, i, int(items[i-from].Value.(event).value))
	}
}
