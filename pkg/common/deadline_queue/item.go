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

package deadlinequeue

import (
	"sync"
	"time"
)

// QueueItem is the interface an item enqueued in the deadline queue needs to support.
type QueueItem interface {
	// IsScheduled returns true if the queue item is enqueued in the deadline queue.
	IsScheduled() bool
	// Deadline returns the deadline at which the queue item will be dequeued.
	Deadline() time.Time
	// SetDeadline sets the time at which the queue item will be dequeued.
	SetDeadline(deadline time.Time)

	// SetIndex sets the index of the queue item in the queue.
	SetIndex(index int)
	// Index fetches the index of the queue item in the queue.
	Index() int
}

// queueItemMixin is an implementation of the queue item in the
// deadline queue for the goal state scheduler.
type queueItemMixin struct {
	sync.RWMutex            // the mutex to synchronize access to this object
	queueIndex    int       // the index in the queue
	queueDeadline time.Time // the current deadline
}

func (i *queueItemMixin) Index() int {
	i.RLock()
	defer i.RUnlock()
	return i.queueIndex
}

func (i *queueItemMixin) SetIndex(index int) {
	i.Lock()
	defer i.Unlock()
	i.queueIndex = index
}

func (i *queueItemMixin) Deadline() time.Time {
	i.RLock()
	defer i.RUnlock()
	return i.queueDeadline
}

func (i *queueItemMixin) SetDeadline(deadline time.Time) {
	i.Lock()
	defer i.Unlock()
	i.queueDeadline = deadline
}

func (i *queueItemMixin) IsScheduled() bool {
	i.RLock()
	defer i.RUnlock()
	return !i.Deadline().IsZero()
}

// newQueueItemMixing returns a new queueItemMixin object.
func newQueueItemMixing() queueItemMixin {
	return queueItemMixin{queueIndex: -1}
}

// Item implements a queue item storing a string identifier.
type Item struct {
	queueItemMixin
	str string
}

// NewItem returns a newly created Item.
func NewItem(str string) *Item {
	return &Item{
		queueItemMixin: newQueueItemMixing(),
		str:            str,
	}
}

// GetString fetches the string identifier stored in Item.
func (dqi *Item) GetString() string {
	return dqi.str
}
