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
	"container/heap"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

type testQueueItem struct {
	value int
	i     int
}

func (i *testQueueItem) Index() int                     { return i.i }
func (i *testQueueItem) SetIndex(v int)                 { i.i = v }
func (i *testQueueItem) SetDeadline(deadline time.Time) {}
func (i *testQueueItem) Deadline() time.Time {
	if i.value == 0 {
		return time.Time{}
	}
	return time.Unix(int64(i.value), 0)
}
func (i *testQueueItem) IsScheduled() bool { return !i.Deadline().IsZero() }

func TestTimeoutQueueOrdering(t *testing.T) {
	mtx := NewQueueMetrics(tally.NoopScope)
	q := &deadlineQueue{
		pq:           &priorityQueue{},
		queueChanged: make(chan struct{}, 1),
		mtx:          mtx,
	}
	heap.Init(q.pq)

	i1 := &testQueueItem{1, -1}
	i2 := &testQueueItem{2, -1}
	i3 := &testQueueItem{3, -1}
	i4 := &testQueueItem{4, -1}

	expectOrder := func(expect []*testQueueItem, input []*testQueueItem, updates ...func()) {
		i1.value = 1
		i2.value = 2
		i3.value = 3
		i4.value = 4

		for _, i := range input {
			q.update(i)
		}

		for _, f := range updates {
			f()
		}

		for _, v := range expect {
			p := q.popIfReady().(*testQueueItem)
			assert.Equal(t, v.value, p.value)
		}
	}

	expectOrder([]*testQueueItem{i1, i2, i3, i4}, []*testQueueItem{i1, i2, i3, i4})
	expectOrder([]*testQueueItem{i1, i2, i3, i4}, []*testQueueItem{i4, i3, i2, i1})
	expectOrder([]*testQueueItem{i1, i3, i4, i2}, []*testQueueItem{i1, i2, i3, i4},
		func() {
			i2.value = 5
			q.update(i2)
		})
	expectOrder([]*testQueueItem{i2, i3, i1, i4}, []*testQueueItem{i1, i2, i3, i4},
		func() {
			i1.value = 3
			q.update(i1)
		})
	expectOrder([]*testQueueItem{i1, i3, i4}, []*testQueueItem{i1, i2, i3, i4},
		func() {
			i2.value = 0
			q.update(i2)
			assert.Equal(t, -1, i2.Index())
		})
	expectOrder([]*testQueueItem{}, []*testQueueItem{},
		func() {
			i2.value = 0
			q.update(i2)
		})
}

// TestEnqueueAndDequeueTasks tests the normal deadline queue operation
// where one thread dequeues items and multiple threads can enqueue.
func TestEnqueueAndDequeueTasks(t *testing.T) {
	q := NewDeadlineQueue(NewQueueMetrics(tally.NoopScope))

	c := 100
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		for i := 0; i < c; i++ {
			tt := q.Dequeue(nil)
			assert.NotNil(t, tt)
		}
		wg.Done()
	}()

	for i := 0; i < c; i++ {
		go func() {
			tt := NewItem(strconv.Itoa(1))
			q.Enqueue(tt, time.Now())
		}()
	}

	wg.Wait()
}

// TestTimerChannel tests enqueuing one item and then dequeuing it.
func TestTimerChannel(t *testing.T) {
	q := NewDeadlineQueue(NewQueueMetrics(tally.NoopScope))
	c := 100

	for i := 0; i < c; i++ {
		tt := NewItem(strconv.Itoa(i))
		q.Enqueue(tt, time.Now())
	}

	for i := 0; i < c; i++ {
		tt := q.Dequeue(nil)
		assert.NotNil(t, tt)
	}
}

// TestStopChannel tests stopping the deadline queue using the stop channel.
func TestStopChannel(t *testing.T) {
	q := NewDeadlineQueue(NewQueueMetrics(tally.NoopScope))

	stopChan := make(chan struct{})
	go q.Dequeue(stopChan)

	tt := NewItem(strconv.Itoa(1))
	q.Enqueue(tt, time.Now())

	close(stopChan)
}
