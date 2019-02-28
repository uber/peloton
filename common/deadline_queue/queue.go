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
	"sync"
	"time"
)

// DeadlineQueue defines the interface of a deadline queue implementation.
// Items with a deadline can be enqueued, and when the deadline expires,
// dequeue operation will return back the item.
type DeadlineQueue interface {
	// Enqueue is used to enqueue a queue item with a deadline
	Enqueue(qi QueueItem, deadline time.Time)
	// Dequeue is a blocking call to wait for the next queue item
	// whose deadline expires.
	Dequeue(stopChan <-chan struct{}) QueueItem
}

// NewDeadlineQueue returns a deadline queue object.
func NewDeadlineQueue(mtx *QueueMetrics) DeadlineQueue {
	q := &deadlineQueue{
		pq:           &priorityQueue{},
		queueChanged: make(chan struct{}, 1),
		mtx:          mtx,
	}

	heap.Init(q.pq)

	return q
}

// deadlineQueue implemments the DeadlineQueue interface.
type deadlineQueue struct {
	sync.RWMutex // mutex to access objects in the deadline queue

	pq           *priorityQueue // a priority queue
	queueChanged chan struct{}  // channel to indicate queue has changed
	mtx          *QueueMetrics  // track queue metrics
}

func (q *deadlineQueue) nextDeadline() time.Time {
	if q.pq.Len() == 0 {
		return time.Time{}
	}

	return q.pq.NextDeadline()
}

func (q *deadlineQueue) popIfReady() QueueItem {
	if q.pq.Len() == 0 {
		return nil
	}

	qi := heap.Pop(q.pq).(QueueItem)
	q.mtx.queuePopDelay.Record(time.Since(qi.Deadline()))
	qi.SetDeadline(time.Time{})
	q.mtx.queueLength.Update(float64(q.pq.Len()))
	return qi
}

func (q *deadlineQueue) update(item QueueItem) {
	// Check if it's not in the queue.
	if item.Index() == -1 {
		if item.Deadline().IsZero() {
			// Should not be scheduled.
			return
		}

		heap.Push(q.pq, item)
		q.mtx.queueLength.Update(float64(q.pq.Len()))
		return
	}

	// It's in the queue. Remove if it should not be scheduled.
	if item.Deadline().IsZero() {
		heap.Remove(q.pq, item.Index())
		q.mtx.queueLength.Update(float64(q.pq.Len()))
		return
	}

	heap.Fix(q.pq, item.Index())
}

// Enqueue will be used to enqueue a queue item into a deadline queue
func (q *deadlineQueue) Enqueue(qi QueueItem, deadline time.Time) {
	q.Lock()
	defer q.Unlock()

	// Override only if deadline is earlier.
	if !qi.Deadline().IsZero() && !deadline.Before(qi.Deadline()) {
		return
	}

	qi.SetDeadline(deadline)
	q.update(qi)
	select {
	case q.queueChanged <- struct{}{}:
	default:
	}
}

// Dequeue will be used to dequeue the next queue item whose deadline expires.
// Currently this implementation works only when there is one thread which
// is dequeueing, though multiple threads can enqueue.
func (q *deadlineQueue) Dequeue(stopChan <-chan struct{}) QueueItem {
	for {
		q.RLock()
		deadline := q.nextDeadline()
		q.RUnlock()

		var timer *time.Timer
		var timerChan <-chan time.Time
		if !deadline.IsZero() {
			timer = time.NewTimer(time.Until(deadline))
			timerChan = timer.C
		}

		select {
		case <-timerChan:
			q.Lock()
			r := q.popIfReady()
			q.Unlock()

			if r != nil {
				return r
			}

		case <-q.queueChanged:
			// Wake up to process the next item in the queue

		case <-stopChan:
			return nil
		}

		if timer != nil {
			timer.Stop()
		}
	}
}
