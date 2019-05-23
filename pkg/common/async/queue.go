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

package async

import (
	"container/list"
	"sync"
)

// Queue defines the interface of a queue used by the async pool
// to enqueue jobs and then dequeue the job when a worker becomes available
type Queue interface {
	// Run runs the Queue and will stop the Queue if the stopChan provided
	// is closed
	Run(stopChan chan struct{})
	// Enqueue is used to enqueue a job
	Enqueue(job Job)
	// Dequeue is used to fetch an enqueued job when a worker is available
	Dequeue() Job
}

// queue structure that works similar to an unlimited channel, where Jobs can be
// added using Enqueue and drained by reading from the DequeueChannel.
// TODO: This queue may be changed dramatically going forward, as the main
// purpose right now is to facilitate the Pool.
type queue struct {
	sync.Mutex
	// TODO: Consider using circular buffer, if memory overhead can be lowered.
	list *list.List

	// enqueueSignal is added to after a successful enqueue. By having a buffer
	// size of 1, it's guaranteed that the job is processed.
	enqueueSignal  chan struct{}
	dequeueChannel chan Job
}

// newQueue for enqueing Jobs.
func newQueue() *queue {
	q := &queue{
		list:          list.New(),
		enqueueSignal: make(chan struct{}, 1),
	}
	return q
}

func (q *queue) Run(stopChan chan struct{}) {
	q.dequeueChannel = make(chan Job)

	go func() {
		for {
			q.Lock()

			f := q.list.Front()
			if f == nil {
				q.Unlock()

				// Wait for jobs to be enqueued before continuing.
				<-q.enqueueSignal
				continue
			}

			q.list.Remove(f)
			q.Unlock()

			select {
			case q.dequeueChannel <- f.Value.(Job):
				continue
			case <-stopChan:
				close(q.dequeueChannel)
				return
			}

		}
	}()
}

// Enqueue the Job. This method will return immediately.
func (q *queue) Enqueue(job Job) {
	q.Lock()
	q.list.PushBack(job)
	q.Unlock()

	// Try signal a new items is available.
	select {
	case q.enqueueSignal <- struct{}{}:
	default:
	}
}

// Dequeue the Job.
func (q *queue) Dequeue() Job {
	return <-q.dequeueChannel
}
