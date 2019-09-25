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
	"reflect"
	"sync"
	"time"

	"github.com/uber/peloton/pkg/common/queue"
	"github.com/uber/peloton/pkg/common/stringset"

	log "github.com/sirupsen/logrus"
	"go.uber.org/multierr"
)

const (
	maxTaskQueueSize      = 100000
	defaultDequeueTimeout = 100 * time.Millisecond
)

type taskQueue struct {
	lock  sync.RWMutex
	queue queue.Queue
	// Set containing taskIDs of the tasks currently in task queue
	taskSet stringset.StringSet
}

// TaskQueue is the interface for task queue.
type TaskQueue interface {
	// Enqueue enqueues a list of hostnames into the task eviction queue.
	Enqueue(taskIDs []string) error
	// Dequeue dequeues a hostname from the task eviction queue.
	Dequeue(maxWaitTime time.Duration) (string, error)
	// Length returns the length of task eviction queue at that point in time.
	Length() int
	// Clear contents of task eviction queue.
	Clear()
}

// NewTaskQueue returns an instance of the task queue.
func NewTaskQueue(taskQueueName string) TaskQueue {
	return &taskQueue{
		queue: queue.NewQueue(
			taskQueueName,
			reflect.TypeOf(""),
			maxTaskQueueSize),
		taskSet: stringset.New(),
	}
}

// Enqueue enqueues a list of taskIDs into the task queue.
func (mq *taskQueue) Enqueue(taskIDs []string) error {
	mq.lock.Lock()
	defer mq.lock.Unlock()

	var errs error
	for _, t := range taskIDs {
		if mq.taskSet.Contains(t) {
			log.
				WithField("task_id", t).
				Debug("Skipping enqueue. Task already present in task queue.")
			continue
		}
		err := mq.queue.Enqueue(t)
		if err != nil {
			errs = multierr.Append(errs, err)
			continue
		}
		mq.taskSet.Add(t)
	}
	return errs
}

// Dequeue dequeues a hostname from the task queue.
func (mq *taskQueue) Dequeue(maxWaitTime time.Duration) (string, error) {
	mq.lock.Lock()
	defer mq.lock.Unlock()

	item, err := mq.queue.Dequeue(maxWaitTime)
	if err != nil {
		if _, isTimeout := err.(queue.DequeueTimeOutError); !isTimeout {
			// error is not due to timeout so we log the error
			log.WithError(err).
				Error("unable to dequeue task from task queue.")
		}
		return "", err
	}

	host := item.(string)
	mq.taskSet.Remove(host)
	return host, nil
}

// Length returns the current length of task eviction queue.
func (mq *taskQueue) Length() int {
	mq.lock.RLock()
	defer mq.lock.RUnlock()

	return mq.queue.Length()
}

// Clear clears the contents of the task queue.
func (mq *taskQueue) Clear() {
	mq.lock.Lock()
	defer mq.lock.Unlock()

	for i := mq.queue.Length(); i > 0; i-- {
		_, err := mq.queue.Dequeue(defaultDequeueTimeout)
		if err != nil {
			log.WithError(err).
				Error("unable to dequeue task from task queue.")
		}
	}
	log.Info("task queue cleared")
}
