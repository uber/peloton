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

// maxMaintenanceQueueSize is the max size of the maintenance queue.
const (
	maintenanceQueueName    = "maintenance-queue"
	maxMaintenanceQueueSize = 10000
	defaultDequeueTimeout   = 100 * time.Millisecond
)

type maintenanceQueue struct {
	lock    sync.RWMutex
	queue   queue.Queue
	hostSet stringset.StringSet // Set containing hosts currently in maintenance queue
}

// MaintenanceQueue is the interface for maintenance queue.
type MaintenanceQueue interface {
	// Enqueue enqueues a list of hostnames into the maintenance queue
	Enqueue(hostnames []string) error
	// Dequeue dequeues a hostname from the maintenance queue
	Dequeue(maxWaitTime time.Duration) (string, error)
	// Length returns the length of maintenance queue at any time
	Length() int
	// Clear contents of maintenance queue
	Clear()
}

// NewMaintenanceQueue returns an instance of the maintenance queue
func NewMaintenanceQueue() MaintenanceQueue {
	return &maintenanceQueue{
		queue: queue.NewQueue(
			maintenanceQueueName,
			reflect.TypeOf(""),
			maxMaintenanceQueueSize),
		hostSet: stringset.New(),
	}
}

// Enqueue enqueues a list of hostnames into the maintenance queue
func (mq *maintenanceQueue) Enqueue(hostnames []string) error {
	mq.lock.Lock()
	defer mq.lock.Unlock()

	var errs error
	for _, host := range hostnames {
		if mq.hostSet.Contains(host) {
			log.
				WithField("host", host).
				Debug("Skipping enqueue. Host already present in maintenance queue.")
			continue
		}
		err := mq.queue.Enqueue(host)
		if err != nil {
			errs = multierr.Append(errs, err)
			continue
		}
		mq.hostSet.Add(host)
	}
	return errs
}

// Dequeue dequeues a hostname from the maintenance queue
func (mq *maintenanceQueue) Dequeue(maxWaitTime time.Duration) (string, error) {
	mq.lock.Lock()
	defer mq.lock.Unlock()

	item, err := mq.queue.Dequeue(maxWaitTime)
	if err != nil {
		if _, isTimeout := err.(queue.DequeueTimeOutError); !isTimeout {
			// error is not due to timeout so we log the error
			log.WithError(err).
				Error("unable to dequeue task from maintenance queue")
		}
		return "", err
	}

	host := item.(string)
	mq.hostSet.Remove(host)
	return host, nil
}

// Length returns the length of maintenance queue at any time
func (mq *maintenanceQueue) Length() int {
	mq.lock.RLock()
	defer mq.lock.RUnlock()

	return mq.queue.Length()
}

// Clear clears the contents of the maintenance queue
func (mq *maintenanceQueue) Clear() {
	mq.lock.Lock()
	defer mq.lock.Unlock()

	for len := mq.queue.Length(); len > 0; len-- {
		_, err := mq.queue.Dequeue(defaultDequeueTimeout)
		if err != nil {
			log.WithError(err).
				Error("unable to dequeue task from maintenance queue")
		}
	}
	log.Info("Maintenance queue cleared")
}
