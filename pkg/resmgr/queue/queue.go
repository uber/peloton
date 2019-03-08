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
	"errors"

	"github.com/uber/peloton/.gen/peloton/api/v0/respool"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
)

// Queue is the interface implemented by all the the queues
type Queue interface {
	// Enqueue queues a gang (task list gang) based on its priority into FIFO queue
	Enqueue(gang *resmgrsvc.Gang) error
	// Dequeue dequeues the gang (task list gang) based on the priority and order
	// they came into the queue
	Dequeue() (*resmgrsvc.Gang, error)
	// Peek peeks the gang(list) based on the priority and order
	// they came into the queue.
	// limit is the number of gangs to peek.
	// It will return an error if there is no gang in the queue
	Peek(limit uint32) ([]*resmgrsvc.Gang, error)
	// Remove removes the item from the queue
	Remove(item *resmgrsvc.Gang) error
	// Size returns the total number of items in the queue
	Size() int
}

// CreateQueue is factory method to create the specified queue
func CreateQueue(policy respool.SchedulingPolicy, limit int64) (Queue, error) {
	// Factory method to create specific queue object based on policy
	switch policy {
	case respool.SchedulingPolicy_PriorityFIFO:
		return NewPriorityQueue(limit), nil
	default:
		//if type is invalid, return an error
		return nil, errors.New("invalid queue type")
	}
}
