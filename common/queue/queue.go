package queue

import (
	"fmt"
	"reflect"
	"time"
)

// DequeueTimeOutError represents the error that the dequeue max wait time expired.
type DequeueTimeOutError struct {
	wait time.Duration
}

func (d DequeueTimeOutError) Error() string {
	return fmt.Sprintf("Dequeue max wait time expired: %s", d.wait)
}

// Queue defines the interface of an item queue
type Queue interface {
	GetName() string
	GetItemType() reflect.Type
	Enqueue(item interface{}) error
	Dequeue(maxWaitTime time.Duration) (interface{}, error)
	Length() int
}

// queue implements the Queue interface using go channel
type queue struct {
	channel  chan interface{}
	name     string
	itemType reflect.Type
}

// NewQueue creates a new in-memory queue instance
func NewQueue(name string, itemType reflect.Type, maxQueueSize uint32) Queue {
	q := queue{
		name:     name,
		itemType: itemType,
		channel:  make(chan interface{}, maxQueueSize),
	}
	return &q
}

// GetName returns the name of the queue
func (q *queue) GetName() string {
	return q.name
}

// GetItemType returns the type of the items in the queue
func (q *queue) GetItemType() reflect.Type {
	return q.itemType
}

// Enqueue adds an item into the queue
func (q *queue) Enqueue(item interface{}) error {
	itemType := reflect.Indirect(reflect.ValueOf(item)).Type()
	if itemType != q.itemType {
		return fmt.Errorf("Invalid item type, expected: %v, actual: %v",
			q.itemType, itemType)
	}

	select {
	case q.channel <- item:
		return nil
	default:
		return fmt.Errorf("Out of max queue size")
	}
}

// Dequeue pops out an item from the queue. Will be blocked for
// maxWaitTime if the queue is empty.
func (q *queue) Dequeue(maxWaitTime time.Duration) (interface{}, error) {
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(maxWaitTime)
		timeout <- true
	}()
	select {
	case item := <-q.channel:
		return item, nil
	case <-timeout:
		return nil, DequeueTimeOutError{maxWaitTime}
	}
}

// Length returns the length of the queue at any time
func (q *queue) Length() int {
	return len(q.channel)
}
