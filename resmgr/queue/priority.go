package queue

import (
	"errors"
	"peloton/private/resmgr"
	"sync"

	log "github.com/Sirupsen/logrus"
)

// PriorityQueue is FIFO queue which remove the highest priority task item entered first in the queue
type PriorityQueue struct {
	sync.RWMutex
	list *MultiLevelList
	// limt is the limit of the priority queue
	limit int64
	// count is the running count of the items
	count int64
}

// NewPriorityQueue intializes the fifo queue and returns the pointer
func NewPriorityQueue(limit int64) *PriorityQueue {
	fq := PriorityQueue{
		list:  NewMultiLevelList(),
		limit: limit,
		count: 0,
	}
	return &fq
}

// Enqueue queues the task based on the priority in FIFO queue
func (f *PriorityQueue) Enqueue(ti *resmgr.Task) error {
	f.Lock()
	defer f.Unlock()
	if f.count >= f.limit {
		err := errors.New("Queue Limit is reached")
		return err
	}
	f.list.Push(int(ti.Priority), ti)
	f.count++
	return nil
}

// Dequeue dequeues the task based on the priority and order they came into the queue
func (f *PriorityQueue) Dequeue() (*resmgr.Task, error) {
	// TODO: optimize the write lock here with potential read lock
	f.Lock()
	defer f.Unlock()

	highestPriority := f.list.GetHighestLevel()
	item, err := f.list.Pop(highestPriority)
	if err != nil {
		// TODO: Need to add test case for this case
		for highestPriority != f.list.GetHighestLevel() {
			highestPriority = f.list.GetHighestLevel()
			item, err = f.list.Pop(highestPriority)
			if err == nil {
				break
			}
		}
		return &resmgr.Task{}, err
	}
	if item == nil {
		log.Errorf("Dequeue Failed")
		return &resmgr.Task{}, err
	}
	res := item.(*resmgr.Task)
	f.count--
	return res, nil
}

// Len returns the length of the queue for specified priority
func (f *PriorityQueue) Len(priority int) int {
	return f.list.Len(priority)
}
