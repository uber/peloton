package queue

import (
	"container/list"
	"errors"
	"sync"

	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
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

// Enqueue queues a gang (task list gang) based on its priority into FIFO queue
func (f *PriorityQueue) Enqueue(tlist *list.List) error {
	f.Lock()
	defer f.Unlock()

	if f.count >= f.limit {
		return errors.New("queue Limit is reached")
	}
	if tlist.Len() <= 0 {
		return errors.New("enqueue of empty list")
	}

	firstItem := tlist.Front()
	priority := firstItem.Value.(*resmgr.Task).Priority
	f.list.Push(int(priority), tlist)
	f.count++
	return nil
}

// Dequeue dequeues the gang (task list gang) based on the priority and order
// they came into the queue
func (f *PriorityQueue) Dequeue() (*list.List, error) {
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
		if err != nil {
			return nil, err
		}
	}
	if item == nil {
		return nil, errors.New("dequeue failed")
	}

	res := item.(*list.List)
	f.count--
	return res, nil
}

// Len returns the length of the queue for specified priority
func (f *PriorityQueue) Len(priority int) int {
	return f.list.Len(priority)
}
