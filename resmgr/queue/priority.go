package queue

import (
	"errors"
	"sync"

	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
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

// Enqueue queues a gang (task list gang) based on its priority into FIFO queue
func (f *PriorityQueue) Enqueue(gang *resmgrsvc.Gang) error {
	f.Lock()
	defer f.Unlock()

	if f.count >= f.limit {
		return errors.New("queue Limit is reached")
	}
	if (gang == nil) || (len(gang.Tasks) == 0) {
		return errors.New("enqueue of empty list")
	}

	tasks := gang.GetTasks()
	priority := tasks[0].Priority
	f.list.Push(int(priority), gang)
	f.count++
	return nil
}

// Dequeue dequeues the gang (task list gang) based on the priority and order
// they came into the queue
func (f *PriorityQueue) Dequeue() (*resmgrsvc.Gang, error) {
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

	res := item.(*resmgrsvc.Gang)
	f.count--
	return res, nil
}

// Peek peeks the gang(list) based on the priority and order
// they came into the queue
// It will return an error if there is no gang in the queue
func (f *PriorityQueue) Peek() (*resmgrsvc.Gang, error) {
	// TODO: optimize the write lock here with potential read lock
	f.Lock()
	defer f.Unlock()

	highestPriority := f.list.GetHighestLevel()
	item, err := f.list.PeekItem(highestPriority)
	if err != nil {
		// TODO: Need to add test case for this case
		for highestPriority != f.list.GetHighestLevel() {
			highestPriority = f.list.GetHighestLevel()
			item, err = f.list.PeekItem(highestPriority)
			if err == nil {
				break
			}
		}
		if err != nil {
			return nil, err
		}
	}
	if item == nil {
		return nil, errors.New("Peek failed")
	}
	res := item.(*resmgrsvc.Gang)
	return res, nil
}

// Remove removes the item from the queue
func (f *PriorityQueue) Remove(gang *resmgrsvc.Gang) error {
	f.Lock()
	defer f.Unlock()
	if gang == nil || len(gang.Tasks) <= 0 {
		return errors.New("removal of empty list")
	}

	firstItem := gang.Tasks[0]
	priority := firstItem.Priority
	log.WithFields(log.Fields{
		"ITEM ":    firstItem,
		"Priority": priority,
	}).Debug("Trying to remove")
	return f.list.Remove(int(priority), gang)
}

// Len returns the length of the queue for specified priority
func (f *PriorityQueue) Len(priority int) int {
	return f.list.Len(priority)
}

// Size returns the number of elements in the PriorityQueue
func (f *PriorityQueue) Size() int {
	return f.list.Size()
}
