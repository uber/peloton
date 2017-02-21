package queue

import (
	"container/list"
	"fmt"
	"peloton/api/task"
	"sync"
)

// TaskItem is the struct holding task information
type TaskItem struct {
	taskInfo task.TaskInfo
	priority int
	taskID   string
}

// NewTaskItem creates the new task item for the task
func NewTaskItem(
	taskInfo task.TaskInfo,
	priority int) *TaskItem {
	taskItem := TaskItem{
		taskInfo: taskInfo,
		priority: priority,
	}
	taskItem.taskID = fmt.Sprintf("%s-%d", taskInfo.JobId.Value, taskInfo.InstanceId)
	return &taskItem
}

// PriorityQueue struct holds all the priority queues
type PriorityQueue struct {
	queues map[int]*list.List
	sync.RWMutex
}

// InitPriorityQueue initializes the priority queue
func InitPriorityQueue() *PriorityQueue {
	pq := PriorityQueue{
		queues: make(map[int]*list.List),
	}
	return &pq
}

// Push method adds taskItem to priority queue
func (p *PriorityQueue) Push(t *TaskItem) (bool, error) {
	p.Lock()
	defer p.Unlock()
	if val, ok := p.queues[t.priority]; ok {
		val.PushBack(t)
	} else {
		pList := list.New()
		pList.PushBack(t)
		p.queues[t.priority] = pList
	}
	return true, nil
}

// Pop method removes the Front Item from the priority queue
func (p *PriorityQueue) Pop(priority int) (*TaskItem, error) {
	p.RLock()
	defer p.RUnlock()
	if val, ok := p.queues[priority]; ok {
		e := val.Front().Value
		t := e.(*TaskItem)
		val.Remove(val.Front())
		if val.Len() == 0 {
			delete(p.queues, priority)
		}
		return t, nil
	}
	return nil, nil
}

// Remove method removes the specified item from priority queue
func (p *PriorityQueue) Remove(t *TaskItem) (bool, error) {
	p.RLock()
	defer p.RUnlock()

	l := p.queues[t.priority]
	var itemRemoved = false
	for e := l.Front(); e != nil; e = e.Next() {
		if e.Value.(*TaskItem).taskID == t.taskID {
			l.Remove(e)
			itemRemoved = true
		}
	}

	if itemRemoved == false {
		err := fmt.Errorf("No items found in queue %s", t.taskID)
		return false, err
	}
	if p.queues[t.priority].Len() == 0 {
		delete(p.queues, t.priority)
	}
	return true, nil
}

// RemoveTasks method removes the specified items from priority queue
func (p *PriorityQueue) RemoveTasks(tasks map[string]*TaskItem, priority int) (bool, error) {
	newTasksMap := make(map[string]*TaskItem)
	for k, v := range tasks {
		newTasksMap[k] = v
	}

	p.RLock()
	defer p.RUnlock()

	l := p.queues[priority]
	var next *list.Element
	for e := l.Front(); e != nil; e = next {
		eValue := e.Value.(*TaskItem).taskID
		next = e.Next()
		if val, ok := tasks[eValue]; ok {
			if eValue == val.taskID {
				l.Remove(e)
				delete(newTasksMap, val.taskID)
			}
		}

	}
	if len(newTasksMap) != 0 {
		var tasklist string
		for k, v := range newTasksMap {
			tasklist += fmt.Sprintf(" [ %s ] => [ %d ] ", k, v.priority)
		}
		err := fmt.Errorf("No items found in queue for tasks and priority %s", tasklist)
		return false, err
	}
	if p.queues[priority].Len() == 0 {
		delete(p.queues, priority)
	}
	return true, nil
}

// IsEmpty method checks if the queue for specified priority is empty
func (p *PriorityQueue) IsEmpty(priority int) bool {
	p.Lock()
	defer p.Unlock()
	if val, ok := p.queues[priority]; ok {
		if val.Len() != 0 {
			return false
		}
	}
	return true
}

// Len returns the number of items in a priority queue for specified priority.
func (p *PriorityQueue) Len(priority int) int {
	p.Lock()
	defer p.Unlock()
	if val, ok := p.queues[priority]; ok {
		return val.Len()
	}
	return 0
}
