package queue

import (
	"errors"
	"peloton/api/respool"
	"peloton/api/task"
)

// TaskItem is the struct holding task information
type TaskItem struct {
	TaskInfo *task.TaskInfo
	Priority int
	TaskID   string
}

// NewTaskItem creates the new task item for the task
func NewTaskItem(taskInfo *task.TaskInfo, priority int,
	taskID string) *TaskItem {
	taskItem := TaskItem{
		TaskInfo: taskInfo,
		Priority: priority,
		TaskID:   taskID,
	}
	return &taskItem
}

// Queue is the interface implemented by all the the queues
type Queue interface {
	Enqueue(task *TaskItem) error
	Dequeue() (*TaskItem, error)
}

// CreateQueue is factory method to create the specified queue
func CreateQueue(policy respool.SchedulingPolicy, limit int64) (Queue, error) {
	// Factory method to create specific queue object based on policy
	switch policy {
	case respool.SchedulingPolicy_PriorityFIFO:
		return NewPriorityQueue(limit), nil
	default:
		//if type is invalid, return an error
		return nil, errors.New("Invalid queue Type")
	}
}
