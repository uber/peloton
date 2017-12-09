package models

import (
	"sync"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	"code.uber.internal/infra/peloton/mimir-lib/model/placement"
)

// NewTask will create a new placement task from a resource manager task and the gang it belongs to.
func NewTask(gang *resmgrsvc.Gang, task *resmgr.Task, deadline time.Time, maxRounds int) *Task {
	return &Task{
		gang:      gang,
		task:      task,
		deadline:  deadline,
		maxRounds: maxRounds,
	}
}

// Task represents a Peloton task, a Mimir placement entity can also be obtained from it.
type Task struct {
	gang      *resmgrsvc.Gang
	task      *resmgr.Task
	deadline  time.Time // The deadline when the task should successfully placed or have failed to do so.
	maxRounds int       // The maximal number of successful placement rounds.
	rounds    int       // The current number of successful placement rounds.
	entity    *placement.Entity
	lock      sync.Mutex
}

// Gang will return the resource manager gang that the task belongs to
func (task *Task) Gang() *resmgrsvc.Gang {
	return task.gang
}

// Task will return the resource manager task of the task.
func (task *Task) Task() *resmgr.Task {
	return task.task
}

// Entity will return the Mimir entity corresponding to the task.
func (task *Task) Entity() *placement.Entity {
	task.lock.Lock()
	defer task.lock.Unlock()
	if task.entity != nil {
		return task.entity
	}
	task.entity = TaskToEntity(task.task)
	return task.entity
}

// IncRounds will increment the number of placement rounds that the task have been through.
func (task *Task) IncRounds() {
	task.rounds++
}

// PastMaxRounds returns true iff the task has gone through its maximal number of placement rounds.
func (task *Task) PastMaxRounds() bool {
	return task.rounds >= task.maxRounds
}

// PastDeadline will return true iff the deadline for the gang have passed.
func (task *Task) PastDeadline(now time.Time) bool {
	return now.After(task.deadline)
}
