package goalstate

import (
	"fmt"
	"sync"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
)

// Tracker is the interface for job manager to track/manage peloton task
// state machines for state/goalstate convergence.
type Tracker interface {
	// AddTask adds the task to state machine
	AddTask(taskInfo *task.TaskInfo) (JMTask, error)

	// GetTask gets the JM task for taskID
	GetTask(taskID *peloton.TaskID) JMTask

	// DeleteTask removes the task from the map
	DeleteTask(taskID *peloton.TaskID) error

	// Clean up all the tasks that have state successfully transited to goalstate.
	RemoveAllDoneTasks() error
}

// NewTracker returns a new task tracker.
func NewTracker() Tracker {
	return &tracker{
		taskMap: make(map[string]JMTask),
	}
}

// tracker is the jmtask tracker.
type tracker struct {
	sync.Mutex

	// taskMap is map from peloton task id -> jmtask.
	taskMap map[string]JMTask
}

// AddTask adds task to task tracker
func (tr *tracker) AddTask(taskInfo *task.TaskInfo) (JMTask, error) {
	tr.Lock()
	defer tr.Unlock()

	pelotonTaskID := fmt.Sprintf(
		"%s-%d",
		taskInfo.GetJobId().GetValue(), taskInfo.GetInstanceId())

	if t, ok := tr.taskMap[pelotonTaskID]; ok {
		// TODO(mu): need to check if state/goalstate has changed if
		// task is already in tracker.
		return t, nil
	}

	t, err := CreateJMTask(taskInfo)
	if err != nil {
		return nil, err
	}
	tr.taskMap[pelotonTaskID] = t
	return t, nil
}

// GetTask gets the JM task for taskID
func (tr *tracker) GetTask(t *peloton.TaskID) JMTask {
	tr.Lock()
	defer tr.Unlock()
	return tr.taskMap[t.Value]
}

// DeleteTask removes the task from the map
func (tr *tracker) DeleteTask(taskID *peloton.TaskID) error {
	tr.Lock()
	defer tr.Unlock()
	delete(tr.taskMap, taskID.Value)
	return nil
}

// RemoveAllDoneTasks removes all the tasks with state converged to goalstate.
func (tr *tracker) RemoveAllDoneTasks() error {
	return nil
}
