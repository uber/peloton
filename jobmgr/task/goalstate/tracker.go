package goalstate

import (
	"fmt"
	"sync"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
)

// NewTracker returns a new task tracker.
func newTracker() *tracker {
	return &tracker{
		taskMap: make(map[string]*jmTask),
	}
}

// tracker is the jmtask tracker.
type tracker struct {
	sync.Mutex

	// taskMap is map from peloton task id -> jmtask.
	taskMap map[string]*jmTask
}

// addTask adds task to task tracker
func (tr *tracker) addTask(taskInfo *task.TaskInfo) (*jmTask, error) {
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

	t, err := newJMTask(taskInfo)
	if err != nil {
		return nil, err
	}
	tr.taskMap[pelotonTaskID] = t
	return t, nil
}

// getTask gets the JM task for taskID
func (tr *tracker) getTask(t *peloton.TaskID) *jmTask {
	tr.Lock()
	defer tr.Unlock()
	return tr.taskMap[t.Value]
}

// deleteTask removes the task from the map
func (tr *tracker) deleteTask(taskID *peloton.TaskID) error {
	tr.Lock()
	defer tr.Unlock()
	delete(tr.taskMap, taskID.Value)
	return nil
}

// removeAllDoneTasks removes all the tasks with state converged to goalstate.
func (tr *tracker) removeAllDoneTasks() error {
	return fmt.Errorf("unimplemented tracker.removeAllDoneTasks")
}
