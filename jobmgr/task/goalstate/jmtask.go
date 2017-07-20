package goalstate

import (
	mesos_v1 "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
)

// JMTask provides an interface to converge task state to goal state.
type JMTask interface {
	ProcessStatusUpdate(status *mesos_v1.TaskStatus) error
}

// CreateJMTask creates the JM task from task info.
func CreateJMTask(taskInfo *task.TaskInfo) (JMTask, error) {
	return &jmTask{
		task: taskInfo,
	}, nil
}

// jmTask is the wrapper around task info for state machine
type jmTask struct {
	task *task.TaskInfo
	// TODO: add statemachine into JM task for state/goalstate transition.
}

// ProcessStatusUpdate takes latest status update then derives actions to converge
// task state to goalstate.
func (j *jmTask) ProcessStatusUpdate(status *mesos_v1.TaskStatus) error {
	// TODO: implement it.
	return nil
}
