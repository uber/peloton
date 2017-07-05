package goalstate

import (
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
)

// CreateJMTask creates the JM task from task info.
func CreateJMTask(taskInfo *task.TaskInfo) (*JMTask, error) {
	return &JMTask{
		task: taskInfo,
	}, nil
}

// JMTask is the wrapper around task info for state machine
type JMTask struct {
	task *task.TaskInfo
	// TODO: add statemachine into JM task for state/goalstate transition.
}
