package goalstate

import (
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"github.com/stretchr/testify/assert"
)

func TestSuggestAction(t *testing.T) {
	assert.Equal(t, _noAction, suggestAction(
		State{task.TaskState_RUNNING, 0},
		State{task.TaskState_RUNNING, 0},
	))

	assert.Equal(t, _stopAction, suggestAction(
		State{task.TaskState_RUNNING, 0},
		State{task.TaskState_RUNNING, 1},
	))

	assert.Equal(t, _stopAction, suggestAction(
		State{task.TaskState_RUNNING, 0},
		State{task.TaskState_KILLED, 0},
	))

	assert.Equal(t, _useGoalVersionAction, suggestAction(
		State{task.TaskState_KILLED, 0},
		State{task.TaskState_RUNNING, 1},
	))

	assert.Equal(t, _noAction, suggestAction(
		State{task.TaskState_KILLED, UnknownVersion},
		State{task.TaskState_RUNNING, 0},
	))

	assert.Equal(t, _noAction, suggestAction(
		State{task.TaskState_KILLED, 0},
		State{task.TaskState_RUNNING, UnknownVersion},
	))
}
