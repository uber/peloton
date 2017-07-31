package goalstate

import (
	"context"
	"testing"
	"time"

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

func TestJMTaskUpdateGoalState(t *testing.T) {
	jmti, err := CreateJMTask(nil)
	assert.NotNil(t, jmti)
	assert.NoError(t, err)

	jmt := jmti.(*jmTask)

	before := time.Now()

	jmt.UpdateGoalState(&task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			GoalState:            task.TaskState_RUNNING,
			DesiredConfigVersion: 7,
		},
	})

	assert.Equal(t, State{task.TaskState_RUNNING, 7}, jmt.goalState)
	assert.True(t, jmt.goalStateTime.After(before))
}

func TestJMTaskProcessState(t *testing.T) {
	jmti, err := CreateJMTask(nil)
	assert.NotNil(t, jmti)
	assert.NoError(t, err)

	jmt := jmti.(*jmTask)

	before := time.Now()

	s := State{task.TaskState_RUNNING, 0}
	assert.NoError(t, jmt.ProcessState(context.Background(), nil, s))

	assert.Equal(t, s, jmt.lastState)
	assert.Equal(t, jmt.lastAction, _noAction)
	assert.True(t, jmt.lastActionTime.After(before))

	// Ensure no action attempted due to retry timeout.
	lt := jmt.lastActionTime
	assert.NoError(t, jmt.ProcessState(context.Background(), nil, State{task.TaskState_RUNNING, 0}))
	assert.Equal(t, lt, jmt.lastActionTime)
}
