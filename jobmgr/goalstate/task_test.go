package goalstate

import (
	"fmt"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestTaskStateAndGoalState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)

	goalStateDriver := &driver{
		jobFactory: jobFactory,
		mtx:        NewMetrics(tally.NoopScope),
		cfg:        &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)
	taskID := fmt.Sprintf("%s-%d", jobID.GetValue(), instanceID)

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
		driver:     goalStateDriver,
	}

	taskState := cached.TaskStateVector{
		State:         pb_task.TaskState_RUNNING,
		ConfigVersion: 1,
	}
	taskGoalState := cached.TaskStateVector{
		State:         pb_task.TaskState_SUCCEEDED,
		ConfigVersion: 1,
	}

	// Test fetching the entity ID
	assert.Equal(t, taskID, taskEnt.GetID())

	// Test fetching the entity state
	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		CurrentState().Return(taskState)

	actState := taskEnt.GetState()
	assert.Equal(t, taskState, actState.(cached.TaskStateVector))

	// Test fetching the entity goal state
	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		GoalState().Return(taskGoalState)

	actGoalState := taskEnt.GetGoalState()
	assert.Equal(t, taskGoalState, actGoalState.(cached.TaskStateVector))

	// No cached task
	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(nil)

	actState = taskEnt.GetState()
	assert.Equal(t, pb_task.TaskState_UNKNOWN, actState.(cached.TaskStateVector).State)

	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(nil)

	actGoalState = taskEnt.GetGoalState()
	assert.Equal(t, pb_task.TaskState_UNKNOWN, actGoalState.(cached.TaskStateVector).State)
}

func TestTaskActionList(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
	}

	taskState := cached.TaskStateVector{
		State: pb_task.TaskState_RUNNING,
	}
	taskGoalState := cached.TaskStateVector{
		State: pb_task.TaskState_SUCCEEDED,
	}
	_, _, actions := taskEnt.GetActionList(taskState, taskGoalState)
	assert.Equal(t, 0, len(actions))

	taskState = cached.TaskStateVector{
		State: pb_task.TaskState_INITIALIZED,
	}
	taskGoalState = cached.TaskStateVector{
		State: pb_task.TaskState_SUCCEEDED,
	}
	_, _, actions = taskEnt.GetActionList(taskState, taskGoalState)
	assert.Equal(t, 1, len(actions))

	taskState = cached.TaskStateVector{
		State: pb_task.TaskState_UNKNOWN,
	}
	taskGoalState = cached.TaskStateVector{
		State: pb_task.TaskState_SUCCEEDED,
	}
	_, _, actions = taskEnt.GetActionList(taskState, taskGoalState)
	assert.Equal(t, 1, len(actions))
}

func TestEngineSuggestActionGoalKilled(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
	}

	a := taskEnt.suggestTaskAction(
		cached.TaskStateVector{State: pb_task.TaskState_KILLED, ConfigVersion: 0},
		cached.TaskStateVector{State: pb_task.TaskState_KILLED, ConfigVersion: 0})
	assert.Equal(t, KilledAction, a)

	a = taskEnt.suggestTaskAction(
		cached.TaskStateVector{State: pb_task.TaskState_KILLED, ConfigVersion: cached.UnknownVersion},
		cached.TaskStateVector{State: pb_task.TaskState_KILLED, ConfigVersion: 1})
	assert.Equal(t, KilledAction, a)

	a = taskEnt.suggestTaskAction(
		cached.TaskStateVector{State: pb_task.TaskState_RUNNING, ConfigVersion: 0},
		cached.TaskStateVector{State: pb_task.TaskState_KILLED, ConfigVersion: 0})
	assert.Equal(t, StopAction, a)

	a = taskEnt.suggestTaskAction(
		cached.TaskStateVector{State: pb_task.TaskState_RUNNING, ConfigVersion: 10},
		cached.TaskStateVector{State: pb_task.TaskState_KILLED, ConfigVersion: cached.UnknownVersion})
	assert.Equal(t, StopAction, a)
}

func TestEngineSuggestActionLaunchedState(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
	}

	a := taskEnt.suggestTaskAction(
		cached.TaskStateVector{State: pb_task.TaskState_LAUNCHED, ConfigVersion: 0},
		cached.TaskStateVector{State: pb_task.TaskState_SUCCEEDED, ConfigVersion: 0})
	assert.Equal(t, LaunchRetryAction, a)
}

func TestEngineSuggestActionGoalRunning(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
	}

	a := taskEnt.suggestTaskAction(
		cached.TaskStateVector{State: pb_task.TaskState_RUNNING, ConfigVersion: 0},
		cached.TaskStateVector{State: pb_task.TaskState_RUNNING, ConfigVersion: 0})
	assert.Equal(t, NoTaskAction, a)

	a = taskEnt.suggestTaskAction(
		cached.TaskStateVector{State: pb_task.TaskState_RUNNING, ConfigVersion: 0},
		cached.TaskStateVector{State: pb_task.TaskState_RUNNING, ConfigVersion: 1})
	assert.Equal(t, StopAction, a)

	a = taskEnt.suggestTaskAction(
		cached.TaskStateVector{State: pb_task.TaskState_KILLED, ConfigVersion: 0},
		cached.TaskStateVector{State: pb_task.TaskState_RUNNING, ConfigVersion: 1})
	assert.Equal(t, NoTaskAction, a)

	a = taskEnt.suggestTaskAction(
		cached.TaskStateVector{State: pb_task.TaskState_INITIALIZED, ConfigVersion: cached.UnknownVersion},
		cached.TaskStateVector{State: pb_task.TaskState_RUNNING, ConfigVersion: 0})
	assert.Equal(t, StartAction, a)

	a = taskEnt.suggestTaskAction(
		cached.TaskStateVector{State: pb_task.TaskState_INITIALIZED, ConfigVersion: 123},
		cached.TaskStateVector{State: pb_task.TaskState_RUNNING, ConfigVersion: 123})
	assert.Equal(t, StartAction, a)

	a = taskEnt.suggestTaskAction(
		cached.TaskStateVector{State: pb_task.TaskState_KILLED, ConfigVersion: 0},
		cached.TaskStateVector{State: pb_task.TaskState_RUNNING, ConfigVersion: 0})
	assert.Equal(t, NoTaskAction, a)
}

func TestEngineSuggestActionGoalPreempting(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
	}

	tt := []struct {
		currentState pb_task.TaskState
		action       TaskAction
	}{
		{
			currentState: pb_task.TaskState_INITIALIZED,
			action:       StopAction,
		},
		{
			currentState: pb_task.TaskState_LAUNCHING,
			action:       StopAction,
		},
		{
			currentState: pb_task.TaskState_LAUNCHED,
			action:       StopAction,
		},
		{
			currentState: pb_task.TaskState_RUNNING,
			action:       StopAction,
		},
		{
			currentState: pb_task.TaskState_LOST,
			action:       PreemptAction,
		},
		{
			currentState: pb_task.TaskState_KILLED,
			action:       PreemptAction,
		},
	}

	var a TaskAction
	for _, test := range tt {
		a = taskEnt.suggestTaskAction(
			cached.TaskStateVector{State: test.currentState, ConfigVersion: 0},
			cached.TaskStateVector{State: pb_task.TaskState_PREEMPTING, ConfigVersion: 0})
		assert.Equal(t, test.action, a)
	}
}
