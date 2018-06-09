package goalstate

import (
	"fmt"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

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
		State:         pbtask.TaskState_RUNNING,
		ConfigVersion: 1,
	}
	taskGoalState := cached.TaskStateVector{
		State:         pbtask.TaskState_SUCCEEDED,
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
	assert.Equal(t, pbtask.TaskState_UNKNOWN, actState.(cached.TaskStateVector).State)

	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(nil)

	actGoalState = taskEnt.GetGoalState()
	assert.Equal(t, pbtask.TaskState_UNKNOWN, actGoalState.(cached.TaskStateVector).State)
}

func TestTaskActionList(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
	}

	taskState := cached.TaskStateVector{
		State: pbtask.TaskState_RUNNING,
	}
	taskGoalState := cached.TaskStateVector{
		State: pbtask.TaskState_SUCCEEDED,
	}
	_, _, actions := taskEnt.GetActionList(taskState, taskGoalState)
	assert.Equal(t, 0, len(actions))

	taskState = cached.TaskStateVector{
		State: pbtask.TaskState_INITIALIZED,
	}
	taskGoalState = cached.TaskStateVector{
		State: pbtask.TaskState_SUCCEEDED,
	}
	_, _, actions = taskEnt.GetActionList(taskState, taskGoalState)
	assert.Equal(t, 1, len(actions))

	taskState = cached.TaskStateVector{
		State: pbtask.TaskState_UNKNOWN,
	}
	taskGoalState = cached.TaskStateVector{
		State: pbtask.TaskState_SUCCEEDED,
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
		cached.TaskStateVector{State: pbtask.TaskState_RUNNING, ConfigVersion: 0},
		cached.TaskStateVector{State: pbtask.TaskState_KILLED, ConfigVersion: 0})
	assert.Equal(t, StopAction, a)

	a = taskEnt.suggestTaskAction(
		cached.TaskStateVector{State: pbtask.TaskState_RUNNING, ConfigVersion: 10},
		cached.TaskStateVector{State: pbtask.TaskState_KILLED, ConfigVersion: cached.UnknownVersion})
	assert.Equal(t, StopAction, a)

	a = taskEnt.suggestTaskAction(
		cached.TaskStateVector{State: pbtask.TaskState_STARTING, ConfigVersion: 10},
		cached.TaskStateVector{State: pbtask.TaskState_KILLED, ConfigVersion: cached.UnknownVersion})
	assert.Equal(t, StopAction, a)

	a = taskEnt.suggestTaskAction(
		cached.TaskStateVector{State: pbtask.TaskState_LAUNCHED, ConfigVersion: 10},
		cached.TaskStateVector{State: pbtask.TaskState_KILLED, ConfigVersion: cached.UnknownVersion})
	assert.Equal(t, StopAction, a)

	a = taskEnt.suggestTaskAction(
		cached.TaskStateVector{State: pbtask.TaskState_KILLING, ConfigVersion: 10},
		cached.TaskStateVector{State: pbtask.TaskState_KILLED, ConfigVersion: cached.UnknownVersion})
	assert.Equal(t, ExecutorShutdownAction, a)

	a = taskEnt.suggestTaskAction(
		cached.TaskStateVector{State: pbtask.TaskState_PREEMPTING, ConfigVersion: 10},
		cached.TaskStateVector{State: pbtask.TaskState_KILLED, ConfigVersion: cached.UnknownVersion})
	assert.Equal(t, TaskStateInvalidAction, a)
}

func TestEngineSuggestActionLaunchedState(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
	}

	a := taskEnt.suggestTaskAction(
		cached.TaskStateVector{State: pbtask.TaskState_LAUNCHED, ConfigVersion: 0},
		cached.TaskStateVector{State: pbtask.TaskState_SUCCEEDED, ConfigVersion: 0})
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
		cached.TaskStateVector{State: pbtask.TaskState_RUNNING, ConfigVersion: 0},
		cached.TaskStateVector{State: pbtask.TaskState_RUNNING, ConfigVersion: 0})
	assert.Equal(t, NoTaskAction, a)

	a = taskEnt.suggestTaskAction(
		cached.TaskStateVector{State: pbtask.TaskState_RUNNING, ConfigVersion: 0},
		cached.TaskStateVector{State: pbtask.TaskState_RUNNING, ConfigVersion: 1})
	assert.Equal(t, StopAction, a)

	a = taskEnt.suggestTaskAction(
		cached.TaskStateVector{State: pbtask.TaskState_KILLED, ConfigVersion: 0},
		cached.TaskStateVector{State: pbtask.TaskState_RUNNING, ConfigVersion: 1})
	assert.Equal(t, NoTaskAction, a)

	a = taskEnt.suggestTaskAction(
		cached.TaskStateVector{State: pbtask.TaskState_INITIALIZED, ConfigVersion: cached.UnknownVersion},
		cached.TaskStateVector{State: pbtask.TaskState_RUNNING, ConfigVersion: 0})
	assert.Equal(t, StartAction, a)

	a = taskEnt.suggestTaskAction(
		cached.TaskStateVector{State: pbtask.TaskState_INITIALIZED, ConfigVersion: 123},
		cached.TaskStateVector{State: pbtask.TaskState_RUNNING, ConfigVersion: 123})
	assert.Equal(t, StartAction, a)

	a = taskEnt.suggestTaskAction(
		cached.TaskStateVector{State: pbtask.TaskState_STARTING, ConfigVersion: 0},
		cached.TaskStateVector{State: pbtask.TaskState_RUNNING, ConfigVersion: 0})
	assert.Equal(t, LaunchRetryAction, a)

	a = taskEnt.suggestTaskAction(
		cached.TaskStateVector{State: pbtask.TaskState_LAUNCHED, ConfigVersion: 0},
		cached.TaskStateVector{State: pbtask.TaskState_RUNNING, ConfigVersion: 0})
	assert.Equal(t, LaunchRetryAction, a)

	a = taskEnt.suggestTaskAction(
		cached.TaskStateVector{State: pbtask.TaskState_PREEMPTING, ConfigVersion: 0},
		cached.TaskStateVector{State: pbtask.TaskState_RUNNING, ConfigVersion: 0})
	assert.Equal(t, TaskStateInvalidAction, a)

	a = taskEnt.suggestTaskAction(
		cached.TaskStateVector{State: pbtask.TaskState_KILLING, ConfigVersion: 0},
		cached.TaskStateVector{State: pbtask.TaskState_RUNNING, ConfigVersion: 0})
	assert.Equal(t, TaskStateInvalidAction, a)
}

func TestEngineSuggestActionGoalPreempting(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
	}

	tt := []struct {
		currentState pbtask.TaskState
		action       TaskAction
	}{
		{
			currentState: pbtask.TaskState_INITIALIZED,
			action:       StopAction,
		},
		{
			currentState: pbtask.TaskState_LAUNCHED,
			action:       StopAction,
		},
		{
			currentState: pbtask.TaskState_STARTING,
			action:       StopAction,
		},
		{
			currentState: pbtask.TaskState_RUNNING,
			action:       StopAction,
		},
		{
			currentState: pbtask.TaskState_LOST,
			action:       PreemptAction,
		},
		{
			currentState: pbtask.TaskState_KILLED,
			action:       PreemptAction,
		},
		{
			currentState: pbtask.TaskState_KILLING,
			action:       ExecutorShutdownAction,
		},
		{
			currentState: pbtask.TaskState_PREEMPTING,
			action:       TaskStateInvalidAction,
		},
	}

	var a TaskAction
	for i, test := range tt {
		a = taskEnt.suggestTaskAction(
			cached.TaskStateVector{State: test.currentState, ConfigVersion: 0},
			cached.TaskStateVector{State: pbtask.TaskState_PREEMPTING, ConfigVersion: 0})
		assert.Equal(t, test.action, a, "test %d fails", i)
	}
}

// Task with goal state FAILED should always invoke TaskStateInvalidAction
func TestEngineSuggestActionGoalFailed(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
	}

	testStates := []pbtask.TaskState{
		pbtask.TaskState_INITIALIZED,
		pbtask.TaskState_PENDING,
		pbtask.TaskState_LAUNCHED,
		pbtask.TaskState_STARTING,
		pbtask.TaskState_RUNNING,
		pbtask.TaskState_SUCCEEDED,
		pbtask.TaskState_FAILED,
		pbtask.TaskState_LOST,
		pbtask.TaskState_PREEMPTING,
		pbtask.TaskState_KILLING,
		pbtask.TaskState_KILLED,
	}

	for i, state := range testStates {
		a := taskEnt.suggestTaskAction(
			cached.TaskStateVector{State: state, ConfigVersion: 0},
			cached.TaskStateVector{State: pbtask.TaskState_FAILED, ConfigVersion: 0})
		assert.Equal(t, TaskStateInvalidAction, a, "test %d fails", i)
	}

}
