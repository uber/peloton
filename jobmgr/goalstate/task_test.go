package goalstate

import (
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/jobmgr/tracked"
	"code.uber.internal/infra/peloton/jobmgr/tracked/mocks"

	"fmt"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestEngineSuggestActionMissingRuntime(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	e := NewEngine(Config{}, nil, tally.NoopScope).(*engine)

	taskMock := mocks.NewMockTask(ctrl)

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_UNKNOWN, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_UNKNOWN_GOAL_STATE, ConfigVersion: 0})
	assert.Equal(t, tracked.ReloadRuntime, e.suggestTaskAction(taskMock))
}

func TestEngineSuggestActionGoalKilled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	e := NewEngine(Config{}, nil, tally.NoopScope).(*engine)

	taskMock := mocks.NewMockTask(ctrl)

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_KILLED, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_KILL, ConfigVersion: 0})
	assert.Equal(t, tracked.UntrackAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_KILLED, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_KILL, ConfigVersion: 1})
	assert.Equal(t, tracked.UseGoalConfigVersionAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_KILLED, ConfigVersion: tracked.UnknownVersion})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_KILL, ConfigVersion: 1})
	assert.Equal(t, tracked.UntrackAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_KILL, ConfigVersion: 0})
	assert.Equal(t, tracked.StopAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 10})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_KILL, ConfigVersion: tracked.UnknownVersion})
	assert.Equal(t, tracked.StopAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_FAILED, ConfigVersion: 10})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_KILL, ConfigVersion: tracked.UnknownVersion})
	assert.Equal(t, tracked.UntrackAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_UNHEALTHY, ConfigVersion: 10})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_KILL, ConfigVersion: tracked.UnknownVersion})
	assert.Equal(t, tracked.StopAction, e.suggestTaskAction(taskMock))
}

func TestEngineSuggestActionGoalRunning(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	e := &engine{}

	taskMock := mocks.NewMockTask(ctrl)

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_RUN, ConfigVersion: 0})
	assert.Equal(t, tracked.NoAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_RUN, ConfigVersion: 1})
	assert.Equal(t, tracked.StopAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_KILLED, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_RUN, ConfigVersion: 1})
	assert.Equal(t, tracked.UseGoalConfigVersionAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_INITIALIZED, ConfigVersion: tracked.UnknownVersion})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_RUN, ConfigVersion: 0})
	assert.Equal(t, tracked.StartAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_INITIALIZED, ConfigVersion: 123})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_RUN, ConfigVersion: 123})
	assert.Equal(t, tracked.StartAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_KILLED, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_RUN, ConfigVersion: 0})
	assert.Equal(t, tracked.InitializeAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_LOST, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_RUN, ConfigVersion: 0})
	assert.Equal(t, tracked.InitializeAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_FAILED, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_RUN, ConfigVersion: 0})
	assert.Equal(t, tracked.InitializeAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_SUCCEEDED, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_RUN, ConfigVersion: 0})
	assert.Equal(t, tracked.InitializeAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_UNHEALTHY, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_RUN, ConfigVersion: 0})
	assert.Equal(t, tracked.StopAction, e.suggestTaskAction(taskMock))
}

func TestEngineSuggestActionGoalSucceeded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	e := &engine{}

	taskMock := mocks.NewMockTask(ctrl)

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_SUCCEED, ConfigVersion: 0})
	assert.Equal(t, tracked.NoAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_INITIALIZED, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_SUCCEED, ConfigVersion: 0})
	assert.Equal(t, tracked.StartAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_KILLED, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_SUCCEED, ConfigVersion: 0})
	assert.Equal(t, tracked.InitializeAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_FAILED, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_SUCCEED, ConfigVersion: 0})
	assert.Equal(t, tracked.InitializeAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_LOST, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_SUCCEED, ConfigVersion: 0})
	assert.Equal(t, tracked.InitializeAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_UNHEALTHY, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_SUCCEED, ConfigVersion: 0})
	assert.Equal(t, tracked.StopAction, e.suggestTaskAction(taskMock))
}

func TestEngineSuggestActionGoalRestart(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		actual pb_task.TaskState
		action tracked.TaskAction
	}{
		{pb_task.TaskState_UNKNOWN, tracked.NoAction},
		{pb_task.TaskState_INITIALIZED, tracked.StopAction},
		{pb_task.TaskState_PENDING, tracked.NoAction},
		{pb_task.TaskState_READY, tracked.NoAction},
		{pb_task.TaskState_PLACING, tracked.NoAction},
		{pb_task.TaskState_PLACED, tracked.NoAction},
		{pb_task.TaskState_LAUNCHING, tracked.StopAction},
		{pb_task.TaskState_LAUNCHED, tracked.StopAction},
		{pb_task.TaskState_STARTING, tracked.NoAction},
		{pb_task.TaskState_RUNNING, tracked.StopAction},
		{pb_task.TaskState_SUCCEEDED, tracked.InitializeAction},
		{pb_task.TaskState_FAILED, tracked.InitializeAction},
		{pb_task.TaskState_LOST, tracked.InitializeAction},
		{pb_task.TaskState_PREEMPTING, tracked.NoAction},
		{pb_task.TaskState_KILLING, tracked.NoAction},
		{pb_task.TaskState_KILLED, tracked.InitializeAction},
		{pb_task.TaskState_UNHEALTHY, tracked.StopAction},
		{pb_task.TaskState_PENDING_HEALTH, tracked.StopAction},
	}

	e := &engine{}
	taskMock := mocks.NewMockTask(ctrl)

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%v -> RESTART: %v", tt.actual, tt.action), func(t *testing.T) {
			taskMock.EXPECT().CurrentState().Return(tracked.State{State: tt.actual, ConfigVersion: 0})
			taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_RESTART, ConfigVersion: 0})
			assert.Equal(t, tt.action, e.suggestTaskAction(taskMock))
		})
	}
}

func TestEngineSuggestActionGoalPreempting(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	e := &engine{}

	taskMock := mocks.NewMockTask(ctrl)

	tests := []struct {
		currentState pb_task.TaskState
		action       tracked.TaskAction
	}{
		{pb_task.TaskState_INITIALIZED, tracked.StopAction},
		{pb_task.TaskState_LAUNCHING, tracked.StopAction},
		{pb_task.TaskState_LAUNCHED, tracked.StopAction},
		{pb_task.TaskState_RUNNING, tracked.StopAction},
		{pb_task.TaskState_LOST, tracked.PreemptAction},
		{pb_task.TaskState_KILLED, tracked.PreemptAction},
		{pb_task.TaskState_UNHEALTHY, tracked.StopAction},
		{pb_task.TaskState_PENDING_HEALTH, tracked.StopAction},
	}

	for _, tt := range tests {
		taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_PREEMPT, ConfigVersion: 0})
		taskMock.EXPECT().CurrentState().Return(tracked.State{State: tt.currentState, ConfigVersion: 0})
		assert.Equal(t, tt.action, e.suggestTaskAction(taskMock))
	}
}

func TestEngineProcessTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobMock := mocks.NewMockJob(ctrl)
	taskMock := mocks.NewMockTask(ctrl)
	managerMock := mocks.NewMockManager(ctrl)

	e := &engine{
		trackedManager: managerMock,
		metrics:        NewMetrics(tally.NoopScope),
	}
	e.cfg.normalize()

	jobID := &peloton.JobID{Value: "my-job"}
	jobMock.EXPECT().ID().AnyTimes().Return(jobID)

	taskMock.EXPECT().Job().AnyTimes().Return(jobMock)
	taskMock.EXPECT().ID().AnyTimes().Return(uint32(0))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_RUN, ConfigVersion: 0})
	taskMock.EXPECT().LastAction().Return(tracked.NoAction, time.Time{})
	managerMock.EXPECT().RunTaskAction(gomock.Any(), jobID, uint32(0), tracked.NoAction).Return(nil)
	managerMock.EXPECT().ScheduleTask(taskMock, time.Time{})

	e.processTask(taskMock)

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_KILL, ConfigVersion: 0})
	taskMock.EXPECT().LastAction().Return(tracked.NoAction, time.Time{})
	managerMock.EXPECT().RunTaskAction(gomock.Any(), jobID, uint32(0), tracked.StopAction).Return(nil)
	managerMock.EXPECT().ScheduleTask(taskMock, gomock.Any())

	e.processTask(taskMock)

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{State: pb_task.TaskGoalState_KILL, ConfigVersion: 0})
	taskMock.EXPECT().LastAction().Return(tracked.StopAction, time.Time{})
	managerMock.EXPECT().RunTaskAction(gomock.Any(), jobID, uint32(0), tracked.StopAction).Return(fmt.Errorf("my error"))
	managerMock.EXPECT().ScheduleTask(taskMock, gomock.Any())

	e.processTask(taskMock)
}
