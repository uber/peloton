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

func TestEngineSuggestActionGoalKilled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	e := NewEngine(Config{}, nil, tally.NoopScope).(*engine)

	taskMock := mocks.NewMockTask(ctrl)

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_KILLED, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.State{State: pb_task.TaskState_KILLED, ConfigVersion: 0})
	assert.Equal(t, tracked.KilledAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_KILLED, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.State{State: pb_task.TaskState_KILLED, ConfigVersion: 1})
	assert.Equal(t, tracked.UseGoalVersionAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_KILLED, ConfigVersion: tracked.UnknownVersion})
	taskMock.EXPECT().GoalState().Return(tracked.State{State: pb_task.TaskState_KILLED, ConfigVersion: 1})
	assert.Equal(t, tracked.KilledAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.State{State: pb_task.TaskState_KILLED, ConfigVersion: 0})
	assert.Equal(t, tracked.StopAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 10})
	taskMock.EXPECT().GoalState().Return(tracked.State{State: pb_task.TaskState_KILLED, ConfigVersion: tracked.UnknownVersion})
	assert.Equal(t, tracked.StopAction, e.suggestTaskAction(taskMock))
}

func TestEngineSuggestActionGoalRunning(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	e := &engine{}

	taskMock := mocks.NewMockTask(ctrl)

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 0})
	assert.Equal(t, tracked.NoAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 1})
	assert.Equal(t, tracked.StopAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_KILLED, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 1})
	assert.Equal(t, tracked.UseGoalVersionAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_INITIALIZED, ConfigVersion: tracked.UnknownVersion})
	taskMock.EXPECT().GoalState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 0})
	assert.Equal(t, tracked.StartAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_INITIALIZED, ConfigVersion: 123})
	taskMock.EXPECT().GoalState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 123})
	assert.Equal(t, tracked.StartAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_KILLED, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 0})
	assert.Equal(t, tracked.NoAction, e.suggestTaskAction(taskMock))
}

func TestEngineSuggestActionGoalPreempting(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	e := &engine{}

	taskMock := mocks.NewMockTask(ctrl)

	tt := []struct {
		currentState pb_task.TaskState
		action       tracked.TaskAction
	}{
		{
			currentState: pb_task.TaskState_INITIALIZED,
			action:       tracked.StopAction,
		},
		{
			currentState: pb_task.TaskState_LAUNCHING,
			action:       tracked.StopAction,
		},
		{
			currentState: pb_task.TaskState_LAUNCHED,
			action:       tracked.StopAction,
		},
		{
			currentState: pb_task.TaskState_RUNNING,
			action:       tracked.StopAction,
		},
		{
			currentState: pb_task.TaskState_LOST,
			action:       tracked.PreemptAction,
		},
		{
			currentState: pb_task.TaskState_KILLED,
			action:       tracked.PreemptAction,
		},
	}

	for _, test := range tt {
		taskMock.EXPECT().GoalState().Return(tracked.State{State: pb_task.TaskState_PREEMPTING, ConfigVersion: 0})
		taskMock.EXPECT().CurrentState().Return(tracked.State{State: test.currentState, ConfigVersion: 0})
		assert.Equal(t, test.action, e.suggestTaskAction(taskMock))
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
	}
	e.cfg.normalize()

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 0})
	taskMock.EXPECT().LastAction().Return(tracked.NoAction, time.Time{})
	taskMock.EXPECT().RunAction(gomock.Any(),
		tracked.NoAction).Return(false, nil)
	managerMock.EXPECT().ScheduleTask(taskMock, time.Time{})

	e.processTask(taskMock)

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.State{State: pb_task.TaskState_KILLED, ConfigVersion: 0})
	taskMock.EXPECT().LastAction().Return(tracked.NoAction, time.Time{})
	taskMock.EXPECT().RunAction(gomock.Any(), tracked.StopAction).Return(
		true, nil)
	managerMock.EXPECT().ScheduleTask(taskMock, gomock.Any())

	e.processTask(taskMock)

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.State{State: pb_task.TaskState_KILLED, ConfigVersion: 0})
	taskMock.EXPECT().LastAction().Return(tracked.StopAction, time.Time{})
	taskMock.EXPECT().RunAction(gomock.Any(), tracked.StopAction).Return(
		true, fmt.Errorf("my error"))
	taskMock.EXPECT().Job().Return(jobMock)
	jobMock.EXPECT().ID().Return(&peloton.JobID{})
	taskMock.EXPECT().ID().Return(uint32(0))
	managerMock.EXPECT().ScheduleTask(taskMock, gomock.Any())

	e.processTask(taskMock)

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_KILLED, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.State{State: pb_task.TaskState_PREEMPTING,
		ConfigVersion: 0})
	taskMock.EXPECT().LastAction().Return(tracked.StopAction, time.Time{})
	taskMock.EXPECT().RunAction(gomock.Any(), tracked.PreemptAction).Return(
		false, nil)
	managerMock.EXPECT().ScheduleTask(taskMock, time.Time{})

	e.processTask(taskMock)
}
