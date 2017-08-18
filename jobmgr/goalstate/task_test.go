package goalstate

import (
	"fmt"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/jobmgr/tracked"
	"code.uber.internal/infra/peloton/jobmgr/tracked/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestEngineSuggestAction(t *testing.T) {
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

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.State{State: pb_task.TaskState_KILLED, ConfigVersion: 0})
	assert.Equal(t, tracked.StopAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_KILLED, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 1})
	assert.Equal(t, tracked.UseGoalVersionAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_KILLED, ConfigVersion: tracked.UnknownVersion})
	taskMock.EXPECT().GoalState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 0})
	assert.Equal(t, tracked.NoAction, e.suggestTaskAction(taskMock))

	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_KILLED, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: tracked.UnknownVersion})
	assert.Equal(t, tracked.NoAction, e.suggestTaskAction(taskMock))
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

	taskMock.EXPECT().Job().Return(jobMock)
	jobMock.EXPECT().Lock()
	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 0})
	taskMock.EXPECT().LastAction().Return(tracked.NoAction, time.Time{})
	taskMock.EXPECT().RunAction(gomock.Any(), tracked.NoAction).Return(nil)
	jobMock.EXPECT().Unlock()
	managerMock.EXPECT().ScheduleTask(taskMock, time.Time{})

	e.processTask(taskMock)

	taskMock.EXPECT().Job().Return(jobMock)
	jobMock.EXPECT().Lock()
	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.State{State: pb_task.TaskState_KILLED, ConfigVersion: 0})
	taskMock.EXPECT().LastAction().Return(tracked.NoAction, time.Time{})
	taskMock.EXPECT().RunAction(gomock.Any(), tracked.StopAction).Return(nil)
	jobMock.EXPECT().Unlock()
	managerMock.EXPECT().ScheduleTask(taskMock, gomock.Any())

	e.processTask(taskMock)

	taskMock.EXPECT().Job().Return(jobMock)
	jobMock.EXPECT().Lock()
	taskMock.EXPECT().CurrentState().Return(tracked.State{State: pb_task.TaskState_RUNNING, ConfigVersion: 0})
	taskMock.EXPECT().GoalState().Return(tracked.State{State: pb_task.TaskState_KILLED, ConfigVersion: 0})
	taskMock.EXPECT().LastAction().Return(tracked.StopAction, time.Time{})
	taskMock.EXPECT().RunAction(gomock.Any(), tracked.StopAction).Return(fmt.Errorf("my error"))
	taskMock.EXPECT().Job().Return(jobMock)
	jobMock.EXPECT().ID().Return(&peloton.JobID{})
	taskMock.EXPECT().ID().Return(uint32(0))
	jobMock.EXPECT().Unlock()
	managerMock.EXPECT().ScheduleTask(taskMock, gomock.Any())

	e.processTask(taskMock)
}
