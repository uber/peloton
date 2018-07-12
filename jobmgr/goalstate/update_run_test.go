package goalstate

import (
	"context"
	"fmt"
	"testing"
	"time"

	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	pbupdate "code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"

	"code.uber.internal/infra/peloton/common/goalstate"
	goalstatemocks "code.uber.internal/infra/peloton/common/goalstate/mocks"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type UpdateRunTestSuite struct {
	suite.Suite
	ctrl                  *gomock.Controller
	updateFactory         *cachedmocks.MockUpdateFactory
	jobFactory            *cachedmocks.MockJobFactory
	updateGoalStateEngine *goalstatemocks.MockEngine
	taskGoalStateEngine   *goalstatemocks.MockEngine
	goalStateDriver       *driver
	jobID                 *peloton.JobID
	updateID              *peloton.UpdateID
	updateEnt             *updateEntity
	cachedJob             *cachedmocks.MockJob
	cachedUpdate          *cachedmocks.MockUpdate
	cachedTask            *cachedmocks.MockTask
}

func TestUpdateRun(t *testing.T) {
	suite.Run(t, new(UpdateRunTestSuite))
}

func (suite *UpdateRunTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.updateFactory = cachedmocks.NewMockUpdateFactory(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.updateGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.taskGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.goalStateDriver = &driver{
		updateFactory: suite.updateFactory,
		jobFactory:    suite.jobFactory,
		updateEngine:  suite.updateGoalStateEngine,
		taskEngine:    suite.taskGoalStateEngine,
		mtx:           NewMetrics(tally.NoopScope),
		cfg:           &Config{},
	}
	suite.goalStateDriver.cfg.normalize()

	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.updateID = &peloton.UpdateID{Value: uuid.NewRandom().String()}
	suite.updateEnt = &updateEntity{
		id:     suite.updateID,
		jobID:  suite.jobID,
		driver: suite.goalStateDriver,
	}

	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.cachedUpdate = cachedmocks.NewMockUpdate(suite.ctrl)
	suite.cachedTask = cachedmocks.NewMockTask(suite.ctrl)
}

func (suite *UpdateRunTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func (suite *UpdateRunTestSuite) TestRunningUpdate() {
	instancesTotal := []uint32{2, 3, 4, 5}

	runtimeDone := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_RUNNING,
		GoalState:            pbtask.TaskState_RUNNING,
		ConfigVersion:        uint64(3),
		DesiredConfigVersion: uint64(3),
	}

	runtimeRunning := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_RUNNING,
		GoalState:            pbtask.TaskState_RUNNING,
		ConfigVersion:        uint64(3),
		DesiredConfigVersion: uint64(4),
	}

	runtimeTerminated := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_KILLED,
		GoalState:            pbtask.TaskState_KILLED,
		ConfigVersion:        uint64(3),
		DesiredConfigVersion: uint64(4),
	}

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances: instancesTotal,
		})

	for _, instID := range instancesTotal {
		suite.cachedJob.EXPECT().
			GetTask(instID).
			Return(suite.cachedTask)

		if instID == instancesTotal[0] {
			// only 1 of the task is still running
			suite.cachedTask.EXPECT().
				GetRunTime(gomock.Any()).
				Return(runtimeRunning, nil)
		} else if instID == instancesTotal[1] {
			// only 1 task is in terminal goal state
			suite.cachedTask.EXPECT().
				GetRunTime(gomock.Any()).
				Return(runtimeTerminated, nil)
		} else {
			// rest are updated
			suite.cachedTask.EXPECT().
				GetRunTime(gomock.Any()).
				Return(runtimeDone, nil)
		}
	}

	suite.cachedUpdate.EXPECT().
		WriteProgress(
			gomock.Any(),
			pbupdate.State_ROLLING_FORWARD,
			[]uint32{3, 4, 5},
			[]uint32{2},
		).Return(nil)

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

func (suite *UpdateRunTestSuite) TestCompletedUpdate() {
	var instancesRemaining []uint32
	instancesTotal := []uint32{2, 3, 4, 5}
	runtime := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_RUNNING,
		GoalState:            pbtask.TaskState_RUNNING,
		ConfigVersion:        uint64(3),
		DesiredConfigVersion: uint64(3),
	}

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances: instancesTotal,
		})

	for _, instID := range instancesTotal {
		suite.cachedJob.EXPECT().
			GetTask(instID).
			Return(suite.cachedTask)

		suite.cachedTask.EXPECT().
			GetRunTime(gomock.Any()).
			Return(runtime, nil)
	}

	suite.cachedUpdate.EXPECT().
		WriteProgress(
			gomock.Any(),
			pbupdate.State_ROLLING_FORWARD,
			[]uint32{2, 3, 4, 5},
			instancesRemaining,
		).Return(nil)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Do(func(entity goalstate.Entity, deadline time.Time) {
			suite.Equal(suite.jobID.GetValue(), entity.GetID())
			updateEnt := entity.(*updateEntity)
			suite.Equal(suite.updateID.GetValue(), updateEnt.id.GetValue())
		})

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

func (suite *UpdateRunTestSuite) TestUpdateFailGetTask() {
	var instancesDone []uint32
	instancesTotal := []uint32{2, 3, 4, 5}

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances: instancesTotal,
		})

	for _, instID := range instancesTotal {
		taskID := fmt.Sprintf("%s-%d", suite.jobID.GetValue(), instID)

		suite.cachedJob.EXPECT().
			GetTask(instID).
			Return(nil)

		suite.taskGoalStateEngine.EXPECT().
			Enqueue(gomock.Any(), gomock.Any()).
			Do(func(taskEntity goalstate.Entity, deadline time.Time) {
				suite.Equal(taskID, taskEntity.GetID())
			})

		suite.cachedJob.EXPECT().
			GetJobType().
			Return(pbjob.JobType_BATCH)

		suite.updateGoalStateEngine.EXPECT().
			Enqueue(gomock.Any(), gomock.Any()).
			Do(func(updateEntity goalstate.Entity, deadline time.Time) {
				suite.Equal(suite.jobID.GetValue(), updateEntity.GetID())
			})
	}

	suite.cachedUpdate.EXPECT().
		WriteProgress(
			gomock.Any(),
			pbupdate.State_ROLLING_FORWARD,
			instancesDone,
			[]uint32{2, 3, 4, 5},
		).Return(nil)

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

func (suite *UpdateRunTestSuite) TestUpdateFailGetUpdate() {
	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(nil)

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

func (suite *UpdateRunTestSuite) TestUpdateFailGetJob() {
	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		Cancel(gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

func (suite *UpdateRunTestSuite) TestUpdateTaskRuntimeGetFail() {
	instancesTotal := []uint32{2, 3, 4, 5}

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances: instancesTotal,
		})

	suite.cachedJob.EXPECT().
		GetTask(gomock.Any()).
		Return(suite.cachedTask)

	suite.cachedTask.EXPECT().
		GetRunTime(gomock.Any()).
		Return(nil, fmt.Errorf("fake db error"))

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

func (suite *UpdateRunTestSuite) TestUpdateProgressDBError() {
	var instancesRemaining []uint32
	instancesTotal := []uint32{2, 3, 4, 5}
	runtime := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_RUNNING,
		GoalState:            pbtask.TaskState_RUNNING,
		ConfigVersion:        uint64(3),
		DesiredConfigVersion: uint64(3),
	}

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances: instancesTotal,
		})

	for _, instID := range instancesTotal {
		suite.cachedJob.EXPECT().
			GetTask(instID).
			Return(suite.cachedTask)

		suite.cachedTask.EXPECT().
			GetRunTime(gomock.Any()).
			Return(runtime, nil)
	}

	suite.cachedUpdate.EXPECT().
		WriteProgress(
			gomock.Any(),
			pbupdate.State_ROLLING_FORWARD,
			[]uint32{2, 3, 4, 5},
			instancesRemaining,
		).Return(fmt.Errorf("fake db error"))

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}
