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
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	resmocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"

	"code.uber.internal/infra/peloton/common/goalstate"
	goalstatemocks "code.uber.internal/infra/peloton/common/goalstate/mocks"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	storemocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type UpdateStartTestSuite struct {
	suite.Suite
	ctrl *gomock.Controller

	jobStore      *storemocks.MockJobStore
	taskStore     *storemocks.MockTaskStore
	updateFactory *cachedmocks.MockUpdateFactory
	jobFactory    *cachedmocks.MockJobFactory
	resmgrClient  *resmocks.MockResourceManagerServiceYARPCClient

	updateGoalStateEngine *goalstatemocks.MockEngine
	taskGoalStateEngine   *goalstatemocks.MockEngine
	goalStateDriver       *driver

	jobID    *peloton.JobID
	updateID *peloton.UpdateID

	updateEnt    *updateEntity
	cachedJob    *cachedmocks.MockJob
	cachedUpdate *cachedmocks.MockUpdate
	cachedTask   *cachedmocks.MockTask

	jobConfig  *pbjob.JobConfig
	jobRuntime *pbjob.RuntimeInfo
}

func TestUpdateStart(t *testing.T) {
	suite.Run(t, new(UpdateStartTestSuite))
}

func (suite *UpdateStartTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.jobStore = storemocks.NewMockJobStore(suite.ctrl)
	suite.taskStore = storemocks.NewMockTaskStore(suite.ctrl)
	suite.updateFactory = cachedmocks.NewMockUpdateFactory(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.updateGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.taskGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.resmgrClient =
		resmocks.NewMockResourceManagerServiceYARPCClient(suite.ctrl)

	suite.goalStateDriver = &driver{
		jobStore:      suite.jobStore,
		taskStore:     suite.taskStore,
		updateFactory: suite.updateFactory,
		jobFactory:    suite.jobFactory,
		resmgrClient:  suite.resmgrClient,
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

	suite.jobConfig = &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 4,
		},
		InstanceCount: 10,
	}

	suite.jobRuntime = &pbjob.RuntimeInfo{
		State:     pbjob.JobState_RUNNING,
		GoalState: pbjob.JobState_RUNNING,
	}
}

func (suite *UpdateStartTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func (suite *UpdateStartTestSuite) TestUpdateStartCacheUpdateGetFail() {
	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(nil)

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

func (suite *UpdateStartTestSuite) TestUpdateStartCacheJobGetFail() {
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
		Return(nil)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Do(func(updateEntity goalstate.Entity, deadline time.Time) {
			suite.Equal(suite.jobID.GetValue(), updateEntity.GetID())
		})

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

func (suite *UpdateStartTestSuite) TestUpdateStartCacheJobGetError() {
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

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

func (suite *UpdateStartTestSuite) TestUpdateStartJobConfigGetFail() {
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

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(nil, fmt.Errorf("fake db error"))

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

func (suite *UpdateStartTestSuite) TestUpdateStartTaskConfigCreateFail() {
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

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(suite.jobConfig, nil)

	suite.taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

func (suite *UpdateStartTestSuite) TestUpdateStartAddInstancesRuntimeGetFail() {
	instancesAdded := []uint32{6, 7, 8, 9}

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

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(suite.jobConfig, nil)

	suite.taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any()).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(instancesAdded)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, fmt.Errorf("fake db error"))

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

func (suite *UpdateStartTestSuite) TestUpdateStartAddInstancesRuntimeSetFail() {
	instancesAdded := []uint32{6, 7, 8, 9}
	suite.jobRuntime.GoalState = pbjob.JobState_KILLED

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

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(suite.jobConfig, nil)

	suite.taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any()).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(instancesAdded)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(suite.jobRuntime, nil)

	suite.cachedJob.EXPECT().
		Update(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Do(func(_ context.Context, jobInfo *pbjob.JobInfo, _ cached.UpdateRequest) {
			suite.Equal(pbjob.JobState_RUNNING, jobInfo.GetRuntime().GetGoalState())
		}).Return(fmt.Errorf("fake db error"))

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

func (suite *UpdateStartTestSuite) TestUpdateStartAddInstCreateTaskFail() {
	instancesAdded := []uint32{6, 7, 8, 9}

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

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(suite.jobConfig, nil)

	suite.taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any()).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(instancesAdded)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(suite.jobRuntime, nil)

	for _, instID := range instancesAdded {
		suite.cachedJob.EXPECT().
			GetTask(instID).
			Return(nil)
	}

	suite.cachedJob.EXPECT().
		CreateTasks(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

func (suite *UpdateStartTestSuite) TestUpdateStartAddInstEnqueueFail() {
	instancesAdded := []uint32{6, 7, 8, 9}

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

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(suite.jobConfig, nil)

	suite.taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any()).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(instancesAdded)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(suite.jobRuntime, nil)

	for _, instID := range instancesAdded {
		suite.cachedJob.EXPECT().
			GetTask(instID).
			Return(nil)
	}

	suite.cachedJob.EXPECT().
		CreateTasks(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	suite.resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.EnqueueGangsResponse{}, fmt.Errorf("fake db error"))

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

func (suite *UpdateStartTestSuite) TestUpdateStartWriteProgressFail() {
	instancesAdded := []uint32{6, 7, 8, 9}
	instancesTotal := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

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

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(suite.jobConfig, nil)

	suite.taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any()).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(instancesAdded)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(suite.jobRuntime, nil)

	for _, instID := range instancesAdded {
		suite.cachedJob.EXPECT().
			GetTask(instID).
			Return(nil)
	}

	suite.cachedJob.EXPECT().
		CreateTasks(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	suite.resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any()).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances: instancesTotal,
		})

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return([]uint32{})

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances: instancesTotal,
		})

	suite.cachedUpdate.EXPECT().
		WriteProgress(
			gomock.Any(),
			pbupdate.State_ROLLING_FORWARD,
			[]uint32{},
			instancesTotal,
		).Return(fmt.Errorf("fake db error"))

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

func (suite *UpdateStartTestSuite) TestUpdateStartAddInstances() {
	instancesAdded := []uint32{6, 7, 8, 9}
	instancesTotal := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

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

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(suite.jobConfig, nil)

	suite.taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any()).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(instancesAdded)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(suite.jobRuntime, nil)

	for _, instID := range instancesAdded {
		if instID == 6 {
			suite.cachedJob.EXPECT().
				GetTask(instID).
				Return(suite.cachedTask)
			suite.cachedTask.EXPECT().
				GetRunTime(gomock.Any()).
				Return(&pbtask.RuntimeInfo{
					State: pbtask.TaskState_RUNNING,
				}, nil)
		} else if instID == 7 {
			suite.cachedJob.EXPECT().
				GetTask(instID).
				Return(suite.cachedTask)
			suite.cachedTask.EXPECT().
				GetRunTime(gomock.Any()).
				Return(&pbtask.RuntimeInfo{
					State: pbtask.TaskState_INITIALIZED,
				}, nil)
		} else {
			suite.cachedJob.EXPECT().
				GetTask(instID).
				Return(nil)
		}
	}

	suite.cachedJob.EXPECT().
		CreateTasks(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	suite.resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any()).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances: instancesTotal,
		})

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return([]uint32{})

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances: instancesTotal,
		})

	suite.cachedUpdate.EXPECT().
		WriteProgress(
			gomock.Any(),
			pbupdate.State_ROLLING_FORWARD,
			[]uint32{},
			instancesTotal,
		).Return(nil)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Do(func(entity goalstate.Entity, deadline time.Time) {
			suite.Equal(suite.jobID.GetValue(), entity.GetID())
			updateEnt := entity.(*updateEntity)
			suite.Equal(suite.updateID.GetValue(), updateEnt.id.GetValue())
		})

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

func (suite *UpdateStartTestSuite) TestUpdateStartNoChangeFail() {
	instancesTotal := []uint32{}

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

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(suite.jobConfig, nil)

	suite.taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any()).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return([]uint32{})

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances: instancesTotal,
		})

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

func (suite *UpdateStartTestSuite) TestUpdateStartNoChange() {
	instancesTotal := []uint32{}

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

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(suite.jobConfig, nil)

	suite.taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any()).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return([]uint32{})

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances: instancesTotal,
		})

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any()).
		Do(func(
			_ context.Context,
			runtimes map[uint32]cached.RuntimeDiff) {
			for _, runtimeDiff := range runtimes {
				suite.Equal(
					suite.jobConfig.ChangeLog.Version,
					runtimeDiff[cached.ConfigVersionField])
				suite.Equal(
					suite.jobConfig.ChangeLog.Version,
					runtimeDiff[cached.DesiredConfigVersionField],
				)
			}
		}).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return([]uint32{})

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances: instancesTotal,
		})

	suite.cachedUpdate.EXPECT().
		WriteProgress(
			gomock.Any(),
			pbupdate.State_ROLLING_FORWARD,
			[]uint32{},
			instancesTotal,
		).Return(nil)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Do(func(updateEntity goalstate.Entity, deadline time.Time) {
			suite.Equal(suite.jobID.GetValue(), updateEntity.GetID())
		})

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

func (suite *UpdateStartTestSuite) TestUpdateStartUpdateInstancesDBError() {
	instancesUpdated := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	instancesTotal := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

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

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(suite.jobConfig, nil)

	suite.taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any()).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return([]uint32{})

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances: instancesTotal,
		})

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(instancesUpdated)

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

func (suite *UpdateStartTestSuite) TestUpdateStartUpdateInstances() {
	instancesUpdated := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	instancesTotal := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

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

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(suite.jobConfig, nil)

	suite.taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any()).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return([]uint32{})

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances: instancesTotal,
		})

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(instancesUpdated)

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any()).
		Do(func(
			_ context.Context,
			runtimes map[uint32]cached.RuntimeDiff) {
			for _, runtimeDiff := range runtimes {
				suite.Equal(
					suite.jobConfig.ChangeLog.Version,
					runtimeDiff[cached.DesiredConfigVersionField],
				)
			}
		}).
		Return(nil)

	for _, instID := range instancesUpdated {
		taskID := fmt.Sprintf("%s-%d", suite.jobID.GetValue(), instID)
		suite.cachedJob.EXPECT().
			ID().
			Return(suite.jobID)
		suite.taskGoalStateEngine.EXPECT().
			Enqueue(gomock.Any(), gomock.Any()).
			Do(func(taskEntity goalstate.Entity, deadline time.Time) {
				suite.Equal(taskID, taskEntity.GetID())
			})
	}

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances: instancesTotal,
		})

	suite.cachedUpdate.EXPECT().
		WriteProgress(
			gomock.Any(),
			pbupdate.State_ROLLING_FORWARD,
			[]uint32{},
			instancesTotal,
		).Return(nil)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Do(func(updateEntity goalstate.Entity, deadline time.Time) {
			suite.Equal(suite.jobID.GetValue(), updateEntity.GetID())
		})

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.NoError(err)
}
