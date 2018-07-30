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
	goalstateutil "code.uber.internal/infra/peloton/jobmgr/util/goalstate"
	storemocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
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
	jobStore              *storemocks.MockJobStore
	resmgrClient          *resmocks.MockResourceManagerServiceYARPCClient
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
	suite.jobStore = storemocks.NewMockJobStore(suite.ctrl)
	suite.resmgrClient = resmocks.NewMockResourceManagerServiceYARPCClient(suite.ctrl)
	suite.goalStateDriver = &driver{
		updateFactory: suite.updateFactory,
		jobFactory:    suite.jobFactory,
		updateEngine:  suite.updateGoalStateEngine,
		taskEngine:    suite.taskGoalStateEngine,
		jobStore:      suite.jobStore,
		mtx:           NewMetrics(tally.NoopScope),
		cfg:           &Config{},
		resmgrClient:  suite.resmgrClient,
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
		ConfigVersion:        uint64(4),
		DesiredConfigVersion: uint64(4),
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

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  instancesTotal,
			JobVersion: uint64(4),
		}).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(instancesTotal)

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(&pbupdate.UpdateConfig{
			BatchSize: 0,
		})

	for _, instID := range instancesTotal {
		suite.cachedJob.EXPECT().
			AddTask(instID).
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
			[]uint32{4, 5},
			[]uint32{2, 3},
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
		GetInstancesAdded().
		Return(instancesTotal)

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  instancesTotal,
			JobVersion: uint64(3),
		}).Times(2)

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(&pbupdate.UpdateConfig{
			BatchSize: 0,
		})

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID)

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			JobVersion: uint64(3),
			Instances:  instancesTotal,
		})

	for _, instID := range instancesTotal {
		suite.cachedJob.EXPECT().
			AddTask(instID).
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

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID)

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
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  instancesTotal,
			JobVersion: uint64(1),
		}).
		Times(2)

	suite.cachedJob.EXPECT().
		AddTask(gomock.Any()).
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
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  instancesTotal,
			JobVersion: uint64(3),
		}).
		Times(2)

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(instancesTotal)

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(&pbupdate.UpdateConfig{
			BatchSize: 0,
		})

	for _, instID := range instancesTotal {
		suite.cachedJob.EXPECT().
			AddTask(instID).
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

// TestUpdateRun_FullyRunning_AddInstances test add instances for a fully running
// job
func (suite *UpdateRunTestSuite) TestUpdateRun_FullyRunning_AddInstances() {
	oldInstanceNumber := uint32(10)
	newInstanceNumber := uint32(20)
	batchSize := uint32(5)

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  newSlice(oldInstanceNumber, newInstanceNumber),
			JobVersion: uint64(4),
		}).AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(newSlice(oldInstanceNumber, newInstanceNumber))

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(&pbupdate.UpdateConfig{
			BatchSize: batchSize,
		}).AnyTimes()

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(gomock.Any(), gomock.Any(), uint64(4)).
		Return(&pbjob.JobConfig{
			ChangeLog: &peloton.ChangeLog{Version: uint64(4)},
		}, nil)

	for _, instID := range newSlice(oldInstanceNumber, newInstanceNumber) {
		suite.cachedJob.EXPECT().
			AddTask(instID).
			Return(suite.cachedTask)
		suite.cachedTask.EXPECT().
			GetRunTime(gomock.Any()).
			Return(nil,
				yarpcerrors.NotFoundErrorf("new instance has no runtime yet"))
		suite.cachedJob.EXPECT().
			RemoveTask(instID).
			Return()
	}

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{State: pbjob.JobState_RUNNING}, nil)

	suite.cachedJob.EXPECT().
		CreateTasks(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, runtimes map[uint32]*pbtask.RuntimeInfo, _ string) {
			suite.Len(runtimes, int(batchSize))
		}).Return(nil)

	suite.resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil)

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any()).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		WriteProgress(
			gomock.Any(),
			pbupdate.State_ROLLING_FORWARD,
			gomock.Any(),
			gomock.Any()).
		Do(func(_ context.Context, _ pbupdate.State,
			instancesDone []uint32, instancesCurrent []uint32) {
			suite.EqualValues(instancesCurrent,
				newSlice(oldInstanceNumber, oldInstanceNumber+batchSize))
			suite.Empty(instancesDone)
		}).Return(nil)

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateRun_FullyRunning_UpgradeInstances test upgrade instances for a fully running
// job
func (suite *UpdateRunTestSuite) TestUpdateRun_FullyRunning_UpgradeInstances() {
	instanceNumber := uint32(10)
	batchSize := uint32(5)
	jobVersion := uint64(3)
	newJobVersion := uint64(4)

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  newSlice(0, instanceNumber),
			JobVersion: newJobVersion,
		}).AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(newSlice(0, instanceNumber))

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(&pbupdate.UpdateConfig{
			BatchSize: batchSize,
		}).AnyTimes()

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(gomock.Any(), gomock.Any(), newJobVersion).
		Return(&pbjob.JobConfig{
			ChangeLog: &peloton.ChangeLog{Version: newJobVersion},
		}, nil)

	for _, instID := range newSlice(0, instanceNumber) {
		suite.cachedJob.EXPECT().
			AddTask(instID).
			Return(suite.cachedTask)
		suite.cachedTask.EXPECT().
			GetRunTime(gomock.Any()).
			Return(&pbtask.RuntimeInfo{
				State:                pbtask.TaskState_RUNNING,
				ConfigVersion:        jobVersion,
				DesiredConfigVersion: jobVersion,
			}, nil)
	}

	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return().
		Times(int(batchSize))

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any()).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		WriteProgress(
			gomock.Any(),
			pbupdate.State_ROLLING_FORWARD,
			gomock.Any(),
			gomock.Any()).
		Do(func(_ context.Context, _ pbupdate.State,
			instancesDone []uint32, instancesCurrent []uint32) {
			suite.EqualValues(instancesCurrent,
				newSlice(0, batchSize))
			suite.Empty(instancesDone)
		}).Return(nil)

	for _, instID := range newSlice(0, batchSize) {
		suite.cachedJob.EXPECT().
			GetTask(instID).
			Return(suite.cachedTask)
		suite.cachedTask.EXPECT().
			GetRunTime(gomock.Any()).
			Return(&pbtask.RuntimeInfo{
				State:                pbtask.TaskState_RUNNING,
				ConfigVersion:        jobVersion,
				DesiredConfigVersion: jobVersion,
			}, nil)
	}

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateRun_ContainsKilledTask_UpgradeInstances tests the case to upgrade
// a job with killed tasks
func (suite *UpdateRunTestSuite) TestUpdateRun_ContainsKilledTask_UpgradeInstances() {
	instanceNumber := uint32(10)
	batchSize := uint32(5)
	jobVersion := uint64(3)
	newJobVersion := uint64(4)

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  newSlice(0, instanceNumber),
			JobVersion: newJobVersion,
		}).AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(newSlice(0, instanceNumber))

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(&pbupdate.UpdateConfig{
			BatchSize: batchSize,
		}).AnyTimes()

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(gomock.Any(), gomock.Any(), newJobVersion).
		Return(&pbjob.JobConfig{
			ChangeLog: &peloton.ChangeLog{Version: newJobVersion},
		}, nil)

	for _, instID := range newSlice(0, instanceNumber) {
		suite.cachedJob.EXPECT().
			AddTask(instID).
			Return(suite.cachedTask)
		suite.cachedTask.EXPECT().
			GetRunTime(gomock.Any()).
			Return(&pbtask.RuntimeInfo{
				State:                pbtask.TaskState_KILLED,
				ConfigVersion:        jobVersion,
				DesiredConfigVersion: jobVersion,
			}, nil)
	}

	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return().
		Times(int(batchSize))

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any()).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		WriteProgress(
			gomock.Any(),
			pbupdate.State_ROLLING_FORWARD,
			gomock.Any(),
			gomock.Any()).
		Do(func(_ context.Context, _ pbupdate.State,
			instancesDone []uint32, instancesCurrent []uint32) {
			suite.EqualValues(instancesCurrent,
				newSlice(0, batchSize))
			suite.Empty(instancesDone)
		}).Return(nil)

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID)

	suite.cachedJob.EXPECT().
		GetTask(uint32(0)).
		Return(suite.cachedTask)

	suite.cachedTask.EXPECT().
		GetRunTime(gomock.Any()).
		Return(&pbtask.RuntimeInfo{
			State:                pbtask.TaskState_KILLED,
			GoalState:            pbtask.TaskState_KILLED,
			ConfigVersion:        jobVersion,
			DesiredConfigVersion: jobVersion,
		}, nil)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateRun_KilledJob_AddInstances tests add instances to killed job
func (suite *UpdateRunTestSuite) TestUpdateRun_KilledJob_AddInstances() {
	oldInstanceNumber := uint32(10)
	newInstanceNumber := uint32(20)
	batchSize := uint32(5)

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  newSlice(oldInstanceNumber, newInstanceNumber),
			JobVersion: uint64(4),
		}).AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(newSlice(oldInstanceNumber, newInstanceNumber))

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(&pbupdate.UpdateConfig{
			BatchSize: batchSize,
		}).AnyTimes()

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(gomock.Any(), gomock.Any(), uint64(4)).
		Return(&pbjob.JobConfig{
			ChangeLog: &peloton.ChangeLog{Version: uint64(4)},
		}, nil)

	for _, instID := range newSlice(oldInstanceNumber, newInstanceNumber) {
		suite.cachedJob.EXPECT().
			AddTask(instID).
			Return(suite.cachedTask)
		suite.cachedTask.EXPECT().
			GetRunTime(gomock.Any()).
			Return(nil,
				yarpcerrors.NotFoundErrorf("new instance has no runtime yet"))
		suite.cachedJob.EXPECT().
			RemoveTask(instID).
			Return()
	}

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{GoalState: pbjob.JobState_KILLED}, nil)

	suite.cachedJob.EXPECT().
		Update(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Do(func(_ context.Context, jobInfo *pbjob.JobInfo, _ cached.UpdateRequest) {
			suite.Equal(jobInfo.Runtime.GoalState,
				goalstateutil.GetDefaultJobGoalState(pbjob.JobType_SERVICE))
		}).
		Return(nil)

	suite.cachedJob.EXPECT().
		CreateTasks(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, runtimes map[uint32]*pbtask.RuntimeInfo, _ string) {
			suite.Len(runtimes, int(batchSize))
		}).Return(nil)

	suite.resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil)

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any()).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		WriteProgress(
			gomock.Any(),
			pbupdate.State_ROLLING_FORWARD,
			gomock.Any(),
			gomock.Any()).
		Do(func(_ context.Context, _ pbupdate.State,
			instancesDone []uint32, instancesCurrent []uint32) {
			suite.EqualValues(instancesCurrent,
				newSlice(oldInstanceNumber, oldInstanceNumber+batchSize))
			suite.Empty(instancesDone)
		}).Return(nil)

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateRun_DBError_AddInstances test add instances and db has error
// when add the tasks
func (suite *UpdateRunTestSuite) TestUpdateRun_DBError_AddInstances() {
	oldInstanceNumber := uint32(10)
	newInstanceNumber := uint32(20)
	batchSize := uint32(5)

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  newSlice(oldInstanceNumber, newInstanceNumber),
			JobVersion: uint64(4),
		}).AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(newSlice(oldInstanceNumber, newInstanceNumber))

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(&pbupdate.UpdateConfig{
			BatchSize: batchSize,
		}).AnyTimes()

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(gomock.Any(), gomock.Any(), uint64(4)).
		Return(&pbjob.JobConfig{
			ChangeLog: &peloton.ChangeLog{Version: uint64(4)},
		}, nil)

	for _, instID := range newSlice(oldInstanceNumber, newInstanceNumber) {
		suite.cachedJob.EXPECT().
			AddTask(instID).
			Return(suite.cachedTask)
		suite.cachedTask.EXPECT().
			GetRunTime(gomock.Any()).
			Return(nil,
				yarpcerrors.NotFoundErrorf("new instance has no runtime yet"))
		suite.cachedJob.EXPECT().
			RemoveTask(instID).
			Return()
	}

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{State: pbjob.JobState_RUNNING}, nil)

	suite.cachedJob.EXPECT().
		CreateTasks(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, runtimes map[uint32]*pbtask.RuntimeInfo, _ string) {
			suite.Len(runtimes, int(batchSize))
		}).Return(nil)

	suite.resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil)

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any()).
		Return(yarpcerrors.UnavailableErrorf("test error"))

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.Error(err)
}

// TestUpdateRun_DBError_UpgradeInstances test add instances and db has error
// when update the tasks
func (suite *UpdateRunTestSuite) TestUpdateRun_DBError_UpgradeInstances() {
	instanceNumber := uint32(10)
	batchSize := uint32(5)
	jobVersion := uint64(3)
	newJobVersion := uint64(4)

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  newSlice(0, instanceNumber),
			JobVersion: newJobVersion,
		}).AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(newSlice(0, instanceNumber))

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(&pbupdate.UpdateConfig{
			BatchSize: batchSize,
		}).AnyTimes()

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(gomock.Any(), gomock.Any(), newJobVersion).
		Return(&pbjob.JobConfig{
			ChangeLog: &peloton.ChangeLog{Version: newJobVersion},
		}, nil)

	for _, instID := range newSlice(0, instanceNumber) {
		suite.cachedJob.EXPECT().
			AddTask(instID).
			Return(suite.cachedTask)
		suite.cachedTask.EXPECT().
			GetRunTime(gomock.Any()).
			Return(&pbtask.RuntimeInfo{
				State:                pbtask.TaskState_RUNNING,
				ConfigVersion:        jobVersion,
				DesiredConfigVersion: jobVersion,
			}, nil)
	}

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any()).
		Return(yarpcerrors.UnavailableErrorf("test error"))

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.Error(err)
}

func newSlice(start uint32, end uint32) []uint32 {
	result := make([]uint32, 0, end-start)
	for i := start; i < end; i++ {
		result = append(result, i)
	}
	return result
}
