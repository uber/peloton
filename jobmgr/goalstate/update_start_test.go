package goalstate

import (
	"context"
	"fmt"
	"testing"
	"time"

	mesosv1 "code.uber.internal/infra/peloton/.gen/mesos/v1"
	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	pbupdate "code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"
	"code.uber.internal/infra/peloton/.gen/peloton/private/models"
	resmocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"

	"code.uber.internal/infra/peloton/common/goalstate"
	goalstatemocks "code.uber.internal/infra/peloton/common/goalstate/mocks"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	jobmgrcommon "code.uber.internal/infra/peloton/jobmgr/common"
	storemocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
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

	jobConfig     *pbjob.JobConfig
	prevJobConfig *pbjob.JobConfig
	jobRuntime    *pbjob.RuntimeInfo
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

	commandValueNew := "new.sh"
	taskConfigNew := &pbtask.TaskConfig{
		Command: &mesosv1.CommandInfo{
			Value: &commandValueNew,
		},
	}
	suite.jobConfig = &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 4,
		},
		InstanceCount: 10,
		DefaultConfig: taskConfigNew,
	}

	commandValue := "entrypoint.sh"
	taskConfig := &pbtask.TaskConfig{
		Command: &mesosv1.CommandInfo{
			Value: &commandValue,
		},
	}
	suite.prevJobConfig = &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 3,
		},
		InstanceCount: 5,
		DefaultConfig: taskConfig,
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

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			JobVersion: suite.jobConfig.ChangeLog.Version,
		}).AnyTimes()

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(
			gomock.Any(), suite.jobID, suite.jobConfig.ChangeLog.Version).
		Return(nil, nil, fmt.Errorf("fake db error"))

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

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			JobVersion: suite.jobConfig.ChangeLog.Version,
		}).AnyTimes()

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(
			gomock.Any(), suite.jobID, suite.jobConfig.ChangeLog.Version).
		Return(suite.jobConfig, &models.ConfigAddOn{}, nil)

	suite.taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

func (suite *UpdateStartTestSuite) TestUpdateStartWriteProgressFail() {
	instancesTotal := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID).
		AnyTimes()

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(
			gomock.Any(), suite.jobID, suite.jobConfig.ChangeLog.Version).
		Return(suite.jobConfig, &models.ConfigAddOn{}, nil)

	suite.taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_RESTART)

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  instancesTotal,
			JobVersion: suite.jobConfig.ChangeLog.Version,
		}).AnyTimes()

	suite.cachedUpdate.EXPECT().
		WriteProgress(
			gomock.Any(),
			pbupdate.State_ROLLING_FORWARD,
			[]uint32{},
			[]uint32{},
			gomock.Any(),
		).Return(fmt.Errorf("fake db error"))

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

// TestUpdateContainsUnchangedInstance test the situation update
// contains unchanged instance
func (suite *UpdateStartTestSuite) TestUpdateContainsUnchangedInstance() {
	instancesTotal := []uint32{0}

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID).
		AnyTimes()

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(
			gomock.Any(), suite.jobID, suite.jobConfig.ChangeLog.Version).
		Return(suite.jobConfig, &models.ConfigAddOn{}, nil)

	suite.taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_RESTART)

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  instancesTotal,
			JobVersion: suite.jobConfig.ChangeLog.Version,
		}).AnyTimes()

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff) {
			for i := uint32(len(instancesTotal)); i < suite.jobConfig.GetInstanceCount(); i++ {
				runtimeDiff := runtimeDiffs[i]
				suite.NotNil(runtimeDiff)
				suite.Equal(runtimeDiff[jobmgrcommon.DesiredConfigVersionField],
					suite.jobConfig.GetChangeLog().Version)
				suite.Equal(runtimeDiff[jobmgrcommon.ConfigVersionField],
					suite.jobConfig.GetChangeLog().Version)
			}
		}).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		WriteProgress(
			gomock.Any(),
			pbupdate.State_ROLLING_FORWARD,
			[]uint32{},
			[]uint32{},
			gomock.Any(),
		).Return(nil)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateStart_ContainsUnchangedInstance_PatchTasksFail test the situation update
// contains unchanged instance
func (suite *UpdateStartTestSuite) TestUpdateStart_ContainsUnchangedInstance_PatchTasksFail() {
	instancesTotal := []uint32{0}

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID).
		AnyTimes()

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(
			gomock.Any(), suite.jobID, suite.jobConfig.ChangeLog.Version).
		Return(suite.jobConfig, &models.ConfigAddOn{}, nil)

	suite.taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_RESTART)

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  instancesTotal,
			JobVersion: suite.jobConfig.ChangeLog.Version,
		}).AnyTimes()

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any()).
		Return(yarpcerrors.UnavailableErrorf("test error"))

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.Error(err)
}

// TestUpdateStart_NoUnchangedInstance test the situation update
// contains no unchanged instance
func (suite *UpdateStartTestSuite) TestUpdateStart_NoUnchangedInstance() {
	var instancesTotal []uint32
	for i := uint32(0); i < suite.jobConfig.GetInstanceCount(); i++ {
		instancesTotal = append(instancesTotal, i)
	}

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID).
		AnyTimes()

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(
			gomock.Any(), suite.jobID, suite.jobConfig.ChangeLog.Version).
		Return(suite.jobConfig, &models.ConfigAddOn{}, nil)

	suite.taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_RESTART)

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  instancesTotal,
			JobVersion: suite.jobConfig.ChangeLog.Version,
		}).AnyTimes()

	suite.cachedUpdate.EXPECT().
		WriteProgress(
			gomock.Any(),
			pbupdate.State_ROLLING_FORWARD,
			[]uint32{},
			[]uint32{},
			gomock.Any(),
		).Return(nil)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateWorkflowUpdate tests starting a workflow update with no unchanged instances
func (suite *UpdateStartTestSuite) TestUpdateWorkflowUpdate() {
	instancesTotal := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	taskRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < suite.prevJobConfig.InstanceCount; i++ {
		runtime := &pbtask.RuntimeInfo{
			State:         pbtask.TaskState_RUNNING,
			ConfigVersion: suite.prevJobConfig.ChangeLog.Version,
		}
		taskRuntimes[i] = runtime
	}

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			JobVersion: suite.jobConfig.ChangeLog.Version,
			Instances:  instancesTotal,
		}).AnyTimes()

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(
			gomock.Any(), suite.jobID, suite.jobConfig.ChangeLog.Version).
		Return(suite.jobConfig, &models.ConfigAddOn{}, nil)

	suite.taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			JobVersion: suite.prevJobConfig.ChangeLog.Version,
		})

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(
			gomock.Any(), suite.jobID, suite.prevJobConfig.ChangeLog.Version).
		Return(suite.prevJobConfig, &models.ConfigAddOn{}, nil)

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, nil).
		Return(taskRuntimes, nil)

	suite.taskStore.EXPECT().
		GetTaskConfig(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(suite.prevJobConfig.DefaultConfig, nil, nil).
		Times(int(suite.prevJobConfig.InstanceCount))

	suite.cachedUpdate.EXPECT().
		Modify(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(
			_ context.Context,
			instancesAdded []uint32,
			instancesUpdated []uint32,
			instancesRemoved []uint32,
		) {
			suite.Len(instancesAdded,
				int(suite.jobConfig.InstanceCount-suite.prevJobConfig.InstanceCount))
			suite.Len(instancesUpdated, int(suite.prevJobConfig.InstanceCount))
			suite.Empty(instancesRemoved)
		}).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  instancesTotal,
			JobVersion: suite.jobConfig.ChangeLog.Version,
		}).AnyTimes()

	suite.cachedUpdate.EXPECT().
		WriteProgress(
			gomock.Any(),
			pbupdate.State_ROLLING_FORWARD,
			[]uint32{},
			[]uint32{},
			gomock.Any(),
		).Return(nil)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateWorkflowUpdatePrevJobConfigGetError tests getting an error when
// previous job config is fetched
func (suite *UpdateStartTestSuite) TestUpdateWorkflowUpdatePrevJobConfigGetError() {
	instancesTotal := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			JobVersion: suite.jobConfig.ChangeLog.Version,
			Instances:  instancesTotal,
		}).AnyTimes()

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(
			gomock.Any(), suite.jobID, suite.jobConfig.ChangeLog.Version).
		Return(suite.jobConfig, &models.ConfigAddOn{}, nil)

	suite.taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			JobVersion: suite.prevJobConfig.ChangeLog.Version,
		})

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(
			gomock.Any(), suite.jobID, suite.prevJobConfig.ChangeLog.Version).
		Return(nil, &models.ConfigAddOn{}, fmt.Errorf("fake db error"))

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.Error(err)
}

// TestUpdateWorkflowUpdate tests getting an error when task runtimes are fetched
func (suite *UpdateStartTestSuite) TestUpdateWorkflowUpdateGetRuntimeError() {
	instancesTotal := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			JobVersion: suite.jobConfig.ChangeLog.Version,
			Instances:  instancesTotal,
		}).AnyTimes()

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(
			gomock.Any(), suite.jobID, suite.jobConfig.ChangeLog.Version).
		Return(suite.jobConfig, &models.ConfigAddOn{}, nil)

	suite.taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			JobVersion: suite.prevJobConfig.ChangeLog.Version,
		})

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(
			gomock.Any(), suite.jobID, suite.prevJobConfig.ChangeLog.Version).
		Return(suite.prevJobConfig, &models.ConfigAddOn{}, nil)

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID)

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, nil).
		Return(nil, fmt.Errorf("fake db error"))

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.Error(err)
}

// TestUpdateWorkflowUpdateModifyError tests getting an error when modifying the update
func (suite *UpdateStartTestSuite) TestUpdateWorkflowUpdateModifyError() {
	instancesTotal := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	taskRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < suite.prevJobConfig.InstanceCount; i++ {
		runtime := &pbtask.RuntimeInfo{
			State:         pbtask.TaskState_RUNNING,
			ConfigVersion: suite.prevJobConfig.ChangeLog.Version,
		}
		taskRuntimes[i] = runtime
	}

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			JobVersion: suite.jobConfig.ChangeLog.Version,
			Instances:  instancesTotal,
		}).AnyTimes()

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(
			gomock.Any(), suite.jobID, suite.jobConfig.ChangeLog.Version).
		Return(suite.jobConfig, &models.ConfigAddOn{}, nil)

	suite.taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			JobVersion: suite.prevJobConfig.ChangeLog.Version,
		})

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(
			gomock.Any(), suite.jobID, suite.prevJobConfig.ChangeLog.Version).
		Return(suite.prevJobConfig, &models.ConfigAddOn{}, nil)

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, nil).
		Return(taskRuntimes, nil)

	suite.taskStore.EXPECT().
		GetTaskConfig(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(suite.prevJobConfig.DefaultConfig, nil, nil).
		Times(int(suite.prevJobConfig.InstanceCount))

	suite.cachedUpdate.EXPECT().
		Modify(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.Error(err)
}
