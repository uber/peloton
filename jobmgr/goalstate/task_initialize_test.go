package goalstate

import (
	"context"
	"errors"
	"testing"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/models"

	goalstatemocks "code.uber.internal/infra/peloton/common/goalstate/mocks"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	jobmgrcommon "code.uber.internal/infra/peloton/jobmgr/common"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type TestTaskInitializeSuite struct {
	suite.Suite
	mockCtrl            *gomock.Controller
	jobStore            *store_mocks.MockJobStore
	taskStore           *store_mocks.MockTaskStore
	jobGoalStateEngine  *goalstatemocks.MockEngine
	taskGoalStateEngine *goalstatemocks.MockEngine
	jobFactory          *cachedmocks.MockJobFactory
	cachedJob           *cachedmocks.MockJob
	cachedTask          *cachedmocks.MockTask
	jobConfig           *cachedmocks.MockJobConfigCache
	cachedConfig        *cachedmocks.MockJobConfigCache
	goalStateDriver     *driver
	jobID               *peloton.JobID
	instanceID          uint32
	newConfigVersion    uint64
	oldMesosTaskID      string
	taskEnt             *taskEntity
	runtime             *pbtask.RuntimeInfo
}

func TestTaskInitializeRun(t *testing.T) {
	suite.Run(t, new(TestTaskInitializeSuite))
}

func (suite *TestTaskInitializeSuite) SetupTest() {
	suite.mockCtrl = gomock.NewController(suite.T())
	defer suite.mockCtrl.Finish()

	suite.jobStore = store_mocks.NewMockJobStore(suite.mockCtrl)
	suite.taskStore = store_mocks.NewMockTaskStore(suite.mockCtrl)
	suite.jobGoalStateEngine = goalstatemocks.NewMockEngine(suite.mockCtrl)
	suite.taskGoalStateEngine = goalstatemocks.NewMockEngine(suite.mockCtrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.mockCtrl)
	suite.cachedJob = cachedmocks.NewMockJob(suite.mockCtrl)
	suite.cachedTask = cachedmocks.NewMockTask(suite.mockCtrl)
	suite.jobConfig = cachedmocks.NewMockJobConfigCache(suite.mockCtrl)
	suite.cachedConfig = cachedmocks.NewMockJobConfigCache(suite.mockCtrl)

	suite.goalStateDriver = &driver{
		jobEngine:  suite.jobGoalStateEngine,
		taskEngine: suite.taskGoalStateEngine,
		jobStore:   suite.jobStore,
		taskStore:  suite.taskStore,
		jobFactory: suite.jobFactory,
		mtx:        NewMetrics(tally.NoopScope),
		cfg:        &Config{},
	}
	suite.goalStateDriver.cfg.normalize()
	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.instanceID = uint32(0)

	suite.taskEnt = &taskEntity{
		jobID:      suite.jobID,
		instanceID: suite.instanceID,
		driver:     suite.goalStateDriver,
	}

	suite.newConfigVersion = uint64(2)
	suite.oldMesosTaskID = uuid.New()

	suite.runtime = &pbtask.RuntimeInfo{
		State: pbtask.TaskState_KILLED,
		MesosTaskId: &mesos_v1.TaskID{
			Value: &suite.oldMesosTaskID,
		},
		ConfigVersion:        suite.newConfigVersion - 1,
		DesiredConfigVersion: suite.newConfigVersion,
	}
}

// Test TaskInitialize in  happy case
func (suite *TestTaskInitializeSuite) TestTaskInitialize() {
	testTable := []struct {
		taskConfig  *pbtask.TaskConfig
		healthState pbtask.HealthState
	}{
		{
			taskConfig:  &pbtask.TaskConfig{},
			healthState: pbtask.HealthState_DISABLED,
		},
		{
			taskConfig: &pbtask.TaskConfig{
				HealthCheck: &pbtask.HealthCheckConfig{
					Enabled:                true,
					InitialIntervalSecs:    10,
					IntervalSecs:           10,
					MaxConsecutiveFailures: 10,
					TimeoutSecs:            10,
				},
			},
			healthState: pbtask.HealthState_HEALTH_UNKNOWN,
		},
		{
			taskConfig: &pbtask.TaskConfig{
				HealthCheck: &pbtask.HealthCheckConfig{
					Enabled:                false,
					InitialIntervalSecs:    10,
					IntervalSecs:           10,
					MaxConsecutiveFailures: 10,
					TimeoutSecs:            10,
				},
			},
			healthState: pbtask.HealthState_DISABLED,
		},
	}

	for _, tt := range testTable {
		newRuntime := suite.runtime

		suite.jobFactory.EXPECT().
			GetJob(suite.jobID).Return(suite.cachedJob)

		suite.cachedJob.EXPECT().
			GetTask(suite.instanceID).Return(suite.cachedTask)

		suite.cachedTask.EXPECT().
			GetRunTime(gomock.Any()).Return(suite.runtime, nil)

		suite.cachedJob.EXPECT().GetConfig(gomock.Any()).Return(suite.cachedConfig, nil)

		suite.taskStore.EXPECT().GetTaskConfig(
			gomock.Any(), suite.jobID, suite.instanceID, suite.newConfigVersion).
			Return(tt.taskConfig, &models.ConfigAddOn{}, nil)

		suite.cachedJob.EXPECT().PatchTasks(gomock.Any(), gomock.Any()).Do(
			func(_ context.Context, runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff) {
				for _, runtimeDiff := range runtimeDiffs {
					suite.Equal(
						pbtask.TaskState_INITIALIZED,
						runtimeDiff[jobmgrcommon.StateField],
					)
					suite.Equal(
						pbtask.TaskState_SUCCEEDED,
						runtimeDiff[jobmgrcommon.GoalStateField],
					)
					suite.Equal(
						suite.newConfigVersion,
						runtimeDiff[jobmgrcommon.ConfigVersionField],
					)
					suite.Equal(
						tt.healthState,
						runtimeDiff[jobmgrcommon.HealthyField],
					)

				}
			}).Return(nil)

		suite.cachedConfig.EXPECT().
			GetType().Return(pb_job.JobType_BATCH)

		suite.cachedJob.EXPECT().
			GetJobType().Return(pb_job.JobType_BATCH)

		suite.taskGoalStateEngine.EXPECT().
			Enqueue(gomock.Any(), gomock.Any()).
			Return()

		suite.jobGoalStateEngine.EXPECT().
			Enqueue(gomock.Any(), gomock.Any()).
			Return()

		err := TaskInitialize(context.Background(), suite.taskEnt)
		suite.NoError(err)
		suite.NotEqual(suite.oldMesosTaskID, newRuntime.MesosTaskId)
	}
}

// Test TaskInitialize with no cached job
func (suite *TestTaskInitializeSuite) TestTaskInitializeNoJob() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).Return(nil)

	err := TaskInitialize(context.Background(), suite.taskEnt)
	suite.NoError(err)
}

// Test TaskInitialize with no cached task
func (suite *TestTaskInitializeSuite) TestTaskInitializeNoTask() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetTask(suite.instanceID).Return(nil)

	err := TaskInitialize(context.Background(), suite.taskEnt)
	suite.NoError(err)
}

// Test TaskInitialize when no task runtime exist
func (suite *TestTaskInitializeSuite) TestTaskInitializeNoTaskRuntime() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetTask(suite.instanceID).Return(suite.cachedTask)

	suite.cachedTask.EXPECT().
		GetRunTime(gomock.Any()).Return(nil, errors.New(""))

	err := TaskInitialize(context.Background(), suite.taskEnt)
	suite.Error(err)
}

// Test TaskInitialize when no job config exist
func (suite *TestTaskInitializeSuite) TestTaskInitializeNoJobConfig() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetTask(suite.instanceID).Return(suite.cachedTask)

	suite.cachedTask.EXPECT().
		GetRunTime(gomock.Any()).Return(suite.runtime, nil)

	suite.cachedJob.EXPECT().GetConfig(gomock.Any()).Return(nil, errors.New(""))

	err := TaskInitialize(context.Background(), suite.taskEnt)
	suite.Error(err)
}

// Test TaskInitialize when task config exist
func (suite *TestTaskInitializeSuite) TestTaskInitializeNoTaskConfig() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetTask(suite.instanceID).Return(suite.cachedTask)

	suite.cachedTask.EXPECT().
		GetRunTime(gomock.Any()).Return(suite.runtime, nil)

	suite.cachedJob.EXPECT().GetConfig(gomock.Any()).Return(suite.cachedConfig, nil)

	suite.taskStore.EXPECT().GetTaskConfig(
		gomock.Any(), suite.jobID, suite.instanceID, suite.newConfigVersion).Return(nil, nil, errors.New(""))

	err := TaskInitialize(context.Background(), suite.taskEnt)
	suite.Error(err)
}
