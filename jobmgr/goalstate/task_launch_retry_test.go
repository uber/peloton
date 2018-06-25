package goalstate

import (
	"context"
	"errors"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	hostmocks "code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	res_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"

	goalstatemocks "code.uber.internal/infra/peloton/common/goalstate/mocks"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type TestTaskLaunchRetrySuite struct {
	suite.Suite
	mockCtrl            *gomock.Controller
	jobStore            *store_mocks.MockJobStore
	taskStore           *store_mocks.MockTaskStore
	jobGoalStateEngine  *goalstatemocks.MockEngine
	taskGoalStateEngine *goalstatemocks.MockEngine
	jobFactory          *cachedmocks.MockJobFactory
	cachedJob           *cachedmocks.MockJob
	cachedTask          *cachedmocks.MockTask
	mockHostMgr         *hostmocks.MockInternalHostServiceYARPCClient
	jobConfig           *cachedmocks.MockJobConfig
	goalStateDriver     *driver
	resmgrClient        *res_mocks.MockResourceManagerServiceYARPCClient
	jobID               *peloton.JobID
	instanceID          uint32
}

func (suite *TestTaskLaunchRetrySuite) SetupTest() {
	suite.mockCtrl = gomock.NewController(suite.T())
	defer suite.mockCtrl.Finish()

	suite.jobStore = store_mocks.NewMockJobStore(suite.mockCtrl)
	suite.taskStore = store_mocks.NewMockTaskStore(suite.mockCtrl)
	suite.jobGoalStateEngine = goalstatemocks.NewMockEngine(suite.mockCtrl)
	suite.taskGoalStateEngine = goalstatemocks.NewMockEngine(suite.mockCtrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.mockCtrl)
	suite.cachedJob = cachedmocks.NewMockJob(suite.mockCtrl)
	suite.cachedTask = cachedmocks.NewMockTask(suite.mockCtrl)
	suite.resmgrClient = res_mocks.NewMockResourceManagerServiceYARPCClient(suite.mockCtrl)
	suite.jobConfig = cachedmocks.NewMockJobConfig(suite.mockCtrl)
	suite.mockHostMgr = hostmocks.NewMockInternalHostServiceYARPCClient(suite.mockCtrl)

	suite.goalStateDriver = &driver{
		jobEngine:     suite.jobGoalStateEngine,
		taskEngine:    suite.taskGoalStateEngine,
		jobStore:      suite.jobStore,
		taskStore:     suite.taskStore,
		jobFactory:    suite.jobFactory,
		hostmgrClient: suite.mockHostMgr,
		resmgrClient:  suite.resmgrClient,
		mtx:           NewMetrics(tally.NoopScope),
		cfg:           &Config{},
	}
	suite.goalStateDriver.cfg.normalize()
	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.instanceID = uint32(0)
	suite.instanceID = uint32(0)
}

func TestTaskLaunchRetry(t *testing.T) {
	suite.Run(t, new(TestTaskLaunchRetrySuite))
}

func (suite *TestTaskLaunchRetrySuite) TestTaskLaunchTimeout() {
	oldMesosTaskID := &mesos_v1.TaskID{
		Value: &[]string{uuid.New()}[0],
	}

	runtime := suite.getRunTime(
		pb_task.TaskState_LAUNCHED,
		pb_task.TaskState_SUCCEEDED,
		oldMesosTaskID)
	config := &pb_task.TaskConfig{}

	for i := 0; i < 2; i++ {
		suite.jobFactory.EXPECT().
			GetJob(suite.jobID).Return(suite.cachedJob)

		suite.cachedJob.EXPECT().
			GetTask(suite.instanceID).Return(suite.cachedTask)

		suite.cachedTask.EXPECT().
			GetRunTime(gomock.Any()).Return(runtime, nil)

		suite.cachedTask.EXPECT().
			GetLastRuntimeUpdateTime().Return(time.Now().Add(-suite.goalStateDriver.cfg.LaunchTimeout))

		suite.jobFactory.EXPECT().
			GetJob(suite.jobID).Return(suite.cachedJob)

		suite.cachedJob.EXPECT().
			GetTask(suite.instanceID).Return(suite.cachedTask)

		suite.cachedTask.EXPECT().
			GetRunTime(gomock.Any()).Return(runtime, nil)

		suite.cachedJob.EXPECT().
			GetConfig(gomock.Any()).
			Return(suite.jobConfig, nil)

		suite.jobConfig.EXPECT().GetType().Return(pb_job.JobType_BATCH)

		suite.cachedJob.EXPECT().PatchTasks(gomock.Any(), gomock.Any()).Do(
			func(_ context.Context, runtimeDiffs map[uint32]map[string]interface{}) {
				for _, runtimeDiff := range runtimeDiffs {
					suite.Equal(oldMesosTaskID, runtimeDiff[cached.PrevMesosTaskIDField])
					suite.NotEqual(oldMesosTaskID, runtimeDiff[cached.MesosTaskIDField])
					suite.Equal(pb_task.TaskState_INITIALIZED, runtimeDiff[cached.StateField])
					suite.Equal(pb_task.TaskState_SUCCEEDED, runtimeDiff[cached.GoalStateField])
				}
			}).Return(nil)

		suite.cachedJob.EXPECT().
			GetJobType().Return(pb_job.JobType_BATCH)

		suite.taskGoalStateEngine.EXPECT().
			Enqueue(gomock.Any(), gomock.Any()).
			Return()

		suite.jobGoalStateEngine.EXPECT().
			Enqueue(gomock.Any(), gomock.Any()).
			Return()

		if i == 0 {
			// test happy case
			suite.taskStore.EXPECT().GetTaskConfig(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(config, nil)
			suite.mockHostMgr.EXPECT().KillTasks(gomock.Any(), &hostsvc.KillTasksRequest{
				TaskIds: []*mesos_v1.TaskID{oldMesosTaskID},
			})
		} else {
			// test skip task kill
			suite.taskStore.EXPECT().GetTaskConfig(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New(""))
			suite.mockHostMgr.EXPECT()
		}
		suite.NoError(TaskLaunchRetry(context.Background(), suite.getTaskEntity(suite.jobID, suite.instanceID)))
	}
}

func (suite *TestTaskLaunchRetrySuite) TestLaunchedTaskSendLaunchInfoResMgr() {
	mesosID := "mesos_id"
	runtime := suite.getRunTime(
		pb_task.TaskState_LAUNCHED,
		pb_task.TaskState_SUCCEEDED,
		&mesos_v1.TaskID{
			Value: &mesosID,
		})

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetTask(suite.instanceID).Return(suite.cachedTask)

	suite.cachedTask.EXPECT().
		GetLastRuntimeUpdateTime().Return(time.Now())

	suite.cachedTask.EXPECT().
		GetRunTime(gomock.Any()).Return(runtime, nil)

	suite.resmgrClient.EXPECT().
		UpdateTasksState(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.UpdateTasksStateResponse{}, nil)

	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	suite.NoError(TaskLaunchRetry(context.Background(),
		suite.getTaskEntity(suite.jobID, suite.instanceID)))
}

func (suite *TestTaskLaunchRetrySuite) TestLaunchRetryError() {
	mesosID := "mesos_id"
	runtime := suite.getRunTime(
		pb_task.TaskState_LAUNCHED,
		pb_task.TaskState_SUCCEEDED,
		&mesos_v1.TaskID{
			Value: &mesosID,
		})
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetTask(suite.instanceID).Return(suite.cachedTask)

	suite.cachedTask.EXPECT().
		GetLastRuntimeUpdateTime().Return(time.Now())

	suite.cachedTask.EXPECT().
		GetRunTime(gomock.Any()).Return(runtime, nil)

	suite.resmgrClient.EXPECT().
		UpdateTasksState(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.UpdateTasksStateResponse{}, errors.New("error"))

	err := TaskLaunchRetry(context.Background(),
		suite.getTaskEntity(suite.jobID, suite.instanceID))
	suite.Error(err)
	suite.Equal(err.Error(), "error")
}

func (suite *TestTaskLaunchRetrySuite) TestTaskStartTimeout() {
	oldMesosTaskID := &mesos_v1.TaskID{
		Value: &[]string{uuid.New()}[0],
	}
	runtime := suite.getRunTime(
		pb_task.TaskState_STARTING,
		pb_task.TaskState_SUCCEEDED,
		oldMesosTaskID)
	config := &pb_task.TaskConfig{}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetTask(suite.instanceID).Return(suite.cachedTask)

	suite.taskStore.EXPECT().GetTaskConfig(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(config, nil)

	suite.cachedTask.EXPECT().
		GetLastRuntimeUpdateTime().Return(time.Now().Add(-suite.goalStateDriver.cfg.LaunchTimeout))

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetTask(suite.instanceID).Return(suite.cachedTask)

	suite.cachedTask.EXPECT().
		GetRunTime(gomock.Any()).Return(runtime, nil)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(suite.jobConfig, nil)

	suite.cachedTask.EXPECT().
		GetRunTime(gomock.Any()).Return(runtime, nil)

	suite.jobConfig.EXPECT().GetType().Return(pb_job.JobType_BATCH)

	suite.cachedJob.EXPECT().PatchTasks(gomock.Any(), gomock.Any()).Do(
		func(_ context.Context, runtimeDiffs map[uint32]map[string]interface{}) {
			for _, runtimeDiff := range runtimeDiffs {
				suite.Equal(oldMesosTaskID, runtimeDiff[cached.PrevMesosTaskIDField])
				suite.NotEqual(oldMesosTaskID, runtimeDiff[cached.MesosTaskIDField])
				suite.Equal(pb_task.TaskState_INITIALIZED, runtimeDiff[cached.StateField])
				suite.Equal(pb_task.TaskState_SUCCEEDED, runtimeDiff[cached.GoalStateField])
			}
		}).Return(nil)

	suite.cachedJob.EXPECT().
		GetJobType().Return(pb_job.JobType_BATCH)

	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	suite.mockHostMgr.EXPECT().KillTasks(gomock.Any(), &hostsvc.KillTasksRequest{
		TaskIds: []*mesos_v1.TaskID{oldMesosTaskID},
	})

	suite.NoError(TaskLaunchRetry(
		context.Background(),
		suite.getTaskEntity(suite.jobID, suite.instanceID)))
}

func (suite *TestTaskLaunchRetrySuite) TestStartingTaskReenqueue() {
	runtime := suite.getRunTime(
		pb_task.TaskState_STARTING,
		pb_task.TaskState_SUCCEEDED,
		nil)
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetTask(suite.instanceID).Return(suite.cachedTask)

	suite.cachedTask.EXPECT().
		GetLastRuntimeUpdateTime().Return(time.Now())

	suite.cachedTask.EXPECT().
		GetRunTime(gomock.Any()).Return(runtime, nil)

	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	suite.NoError(TaskLaunchRetry(context.Background(),
		suite.getTaskEntity(suite.jobID, suite.instanceID)))
}

func (suite *TestTaskLaunchRetrySuite) TestTaskWithUnexpectedStateReenqueue() {
	runtime := suite.getRunTime(
		pb_task.TaskState_RUNNING,
		pb_task.TaskState_SUCCEEDED,
		nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetTask(suite.instanceID).Return(suite.cachedTask)

	suite.cachedTask.EXPECT().
		GetRunTime(gomock.Any()).Return(runtime, nil)

	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	suite.NoError(TaskLaunchRetry(context.Background(),
		suite.getTaskEntity(suite.jobID, suite.instanceID)))
}

// getTaskEntity returns the TaskEntity
func (suite *TestTaskLaunchRetrySuite) getTaskEntity(
	jobID *peloton.JobID,
	instanceID uint32,
) *taskEntity {
	return &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
		driver:     suite.goalStateDriver,
	}
}

// getRunTime returns the runtime for specified
// state, goalstate and mesostaskID
func (suite *TestTaskLaunchRetrySuite) getRunTime(
	state pb_task.TaskState,
	goalState pb_task.TaskState,
	mesosID *mesos_v1.TaskID,
) *pb_task.RuntimeInfo {
	return &pb_task.RuntimeInfo{
		State:       state,
		MesosTaskId: mesosID,
		GoalState:   goalState,
	}
}
