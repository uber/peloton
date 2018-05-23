package preemptor

import (
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	peloton_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"

	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	goalstatemocks "code.uber.internal/infra/peloton/jobmgr/goalstate/mocks"
	storage_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type PreemptorTestSuite struct {
	suite.Suite
	mockCtrl        *gomock.Controller
	preemptor       *preemptor
	mockResmgr      *mocks.MockResourceManagerServiceYARPCClient
	mockTaskStore   *storage_mocks.MockTaskStore
	jobFactory      *cachedmocks.MockJobFactory
	goalStateDriver *goalstatemocks.MockDriver
}

func (suite *PreemptorTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockResmgr = mocks.NewMockResourceManagerServiceYARPCClient(suite.mockCtrl)
	suite.mockTaskStore = storage_mocks.NewMockTaskStore(suite.mockCtrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.mockCtrl)
	suite.goalStateDriver = goalstatemocks.NewMockDriver(suite.mockCtrl)
	suite.preemptor = &preemptor{
		resMgrClient:    suite.mockResmgr,
		taskStore:       suite.mockTaskStore,
		jobFactory:      suite.jobFactory,
		goalStateDriver: suite.goalStateDriver,
		config: &Config{
			PreemptionPeriod:         1 * time.Minute,
			PreemptionDequeueLimit:   10,
			DequeuePreemptionTimeout: 100,
		},
		metrics: NewMetrics(tally.NoopScope),
	}
}

func (suite *PreemptorTestSuite) TearDownSuite() {
	suite.mockCtrl.Finish()
}

func (suite *PreemptorTestSuite) TestPreemptionCycle() {
	taskID := &peloton.TaskID{Value: "test-preemption-1"}
	task := &resmgr.Task{
		Id: taskID,
	}
	taskInfo := &peloton_task.TaskInfo{
		InstanceId: 0,
		Runtime: &peloton_task.RuntimeInfo{
			State: peloton_task.TaskState_RUNNING,
		},
	}

	preemptionCandidate := &resmgr.PreemptionCandidate{
		Id:     task.Id,
		Reason: resmgr.PreemptionReason_PREEMPTION_REASON_REVOKE_RESOURCES,
	}

	cachedJob := cachedmocks.NewMockJob(suite.mockCtrl)
	runtimes := make(map[uint32]*peloton_task.RuntimeInfo)
	runtimes[0] = taskInfo.Runtime

	suite.mockResmgr.EXPECT().GetPreemptibleTasks(gomock.Any(), gomock.Any()).Return(
		&resmgrsvc.GetPreemptibleTasksResponse{
			PreemptionCandidates: []*resmgr.PreemptionCandidate{preemptionCandidate},
			Error:                nil,
		}, nil,
	)
	suite.mockTaskStore.EXPECT().GetTaskByID(gomock.Any(), taskID.Value).Return(taskInfo, nil)
	suite.jobFactory.EXPECT().AddJob(gomock.Any()).Return(cachedJob)
	cachedJob.EXPECT().UpdateTasks(gomock.Any(), runtimes, cached.UpdateCacheAndDB).Return(nil)
	suite.goalStateDriver.EXPECT().EnqueueTask(gomock.Any(), gomock.Any(), gomock.Any()).Return()
	cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH)
	suite.goalStateDriver.EXPECT().
		GetJobRuntimeDuration(job.JobType_BATCH).
		Return(1 * time.Second)
	suite.goalStateDriver.EXPECT().EnqueueJob(gomock.Any(), gomock.Any()).Return()

	err := suite.preemptor.performPreemptionCycle()
	suite.NoError(err)
	suite.Equal(peloton_task.TaskState_PREEMPTING, taskInfo.GetRuntime().GoalState)
}

func (suite *PreemptorTestSuite) TestReconciler_StartStop() {
	defer func() {
		suite.preemptor.Stop()
		suite.False(suite.preemptor.isRunning())
	}()
	err := suite.preemptor.Start()
	suite.NoError(err)
	suite.True(suite.preemptor.isRunning())
}

func TestPreemptor(t *testing.T) {
	suite.Run(t, new(PreemptorTestSuite))
}
