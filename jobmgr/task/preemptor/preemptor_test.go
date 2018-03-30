package preemptor

import (
	"context"
	"testing"
	"time"

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
	runningTaskID := &peloton.TaskID{Value: "test-preemption-1"}
	runningTask := &resmgr.Task{
		Id: runningTaskID,
	}
	runningTaskInfo := &peloton_task.TaskInfo{
		InstanceId: 0,
		Runtime: &peloton_task.RuntimeInfo{
			State:     peloton_task.TaskState_RUNNING,
			GoalState: peloton_task.TaskState_RUNNING,
		},
	}

	killingTaskID := &peloton.TaskID{Value: "test-preemption-2"}
	killingTask := &resmgr.Task{
		Id: killingTaskID,
	}
	killingTaskInfo := &peloton_task.TaskInfo{
		InstanceId: 1,
		Runtime: &peloton_task.RuntimeInfo{
			State:     peloton_task.TaskState_KILLING,
			GoalState: peloton_task.TaskState_KILLED,
		},
	}

	cachedJob := cachedmocks.NewMockJob(suite.mockCtrl)
	runtimes := make(map[uint32]*peloton_task.RuntimeInfo)
	runtimes[0] = runningTaskInfo.Runtime
	runtimes[1] = killingTaskInfo.Runtime

	suite.mockResmgr.EXPECT().GetPreemptibleTasks(gomock.Any(), gomock.Any()).Return(
		&resmgrsvc.GetPreemptibleTasksResponse{
			PreemptionCandidates: []*resmgr.PreemptionCandidate{
				{
					Id:     runningTask.Id,
					Reason: resmgr.PreemptionReason_PREEMPTION_REASON_REVOKE_RESOURCES,
				},
				{
					Id:     killingTask.Id,
					Reason: resmgr.PreemptionReason_PREEMPTION_REASON_REVOKE_RESOURCES,
				}},
			Error: nil,
		}, nil,
	)
	suite.mockTaskStore.EXPECT().GetTaskByID(gomock.Any(), runningTaskID.Value).Return(runningTaskInfo, nil)
	suite.mockTaskStore.EXPECT().GetTaskByID(gomock.Any(), killingTaskID.Value).Return(killingTaskInfo, nil)
	suite.jobFactory.EXPECT().AddJob(gomock.Any()).Return(cachedJob)
	cachedJob.EXPECT().UpdateTasks(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Do(func(ctx context.Context, runtimes map[uint32]*peloton_task.RuntimeInfo, req cached.UpdateRequest) {
			suite.Equal(peloton_task.TaskState_PREEMPTING, runtimes[runningTaskInfo.InstanceId].GetGoalState())
		}).
		Return(nil)
	suite.goalStateDriver.EXPECT().EnqueueTask(gomock.Any(), gomock.Any(), gomock.Any()).Return()

	err := suite.preemptor.performPreemptionCycle()
	suite.NoError(err)
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
