package preemptor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	peloton_task "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"

	"code.uber.internal/infra/peloton/common/lifecycle"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	goalstatemocks "code.uber.internal/infra/peloton/jobmgr/goalstate/mocks"
	storage_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"code.uber.internal/infra/peloton/util"
	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
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
		metrics:   NewMetrics(tally.NoopScope),
		lifeCycle: lifecycle.NewLifeCycle(),
	}
}

func (suite *PreemptorTestSuite) TearDownSuite() {
	suite.mockCtrl.Finish()
}

func (suite *PreemptorTestSuite) TestPreemptionCycle() {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	taskID := fmt.Sprintf("%s-%d", jobID.GetValue(), 0)
	runningTaskID := &peloton.TaskID{Value: taskID}
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

	taskID = fmt.Sprintf("%s-%d", jobID.GetValue(), 1)
	killingTaskID := &peloton.TaskID{Value: taskID}
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

	taskID = fmt.Sprintf("%s-%d", jobID.GetValue(), 2)
	noRestartTaskID := &peloton.TaskID{Value: taskID}
	noRestartTask := &resmgr.Task{
		Id: noRestartTaskID,
	}
	noRestartTaskInfo := &peloton_task.TaskInfo{
		InstanceId: 2,
		Runtime: &peloton_task.RuntimeInfo{
			State:     peloton_task.TaskState_RUNNING,
			GoalState: peloton_task.TaskState_RUNNING,
		},
	}

	cachedJob := cachedmocks.NewMockJob(suite.mockCtrl)
	runningCachedTask := cachedmocks.NewMockTask(suite.mockCtrl)
	killingCachedTask := cachedmocks.NewMockTask(suite.mockCtrl)
	noRestartCachedTask := cachedmocks.NewMockTask(suite.mockCtrl)
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
				},
				{
					Id:     noRestartTask.Id,
					Reason: resmgr.PreemptionReason_PREEMPTION_REASON_REVOKE_RESOURCES,
				},
			},

			Error: nil,
		}, nil,
	)
	suite.jobFactory.EXPECT().AddJob(gomock.Any()).Return(cachedJob).Times(3)
	cachedJob.EXPECT().AddTask(runningTaskInfo.InstanceId).Return(runningCachedTask)
	cachedJob.EXPECT().AddTask(killingTaskInfo.InstanceId).Return(killingCachedTask)
	cachedJob.EXPECT().AddTask(noRestartTaskInfo.InstanceId).Return(noRestartCachedTask)
	suite.mockTaskStore.EXPECT().GetTaskConfig(
		gomock.Any(), jobID, runningTaskInfo.InstanceId, runningTaskInfo.Runtime.ConfigVersion).
		Return(nil, nil)
	suite.mockTaskStore.EXPECT().GetTaskConfig(
		gomock.Any(), jobID, noRestartTaskInfo.InstanceId, noRestartTaskInfo.Runtime.ConfigVersion).
		Return(
			&peloton_task.TaskConfig{
				PreemptionPolicy: &peloton_task.PreemptionPolicy{KillOnPreempt: true},
			}, nil)
	runningCachedTask.EXPECT().GetRunTime(gomock.Any()).Return(runningTaskInfo.Runtime, nil)
	killingCachedTask.EXPECT().GetRunTime(gomock.Any()).Return(killingTaskInfo.Runtime, nil)
	noRestartCachedTask.EXPECT().GetRunTime(gomock.Any()).Return(noRestartTaskInfo.Runtime, nil)

	cachedJob.EXPECT().PatchTasks(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context,
			runtimeDiffs map[uint32]cached.RuntimeDiff) {
			suite.EqualValues(util.CreateMesosTaskID(jobID, runningTaskInfo.InstanceId, 1),
				runtimeDiffs[runningTaskInfo.InstanceId][cached.DesiredMesosTaskIDField])
		}).
		Return(nil)

	cachedJob.EXPECT().PatchTasks(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context,
			runtimeDiffs map[uint32]cached.RuntimeDiff) {
			suite.EqualValues(peloton_task.TaskState_KILLED,
				runtimeDiffs[noRestartTaskInfo.InstanceId][cached.GoalStateField])
		}).
		Return(nil)

	suite.goalStateDriver.EXPECT().EnqueueTask(gomock.Any(), gomock.Any(), gomock.Any()).Return().Times(2)
	cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH).Times(2)
	suite.goalStateDriver.EXPECT().
		JobRuntimeDuration(job.JobType_BATCH).
		Return(1 * time.Second).Times(2)
	suite.goalStateDriver.EXPECT().EnqueueJob(gomock.Any(), gomock.Any()).Return().Times(2)

	err := suite.preemptor.performPreemptionCycle()
	suite.NoError(err)

	// Test GetPreemptibleTasks error
	suite.mockResmgr.EXPECT().GetPreemptibleTasks(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("Fake GetPreemptibleTasks error"))
	err = suite.preemptor.performPreemptionCycle()
	suite.Error(err)

	// Test GetRunTime and PatchTasks error
	suite.mockResmgr.EXPECT().GetPreemptibleTasks(gomock.Any(), gomock.Any()).Return(
		&resmgrsvc.GetPreemptibleTasksResponse{
			PreemptionCandidates: []*resmgr.PreemptionCandidate{
				{
					Id:     runningTask.Id,
					Reason: resmgr.PreemptionReason_PREEMPTION_REASON_REVOKE_RESOURCES,
				},
			},
		}, nil,
	)
	suite.jobFactory.EXPECT().AddJob(gomock.Any()).Return(cachedJob)
	cachedJob.EXPECT().AddTask(uint32(0)).Return(runningCachedTask)
	runningCachedTask.EXPECT().GetRunTime(gomock.Any()).Return(nil, fmt.Errorf("Fake GetRunTime error"))
	err = suite.preemptor.performPreemptionCycle()
	suite.Error(err)
}

func (suite *PreemptorTestSuite) TestReconciler_StartStop() {
	defer func() {
		suite.preemptor.Stop()
		_, ok := <-suite.preemptor.lifeCycle.StopCh()
		suite.False(ok)

		// Stopping preemptor again should be no-op
		err := suite.preemptor.Stop()
		suite.NoError(err)
	}()
	err := suite.preemptor.Start()
	suite.NoError(err)
	suite.NotNil(suite.preemptor.lifeCycle.StopCh())

	// Starting preemptor again should be no-op
	err = suite.preemptor.Start()
	suite.NoError(err)
}

func TestPreemptor(t *testing.T) {
	suite.Run(t, new(PreemptorTestSuite))
}
