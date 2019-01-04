package preemptor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	peloton_task "github.com/uber/peloton/.gen/peloton/api/v0/task"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/private/models"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc/mocks"

	"github.com/uber/peloton/common/lifecycle"
	cachedmocks "github.com/uber/peloton/jobmgr/cached/mocks"
	jobmgrcommon "github.com/uber/peloton/jobmgr/common"
	goalstatemocks "github.com/uber/peloton/jobmgr/goalstate/mocks"
	storage_mocks "github.com/uber/peloton/storage/mocks"
	"github.com/uber/peloton/util"

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
	cachedJob.EXPECT().
		AddTask(gomock.Any(), runningTaskInfo.InstanceId).
		Return(runningCachedTask, nil)
	cachedJob.EXPECT().
		AddTask(gomock.Any(), killingTaskInfo.InstanceId).
		Return(killingCachedTask, nil)
	cachedJob.EXPECT().
		AddTask(gomock.Any(), noRestartTaskInfo.InstanceId).
		Return(noRestartCachedTask, nil)
	suite.mockTaskStore.EXPECT().GetTaskConfig(
		gomock.Any(),
		jobID,
		runningTaskInfo.InstanceId,
		runningTaskInfo.Runtime.ConfigVersion).
		Return(nil, nil, nil)
	suite.mockTaskStore.EXPECT().GetTaskConfig(
		gomock.Any(), jobID,
		noRestartTaskInfo.InstanceId, noRestartTaskInfo.Runtime.ConfigVersion).
		Return(
			&peloton_task.TaskConfig{
				PreemptionPolicy: &peloton_task.PreemptionPolicy{
					KillOnPreempt: true,
				},
			}, &models.ConfigAddOn{}, nil)
	runningCachedTask.EXPECT().GetRunTime(gomock.Any()).Return(
		runningTaskInfo.Runtime,
		nil,
	)
	killingCachedTask.EXPECT().GetRunTime(gomock.Any()).Return(
		killingTaskInfo.Runtime,
		nil)
	noRestartCachedTask.EXPECT().GetRunTime(gomock.Any()).Return(
		noRestartTaskInfo.Runtime,
		nil,
	)

	cachedJob.EXPECT().PatchTasks(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context,
			runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff) {
			suite.EqualValues(util.CreateMesosTaskID(jobID, runningTaskInfo.InstanceId, 1),
				runtimeDiffs[runningTaskInfo.InstanceId][jobmgrcommon.DesiredMesosTaskIDField])
		}).
		Return(nil)

	cachedJob.EXPECT().PatchTasks(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context,
			runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff) {
			suite.EqualValues(peloton_task.TaskState_PREEMPTING,
				runtimeDiffs[noRestartTaskInfo.InstanceId][jobmgrcommon.GoalStateField])
		}).
		Return(nil)

	suite.goalStateDriver.EXPECT().EnqueueTask(gomock.Any(), gomock.Any(), gomock.Any()).Return().Times(2)
	cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH).Times(4)
	suite.goalStateDriver.EXPECT().
		JobRuntimeDuration(job.JobType_BATCH).
		Return(1 * time.Second).Times(2)
	suite.goalStateDriver.EXPECT().EnqueueJob(gomock.Any(), gomock.Any()).Return().Times(2)

	err := suite.preemptor.performPreemptionCycle()
	suite.NoError(err)
}

func (suite *PreemptorTestSuite) TestPreemptionCycleGetPreemptibleTasksError() {
	// Test GetPreemptibleTasks error
	suite.mockResmgr.EXPECT().GetPreemptibleTasks(
		gomock.Any(),
		gomock.Any()).Return(
		nil,
		fmt.Errorf("fake GetPreemptibleTasks error"),
	)
	err := suite.preemptor.performPreemptionCycle()
	suite.Error(err)
}

func (suite *PreemptorTestSuite) TestPreemptionCycleGetRuntimeError() {
	cachedJob := cachedmocks.NewMockJob(suite.mockCtrl)
	runningCachedTask := cachedmocks.NewMockTask(suite.mockCtrl)
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	taskID := fmt.Sprintf("%s-%d", jobID.GetValue(), 0)
	runningTaskID := &peloton.TaskID{Value: taskID}
	// Test GetRunTime and PatchTasks error
	suite.mockResmgr.EXPECT().GetPreemptibleTasks(gomock.Any(), gomock.Any()).Return(
		&resmgrsvc.GetPreemptibleTasksResponse{
			PreemptionCandidates: []*resmgr.PreemptionCandidate{
				{
					Id:     runningTaskID,
					Reason: resmgr.PreemptionReason_PREEMPTION_REASON_REVOKE_RESOURCES,
				},
			},
		}, nil,
	)
	suite.jobFactory.EXPECT().AddJob(gomock.Any()).Return(cachedJob)
	cachedJob.EXPECT().
		AddTask(gomock.Any(), uint32(0)).
		Return(runningCachedTask, nil)
	runningCachedTask.EXPECT().GetRunTime(gomock.Any()).Return(nil, fmt.Errorf("Fake GetRunTime error"))
	err := suite.preemptor.performPreemptionCycle()
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
