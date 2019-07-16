// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/common/util"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	goalstatemocks "github.com/uber/peloton/pkg/jobmgr/goalstate/mocks"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

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
	jobFactory      *cachedmocks.MockJobFactory
	goalStateDriver *goalstatemocks.MockDriver
	taskConfigV2Ops *objectmocks.MockTaskConfigV2Ops
}

func (suite *PreemptorTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockResmgr = mocks.NewMockResourceManagerServiceYARPCClient(suite.mockCtrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.mockCtrl)
	suite.goalStateDriver = goalstatemocks.NewMockDriver(suite.mockCtrl)
	suite.taskConfigV2Ops = objectmocks.NewMockTaskConfigV2Ops(suite.mockCtrl)

	suite.preemptor = &preemptor{
		resMgrClient:    suite.mockResmgr,
		jobFactory:      suite.jobFactory,
		goalStateDriver: suite.goalStateDriver,
		config: &Config{
			PreemptionPeriod:         1 * time.Minute,
			PreemptionDequeueLimit:   10,
			DequeuePreemptionTimeout: 100,
		},
		metrics:         NewMetrics(tally.NoopScope),
		lifeCycle:       lifecycle.NewLifeCycle(),
		taskConfigV2Ops: suite.taskConfigV2Ops,
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

	taskID = fmt.Sprintf("%s-%d", jobID.GetValue(), 3)
	noRestartMaintTaskID := &peloton.TaskID{Value: taskID}
	noRestartMaintTask := &resmgr.Task{
		Id: noRestartMaintTaskID,
	}
	noRestartMaintTaskInfo := &peloton_task.TaskInfo{
		InstanceId: 3,
		Runtime: &peloton_task.RuntimeInfo{
			State:     peloton_task.TaskState_RUNNING,
			GoalState: peloton_task.TaskState_RUNNING,
		},
	}

	cachedJob := cachedmocks.NewMockJob(suite.mockCtrl)
	runningCachedTask := cachedmocks.NewMockTask(suite.mockCtrl)
	killingCachedTask := cachedmocks.NewMockTask(suite.mockCtrl)
	noRestartCachedTask := cachedmocks.NewMockTask(suite.mockCtrl)
	noRestartMaintCachedTask := cachedmocks.NewMockTask(suite.mockCtrl)
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
				{
					Id:     noRestartMaintTask.Id,
					Reason: resmgr.PreemptionReason_PREEMPTION_REASON_HOST_MAINTENANCE,
				},
			},

			Error: nil,
		}, nil,
	)

	suite.jobFactory.EXPECT().AddJob(gomock.Any()).Return(cachedJob).Times(4)
	cachedJob.EXPECT().
		AddTask(gomock.Any(), runningTaskInfo.InstanceId).
		Return(runningCachedTask, nil)
	cachedJob.EXPECT().
		AddTask(gomock.Any(), killingTaskInfo.InstanceId).
		Return(killingCachedTask, nil)
	cachedJob.EXPECT().
		AddTask(gomock.Any(), noRestartTaskInfo.InstanceId).
		Return(noRestartCachedTask, nil)
	cachedJob.EXPECT().
		AddTask(gomock.Any(), noRestartMaintTaskInfo.InstanceId).
		Return(noRestartMaintCachedTask, nil)
	suite.taskConfigV2Ops.EXPECT().GetTaskConfig(
		gomock.Any(),
		jobID,
		runningTaskInfo.InstanceId,
		runningTaskInfo.Runtime.ConfigVersion).
		Return(nil, nil, nil)
	suite.taskConfigV2Ops.EXPECT().GetTaskConfig(
		gomock.Any(), jobID,
		noRestartTaskInfo.InstanceId, noRestartTaskInfo.Runtime.ConfigVersion).
		Return(
			&peloton_task.TaskConfig{
				PreemptionPolicy: &peloton_task.PreemptionPolicy{
					KillOnPreempt: true,
				},
			}, &models.ConfigAddOn{}, nil)
	suite.taskConfigV2Ops.EXPECT().GetTaskConfig(
		gomock.Any(), jobID,
		noRestartMaintTaskInfo.InstanceId,
		noRestartMaintTaskInfo.Runtime.ConfigVersion).
		Return(
			&peloton_task.TaskConfig{
				PreemptionPolicy: &peloton_task.PreemptionPolicy{
					KillOnPreempt: true,
				},
			}, &models.ConfigAddOn{}, nil)
	runningCachedTask.EXPECT().GetRuntime(gomock.Any()).Return(
		runningTaskInfo.Runtime,
		nil,
	)
	killingCachedTask.EXPECT().GetRuntime(gomock.Any()).Return(
		killingTaskInfo.Runtime,
		nil)
	noRestartCachedTask.EXPECT().GetRuntime(gomock.Any()).Return(
		noRestartTaskInfo.Runtime,
		nil,
	)
	noRestartMaintCachedTask.EXPECT().GetRuntime(gomock.Any()).Return(
		noRestartMaintTaskInfo.Runtime,
		nil,
	)

	cachedJob.EXPECT().PatchTasks(gomock.Any(), gomock.Any(), false).
		Do(func(ctx context.Context,
			runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff,
			_ bool) {
			suite.EqualValues(util.CreateMesosTaskID(jobID, runningTaskInfo.InstanceId, 1),
				runtimeDiffs[runningTaskInfo.InstanceId][jobmgrcommon.DesiredMesosTaskIDField])
		}).Return(nil, nil, nil)

	termStatusResource := &peloton_task.TerminationStatus{
		Reason: peloton_task.TerminationStatus_TERMINATION_STATUS_REASON_PREEMPTED_RESOURCES,
	}
	termStatusMaint := &peloton_task.TerminationStatus{
		Reason: peloton_task.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_HOST_MAINTENANCE,
	}
	runtimeDiffsNoRestartTask := map[string]interface{}{
		jobmgrcommon.GoalStateField: peloton_task.TaskState_PREEMPTING,
		jobmgrcommon.ReasonField:    "PREEMPTION_REASON_REVOKE_RESOURCES",
		jobmgrcommon.MessageField:   _msgPreemptingRunningTask,

		jobmgrcommon.TerminationStatusField: termStatusResource,
	}
	runtimeDiffsNoRestartMaintTask := map[string]interface{}{
		jobmgrcommon.GoalStateField: peloton_task.TaskState_PREEMPTING,
		jobmgrcommon.ReasonField:    "PREEMPTION_REASON_HOST_MAINTENANCE",
		jobmgrcommon.MessageField:   _msgPreemptingRunningTask,

		jobmgrcommon.TerminationStatusField: termStatusMaint,
	}
	cachedJob.EXPECT().PatchTasks(gomock.Any(), gomock.Any(), false).
		Do(func(ctx context.Context,
			runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff,
			_ bool) {
			suite.EqualValues(runtimeDiffsNoRestartTask,
				runtimeDiffs[noRestartTaskInfo.InstanceId])
		}).Return(nil, nil, nil)
	cachedJob.EXPECT().PatchTasks(gomock.Any(), gomock.Any(), false).
		Do(func(ctx context.Context,
			runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff,
			_ bool) {
			suite.EqualValues(runtimeDiffsNoRestartMaintTask,
				runtimeDiffs[noRestartMaintTaskInfo.InstanceId])
		}).Return(nil, nil, nil)

	suite.goalStateDriver.EXPECT().
		EnqueueTask(gomock.Any(), gomock.Any(), gomock.Any()).
		Return().
		Times(3)
	cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH).Times(6)
	suite.goalStateDriver.EXPECT().
		JobRuntimeDuration(job.JobType_BATCH).
		Return(1 * time.Second).Times(3)
	suite.goalStateDriver.EXPECT().
		EnqueueJob(gomock.Any(), gomock.Any()).
		Return().
		Times(3)

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
	// Test GetRuntime and PatchTasks error
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
	runningCachedTask.EXPECT().GetRuntime(gomock.Any()).Return(nil, fmt.Errorf("Fake GetRuntime error"))
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
