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

package evictor

import (
	"context"
	"fmt"
	"testing"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	peloton_task "github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/models"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
	resmgrmocks "github.com/uber/peloton/.gen/peloton/private/resmgrsvc/mocks"

	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/common/util"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	goalstatemocks "github.com/uber/peloton/pkg/jobmgr/goalstate/mocks"
	lmmocks "github.com/uber/peloton/pkg/jobmgr/task/lifecyclemgr/mocks"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type evictorTestSuite struct {
	suite.Suite
	mockCtrl             *gomock.Controller
	evictor              *evictor
	mockResmgr           *resmgrmocks.MockResourceManagerServiceYARPCClient
	mockLifecycleManager *lmmocks.MockManager
	jobFactory           *cachedmocks.MockJobFactory
	goalStateDriver      *goalstatemocks.MockDriver
	taskConfigV2Ops      *objectmocks.MockTaskConfigV2Ops
}

func (suite *evictorTestSuite) SetupTest() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockResmgr = resmgrmocks.NewMockResourceManagerServiceYARPCClient(suite.mockCtrl)
	suite.mockLifecycleManager = lmmocks.NewMockManager(suite.mockCtrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.mockCtrl)
	suite.goalStateDriver = goalstatemocks.NewMockDriver(suite.mockCtrl)
	suite.taskConfigV2Ops = objectmocks.NewMockTaskConfigV2Ops(suite.mockCtrl)

	suite.evictor = &evictor{
		resMgrClient:    suite.mockResmgr,
		jobFactory:      suite.jobFactory,
		goalStateDriver: suite.goalStateDriver,
		lm:              suite.mockLifecycleManager,
		config: &Config{
			EvictionPeriod:         1 * time.Minute,
			EvictionDequeueLimit:   10,
			EvictionDequeueTimeout: 100,
		},
		metrics:         NewMetrics(tally.NoopScope),
		lifeCycle:       lifecycle.NewLifeCycle(),
		taskConfigV2Ops: suite.taskConfigV2Ops,
	}
}

func (suite *evictorTestSuite) TearDownTest() {
	suite.mockCtrl.Finish()
}

func (suite *evictorTestSuite) TestPreemptionCycle() {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	taskID := fmt.Sprintf("%s-%d", jobID.GetValue(), 0)
	runningTaskID := &peloton.TaskID{Value: taskID}
	runningMesosTaskID := &mesos.TaskID{
		Value: &[]string{fmt.Sprintf("%s-1", taskID)}[0],
	}
	runningTask := &resmgr.Task{
		Id:     runningTaskID,
		TaskId: runningMesosTaskID,
	}
	runningTaskInfo := &peloton_task.TaskInfo{
		InstanceId: 0,
		Runtime: &peloton_task.RuntimeInfo{
			State:       peloton_task.TaskState_RUNNING,
			GoalState:   peloton_task.TaskState_RUNNING,
			MesosTaskId: runningMesosTaskID,
		},
	}

	taskID = fmt.Sprintf("%s-%d", jobID.GetValue(), 1)
	killingTaskID := &peloton.TaskID{Value: taskID}
	killingMesosTaskID := &mesos.TaskID{
		Value: &[]string{fmt.Sprintf("%s-1", taskID)}[0],
	}
	killingTask := &resmgr.Task{
		Id:     killingTaskID,
		TaskId: killingMesosTaskID,
	}
	killingTaskInfo := &peloton_task.TaskInfo{
		InstanceId: 1,
		Runtime: &peloton_task.RuntimeInfo{
			State:       peloton_task.TaskState_KILLING,
			GoalState:   peloton_task.TaskState_KILLED,
			MesosTaskId: killingMesosTaskID,
		},
	}

	taskID = fmt.Sprintf("%s-%d", jobID.GetValue(), 2)
	noRestartTaskID := &peloton.TaskID{Value: taskID}
	noRestartMesosTaskID := &mesos.TaskID{
		Value: &[]string{fmt.Sprintf("%s-1", taskID)}[0],
	}
	noRestartTask := &resmgr.Task{
		Id:     noRestartTaskID,
		TaskId: noRestartMesosTaskID,
	}
	noRestartTaskInfo := &peloton_task.TaskInfo{
		InstanceId: 2,
		Runtime: &peloton_task.RuntimeInfo{
			State:       peloton_task.TaskState_RUNNING,
			GoalState:   peloton_task.TaskState_RUNNING,
			MesosTaskId: noRestartMesosTaskID,
		},
	}

	taskID = fmt.Sprintf("%s-%d", jobID.GetValue(), 3)
	noRestartMaintTaskID := &peloton.TaskID{Value: taskID}
	noRestartMaintMesosTaskID := &mesos.TaskID{
		Value: &[]string{fmt.Sprintf("%s-1", taskID)}[0],
	}
	noRestartMaintTask := &resmgr.Task{
		Id:     noRestartMaintTaskID,
		TaskId: noRestartMaintMesosTaskID,
	}
	noRestartMaintTaskInfo := &peloton_task.TaskInfo{
		InstanceId: 3,
		Runtime: &peloton_task.RuntimeInfo{
			State:       peloton_task.TaskState_RUNNING,
			GoalState:   peloton_task.TaskState_RUNNING,
			MesosTaskId: noRestartMaintMesosTaskID,
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
					TaskId: runningTask.TaskId,
					Reason: resmgr.PreemptionReason_PREEMPTION_REASON_REVOKE_RESOURCES,
				},
				{
					Id:     killingTask.Id,
					TaskId: killingTask.TaskId,
					Reason: resmgr.PreemptionReason_PREEMPTION_REASON_REVOKE_RESOURCES,
				},
				{
					Id:     noRestartTask.Id,
					TaskId: noRestartTask.TaskId,
					Reason: resmgr.PreemptionReason_PREEMPTION_REASON_REVOKE_RESOURCES,
				},
				{
					Id:     noRestartMaintTask.Id,
					TaskId: noRestartMaintTask.TaskId,
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

	termStatusResource := &peloton_task.TerminationStatus{
		Reason: peloton_task.TerminationStatus_TERMINATION_STATUS_REASON_PREEMPTED_RESOURCES,
	}
	termStatusMaint := &peloton_task.TerminationStatus{
		Reason: peloton_task.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_HOST_MAINTENANCE,
	}
	runtimeDiffNoRestartTask := map[string]interface{}{
		jobmgrcommon.GoalStateField: peloton_task.TaskState_PREEMPTING,
		jobmgrcommon.ReasonField:    "EvictionReason_PREEMPTION",
		jobmgrcommon.MessageField:   _msgEvictingRunningTask,

		jobmgrcommon.TerminationStatusField: termStatusResource,
	}
	runtimeDiffNoRestartMaintTask := map[string]interface{}{
		jobmgrcommon.GoalStateField: peloton_task.TaskState_PREEMPTING,
		jobmgrcommon.ReasonField:    "EvictionReason_HOST_MAINTENANCE",
		jobmgrcommon.MessageField:   _msgEvictingRunningTask,

		jobmgrcommon.TerminationStatusField: termStatusMaint,
	}
	cachedJob.EXPECT().PatchTasks(gomock.Any(), gomock.Any(), false).
		Do(func(ctx context.Context,
			runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff,
			_ bool) {
			if _, ok := runtimeDiffs[runningTaskInfo.InstanceId]; ok {
				suite.EqualValues(util.CreateMesosTaskID(jobID, runningTaskInfo.InstanceId, 2),
					runtimeDiffs[runningTaskInfo.InstanceId][jobmgrcommon.DesiredMesosTaskIDField])
			} else if _, ok := runtimeDiffs[noRestartTaskInfo.InstanceId]; ok {
				suite.EqualValues(runtimeDiffNoRestartTask,
					runtimeDiffs[noRestartTaskInfo.InstanceId])
			} else if _, ok := runtimeDiffs[noRestartMaintTaskInfo.InstanceId]; ok {
				suite.EqualValues(runtimeDiffNoRestartMaintTask,
					runtimeDiffs[noRestartMaintTaskInfo.InstanceId])
			} else {
				suite.FailNow("unknown call to patch tasks")
			}
		}).Return(nil, nil, nil).Times(3)

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

	err := suite.evictor.performPreemptionCycle()
	suite.NoError(err)
}

func (suite *evictorTestSuite) TestPreemptionCycleGetPreemptibleTasksError() {
	// Test GetPreemptibleTasks error
	suite.mockResmgr.EXPECT().GetPreemptibleTasks(
		gomock.Any(),
		gomock.Any()).Return(
		nil,
		fmt.Errorf("fake GetPreemptibleTasks error"),
	)
	err := suite.evictor.performPreemptionCycle()
	suite.Error(err)
}

// TestPreemptionCycleDifferentMesosTaskID tests the case where mesos task id of
// the task in runtime is different from the that of the task to be preempted
func (suite *evictorTestSuite) TestPreemptionCycleDifferentMesosTaskID() {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	taskID := fmt.Sprintf("%s-%d", jobID.GetValue(), 0)
	runningTaskID := &peloton.TaskID{Value: taskID}
	runningMesosTaskID := &mesos.TaskID{
		Value: &[]string{fmt.Sprintf("%s-1", taskID)}[0],
	}
	runningTask := &resmgr.Task{
		Id:     runningTaskID,
		TaskId: runningMesosTaskID,
	}
	runningTaskInfo := &peloton_task.TaskInfo{
		InstanceId: 0,
		Runtime: &peloton_task.RuntimeInfo{
			State:     peloton_task.TaskState_RUNNING,
			GoalState: peloton_task.TaskState_RUNNING,
			MesosTaskId: &mesos.TaskID{
				Value: &[]string{fmt.Sprintf("%s-2", taskID)}[0],
			},
		},
	}

	cachedJob := cachedmocks.NewMockJob(suite.mockCtrl)
	runningCachedTask := cachedmocks.NewMockTask(suite.mockCtrl)

	suite.jobFactory.EXPECT().AddJob(gomock.Any()).Return(cachedJob)

	cachedJob.EXPECT().
		AddTask(gomock.Any(), runningTaskInfo.InstanceId).
		Return(runningCachedTask, nil)

	runningCachedTask.EXPECT().GetRuntime(gomock.Any()).Return(
		runningTaskInfo.Runtime,
		nil,
	)

	suite.mockResmgr.EXPECT().
		GetPreemptibleTasks(gomock.Any(), gomock.Any()).
		Return(
			&resmgrsvc.GetPreemptibleTasksResponse{
				PreemptionCandidates: []*resmgr.PreemptionCandidate{
					{
						Id:     runningTask.Id,
						TaskId: runningTask.TaskId,
						Reason: resmgr.PreemptionReason_PREEMPTION_REASON_REVOKE_RESOURCES,
					},
				},
				Error: nil,
			}, nil)

	suite.NoError(suite.evictor.performPreemptionCycle())
}

func (suite *evictorTestSuite) TestPreemptionCycleGetRuntimeError() {
	cachedJob := cachedmocks.NewMockJob(suite.mockCtrl)
	runningCachedTask := cachedmocks.NewMockTask(suite.mockCtrl)
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	taskID := fmt.Sprintf("%s-%d", jobID.GetValue(), 0)
	runningTaskID := &peloton.TaskID{Value: taskID}
	runningMesosTaskID := &mesos.TaskID{Value: &[]string{fmt.Sprintf("%s-1", taskID)}[0]}

	// Test GetRuntime error
	suite.mockResmgr.EXPECT().GetPreemptibleTasks(gomock.Any(), gomock.Any()).Return(
		&resmgrsvc.GetPreemptibleTasksResponse{
			PreemptionCandidates: []*resmgr.PreemptionCandidate{
				{
					Id:     runningTaskID,
					TaskId: runningMesosTaskID,
					Reason: resmgr.PreemptionReason_PREEMPTION_REASON_REVOKE_RESOURCES,
				},
			},
		}, nil,
	)
	suite.jobFactory.EXPECT().AddJob(gomock.Any()).Return(cachedJob)
	cachedJob.EXPECT().
		AddTask(gomock.Any(), uint32(0)).
		Return(runningCachedTask, nil)
	runningCachedTask.EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, fmt.Errorf("Fake GetRuntime error"))
	err := suite.evictor.performPreemptionCycle()
	suite.Error(err)
}

// TestHostMaintenanceCycle tests the case of killing a task for host maintenance
func (suite *evictorTestSuite) TestHostMaintenanceCycle() {
	cachedJob := cachedmocks.NewMockJob(suite.mockCtrl)
	runningCachedTask := cachedmocks.NewMockTask(suite.mockCtrl)
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	taskID := fmt.Sprintf("%s-%d", jobID.GetValue(), 0)
	runningMesosTaskID := &mesos.TaskID{Value: &[]string{fmt.Sprintf("%s-1", taskID)}[0]}
	runningTaskInfo := &peloton_task.TaskInfo{
		InstanceId: 0,
		Runtime: &peloton_task.RuntimeInfo{
			State:       peloton_task.TaskState_RUNNING,
			GoalState:   peloton_task.TaskState_RUNNING,
			MesosTaskId: runningMesosTaskID,
		},
	}

	suite.mockLifecycleManager.EXPECT().
		GetTasksOnDrainingHosts(gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]string{runningMesosTaskID.GetValue()}, nil)
	suite.jobFactory.EXPECT().AddJob(gomock.Any()).Return(cachedJob)
	cachedJob.EXPECT().
		AddTask(gomock.Any(), uint32(0)).
		Return(runningCachedTask, nil)
	runningCachedTask.EXPECT().GetRuntime(gomock.Any()).Return(
		runningTaskInfo.Runtime,
		nil,
	)
	cachedJob.EXPECT().GetJobType().Return(job.JobType_SERVICE)
	suite.taskConfigV2Ops.EXPECT().
		GetTaskConfig(
			gomock.Any(),
			jobID,
			runningTaskInfo.InstanceId,
			runningTaskInfo.Runtime.ConfigVersion).
		Return(nil, nil, nil)

	runtimeDiff := jobmgrcommon.RuntimeDiff{
		jobmgrcommon.MessageField: _msgEvictingRunningTask,
		jobmgrcommon.ReasonField:  EvictionReason_HOST_MAINTENANCE.String(),
		jobmgrcommon.TerminationStatusField: &peloton_task.TerminationStatus{
			Reason: peloton_task.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_HOST_MAINTENANCE,
		},
		jobmgrcommon.DesiredMesosTaskIDField: util.CreateMesosTaskID(
			jobID,
			runningTaskInfo.InstanceId,
			2,
		),
	}

	cachedJob.EXPECT().PatchTasks(gomock.Any(), gomock.Any(), false).
		Do(func(ctx context.Context,
			runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff,
			_ bool) {
			suite.EqualValues(runtimeDiff, runtimeDiffs[0])
		}).Return(nil, nil, nil)
	suite.goalStateDriver.EXPECT().
		EnqueueTask(gomock.Any(), gomock.Any(), gomock.Any()).
		Return()
	cachedJob.EXPECT().GetJobType().Return(job.JobType_SERVICE)
	suite.goalStateDriver.EXPECT().
		JobRuntimeDuration(job.JobType_SERVICE).
		Return(1 * time.Second)
	suite.goalStateDriver.EXPECT().
		EnqueueJob(gomock.Any(), gomock.Any()).
		Return()

	suite.NoError(suite.evictor.performHostMaintenanceCycle())
}

// TestHostMaintenanceCycleGetTasksError tests the failure case
// of killing a task for host maintenance due to error while getting tasks on
// DRAINING hosts
func (suite *evictorTestSuite) TestHostMaintenanceCycleGetTasksError() {
	suite.mockLifecycleManager.EXPECT().
		GetTasksOnDrainingHosts(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("fake GetTasksOnDrainingHosts error"))

	suite.Error(suite.evictor.performHostMaintenanceCycle())
}

// TestHostMaintenanceCycleTaskIDParseError tests the failure case of killing
// a task for host maintenance due to error while parsing task-id
func (suite *evictorTestSuite) TestHostMaintenanceCycleTaskIDParseError() {
	taskID := fmt.Sprintf("%s-%d", "invalid", 0)
	runningMesosTaskID := fmt.Sprintf("%s-1", taskID)

	suite.mockLifecycleManager.EXPECT().
		GetTasksOnDrainingHosts(gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]string{runningMesosTaskID}, nil)

	suite.Error(suite.evictor.performHostMaintenanceCycle())
}

// TestHostMaintenanceCycleAddTaskError tests the failure case of killing a task
// for host maintenance due to error while adding task to cache
func (suite *evictorTestSuite) TestHostMaintenanceCycleAddTaskError() {
	cachedJob := cachedmocks.NewMockJob(suite.mockCtrl)
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	taskID := fmt.Sprintf("%s-%d", jobID.GetValue(), 0)
	runningMesosTaskID := fmt.Sprintf("%s-1", taskID)

	suite.mockLifecycleManager.EXPECT().
		GetTasksOnDrainingHosts(gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]string{runningMesosTaskID}, nil)
	suite.jobFactory.EXPECT().AddJob(gomock.Any()).Return(cachedJob)
	cachedJob.EXPECT().
		AddTask(gomock.Any(), uint32(0)).
		Return(nil, fmt.Errorf("fake AddTask error"))

	suite.Error(suite.evictor.performHostMaintenanceCycle())
}

// TestHostMaintenanceCycleGetRuntimeError tests the failure case of killing a task
// for host maintenance due to error while getting task runtime
func (suite *evictorTestSuite) TestHostMaintenanceCycleGetRuntimeError() {
	cachedJob := cachedmocks.NewMockJob(suite.mockCtrl)
	runningCachedTask := cachedmocks.NewMockTask(suite.mockCtrl)
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	taskID := fmt.Sprintf("%s-%d", jobID.GetValue(), 0)
	runningMesosTaskID := fmt.Sprintf("%s-1", taskID)

	suite.mockLifecycleManager.EXPECT().
		GetTasksOnDrainingHosts(gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]string{runningMesosTaskID}, nil)
	suite.jobFactory.EXPECT().AddJob(gomock.Any()).Return(cachedJob)
	cachedJob.EXPECT().
		AddTask(gomock.Any(), uint32(0)).
		Return(runningCachedTask, nil)
	runningCachedTask.EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, fmt.Errorf("Fake GetRuntime error"))

	suite.Error(suite.evictor.performHostMaintenanceCycle())
}

// TestHostMaintenanceCycleGetTaskConfigError tests the failure case of killing
// a task for host maintenance due to error while getting task config
func (suite *evictorTestSuite) TestHostMaintenanceCycleGetTaskConfigError() {
	cachedJob := cachedmocks.NewMockJob(suite.mockCtrl)
	runningCachedTask := cachedmocks.NewMockTask(suite.mockCtrl)
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	taskID := fmt.Sprintf("%s-%d", jobID.GetValue(), 0)
	runningMesosTaskID := &mesos.TaskID{Value: &[]string{fmt.Sprintf("%s-1", taskID)}[0]}
	runningTaskInfo := &peloton_task.TaskInfo{
		InstanceId: 0,
		Runtime: &peloton_task.RuntimeInfo{
			State:       peloton_task.TaskState_RUNNING,
			GoalState:   peloton_task.TaskState_RUNNING,
			MesosTaskId: runningMesosTaskID,
		},
	}

	suite.mockLifecycleManager.EXPECT().
		GetTasksOnDrainingHosts(gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]string{runningMesosTaskID.GetValue()}, nil)
	suite.jobFactory.EXPECT().AddJob(gomock.Any()).Return(cachedJob)
	cachedJob.EXPECT().
		AddTask(gomock.Any(), uint32(0)).
		Return(runningCachedTask, nil)
	runningCachedTask.EXPECT().GetRuntime(gomock.Any()).Return(
		runningTaskInfo.Runtime,
		nil,
	)
	suite.taskConfigV2Ops.EXPECT().
		GetTaskConfig(
			gomock.Any(),
			jobID,
			runningTaskInfo.InstanceId,
			runningTaskInfo.Runtime.ConfigVersion).
		Return(nil, nil, fmt.Errorf("fake GetTaskConfig error"))

	suite.Error(suite.evictor.performHostMaintenanceCycle())
}

// TestHostMaintenanceCyclePatchTasksError tests the failure case of killing a
// task for host maintenance due to error while patching task runtime
func (suite *evictorTestSuite) TestHostMaintenanceCyclePatchTasksError() {
	cachedJob := cachedmocks.NewMockJob(suite.mockCtrl)
	runningCachedTask := cachedmocks.NewMockTask(suite.mockCtrl)
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	taskID := fmt.Sprintf("%s-%d", jobID.GetValue(), 0)
	runningMesosTaskID := &mesos.TaskID{Value: &[]string{fmt.Sprintf("%s-1", taskID)}[0]}
	runningTaskInfo := &peloton_task.TaskInfo{
		InstanceId: 0,
		Runtime: &peloton_task.RuntimeInfo{
			State:       peloton_task.TaskState_RUNNING,
			GoalState:   peloton_task.TaskState_RUNNING,
			MesosTaskId: runningMesosTaskID,
		},
	}

	suite.mockLifecycleManager.EXPECT().
		GetTasksOnDrainingHosts(gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]string{runningMesosTaskID.GetValue()}, nil)
	suite.jobFactory.EXPECT().AddJob(gomock.Any()).Return(cachedJob)
	cachedJob.EXPECT().
		AddTask(gomock.Any(), uint32(0)).
		Return(runningCachedTask, nil)
	runningCachedTask.EXPECT().GetRuntime(gomock.Any()).Return(
		runningTaskInfo.Runtime,
		nil,
	)
	cachedJob.EXPECT().GetJobType().Return(job.JobType_SERVICE)
	suite.taskConfigV2Ops.EXPECT().
		GetTaskConfig(
			gomock.Any(),
			jobID,
			runningTaskInfo.InstanceId,
			runningTaskInfo.Runtime.ConfigVersion).
		Return(nil, nil, nil)
	cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, nil, fmt.Errorf("fake PatchTasks error"))

	suite.Error(suite.evictor.performHostMaintenanceCycle())
}

func (suite *evictorTestSuite) TestEvictor_StartStop() {
	defer func() {
		suite.evictor.Stop()
		_, ok := <-suite.evictor.lifeCycle.StopCh()
		suite.False(ok)

		// Stopping evictor again should be no-op
		err := suite.evictor.Stop()
		suite.NoError(err)
	}()
	err := suite.evictor.Start()
	suite.NoError(err)
	suite.NotNil(suite.evictor.lifeCycle.StopCh())

	// Starting evictor again should be no-op
	err = suite.evictor.Start()
	suite.NoError(err)
}

func TestEvictor(t *testing.T) {
	suite.Run(t, new(evictorTestSuite))
}
