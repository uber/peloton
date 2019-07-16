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

package goalstate

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	mesosv1 "github.com/uber/peloton/.gen/mesos/v1"
	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/models"
	goalstatemocks "github.com/uber/peloton/pkg/common/goalstate/mocks"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	storemocks "github.com/uber/peloton/pkg/storage/mocks"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type TaskTerminatedRetryTestSuite struct {
	suite.Suite
	ctrl *gomock.Controller

	taskStore  *storemocks.MockTaskStore
	jobFactory *cachedmocks.MockJobFactory

	updateGoalStateEngine *goalstatemocks.MockEngine
	taskGoalStateEngine   *goalstatemocks.MockEngine
	jobGoalStateEngine    *goalstatemocks.MockEngine
	goalStateDriver       *driver

	jobID      *peloton.JobID
	instanceID uint32
	updateID   *peloton.UpdateID

	taskEnt         *taskEntity
	cachedJob       *cachedmocks.MockJob
	cachedUpdate    *cachedmocks.MockUpdate
	cachedTask      *cachedmocks.MockTask
	taskConfigV2Ops *objectmocks.MockTaskConfigV2Ops

	jobRuntime      *pbjob.RuntimeInfo
	taskConfig      *pbtask.TaskConfig
	taskRuntime     *pbtask.RuntimeInfo
	lostTaskRuntime *pbtask.RuntimeInfo

	mesosTaskID string
}

func TestTerminatedFailRetryStart(t *testing.T) {
	suite.Run(t, new(TaskTerminatedRetryTestSuite))
}

func (suite *TaskTerminatedRetryTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func (suite *TaskTerminatedRetryTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.taskStore = storemocks.NewMockTaskStore(suite.ctrl)
	suite.jobGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.taskGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.updateGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.cachedTask = cachedmocks.NewMockTask(suite.ctrl)
	suite.cachedUpdate = cachedmocks.NewMockUpdate(suite.ctrl)
	suite.taskConfigV2Ops = objectmocks.NewMockTaskConfigV2Ops(suite.ctrl)
	suite.goalStateDriver = &driver{
		jobEngine:       suite.jobGoalStateEngine,
		taskEngine:      suite.taskGoalStateEngine,
		updateEngine:    suite.updateGoalStateEngine,
		taskStore:       suite.taskStore,
		jobFactory:      suite.jobFactory,
		taskConfigV2Ops: suite.taskConfigV2Ops,
		mtx:             NewMetrics(tally.NoopScope),
		cfg: &Config{
			InitialTaskBackoff: 30 * time.Second,
			MaxTaskBackoff:     60 * time.Minute,
		},
	}
	suite.goalStateDriver.cfg.normalize()
	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.updateID = &peloton.UpdateID{Value: uuid.NewRandom().String()}
	suite.instanceID = uint32(0)
	suite.taskEnt = &taskEntity{
		jobID:      suite.jobID,
		instanceID: suite.instanceID,
		driver:     suite.goalStateDriver,
	}
	suite.mesosTaskID = fmt.Sprintf("%s-%d-%d", suite.jobID.GetValue(), suite.instanceID, 1)
	suite.taskRuntime = &pbtask.RuntimeInfo{
		MesosTaskId:   &mesosv1.TaskID{Value: &suite.mesosTaskID},
		State:         pbtask.TaskState_FAILED,
		GoalState:     pbtask.TaskState_SUCCEEDED,
		ConfigVersion: 1,
		Message:       "testFailure",
		Reason:        mesosv1.TaskStatus_REASON_CONTAINER_LAUNCH_FAILED.String(),
		FailureCount:  5,
	}
	suite.lostTaskRuntime = &pbtask.RuntimeInfo{
		MesosTaskId:   &mesosv1.TaskID{Value: &suite.mesosTaskID},
		State:         pbtask.TaskState_LOST,
		GoalState:     pbtask.TaskState_SUCCEEDED,
		ConfigVersion: 1,
		Message:       "Agent peloton-mesos-agent0 removed",
		Reason:        mesosv1.TaskStatus_REASON_AGENT_REMOVED.String(),
	}
	suite.jobRuntime = &pbjob.RuntimeInfo{
		UpdateID: suite.updateID,
	}
	suite.taskConfig = &pbtask.TaskConfig{}
}

// TestTaskFailRetryStatelessNoTask tests restart when the job is not in cache
func (suite *TaskTerminatedRetryTestSuite) TestTaskTerminatedRetryNoJob() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).Return(nil)

	err := TaskTerminatedRetry(context.Background(), suite.taskEnt)
	suite.Nil(err)
}

// TestTaskFailRetryStatelessNoTask tests restart when the task is not in cache
func (suite *TaskTerminatedRetryTestSuite) TestTaskTerminatedRetryNoTask() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).Return(suite.cachedJob)
	suite.cachedJob.EXPECT().
		AddTask(gomock.Any(), suite.instanceID).Return(nil, fmt.Errorf("fake db error"))
	err := TaskTerminatedRetry(context.Background(), suite.taskEnt)
	suite.Error(err)
}

// TestTaskFailRetryStatelessNoTask tests restart when no task runtime
func (suite *TaskTerminatedRetryTestSuite) TestTaskTerminatedRetryNoTaskRuntime() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).Return(suite.cachedJob)
	suite.cachedJob.EXPECT().
		AddTask(gomock.Any(), suite.instanceID).Return(suite.cachedTask, nil)
	suite.cachedTask.EXPECT().
		GetRuntime(gomock.Any()).Return(nil, fmt.Errorf(""))
	err := TaskTerminatedRetry(context.Background(), suite.taskEnt)
	suite.Error(err)
}

// TestTaskFailRetryStatelessNoTask tests restart when no task config
func (suite *TaskTerminatedRetryTestSuite) TestTaskTerminatedRetryNoTaskConfig() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).Return(suite.cachedJob)
	suite.cachedJob.EXPECT().
		AddTask(gomock.Any(), suite.instanceID).Return(suite.cachedTask, nil)
	suite.cachedTask.EXPECT().
		GetRuntime(gomock.Any()).Return(suite.taskRuntime, nil)
	suite.taskConfigV2Ops.EXPECT().GetTaskConfig(
		gomock.Any(),
		suite.jobID,
		suite.instanceID,
		gomock.Any()).Return(nil, nil, fmt.Errorf(""))

	err := TaskTerminatedRetry(context.Background(), suite.taskEnt)
	suite.Error(err)
}

// TestTaskFailRetryStatelessNoTask tests restart when no update
func (suite *TaskTerminatedRetryTestSuite) TestTaskTerminatedRetryNoUpdate() {
	suite.cachedTask.EXPECT().
		ID().
		Return(uint32(0)).
		AnyTimes()

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).Return(suite.cachedJob)
	suite.cachedJob.EXPECT().
		AddTask(gomock.Any(), suite.instanceID).Return(suite.cachedTask, nil)
	suite.taskRuntime.FailureCount = 2
	suite.taskRuntime.Revision = &peloton.ChangeLog{
		UpdatedAt: uint64(time.Now().Add(-3 * time.Minute).UnixNano()),
	}
	suite.cachedTask.EXPECT().
		GetRuntime(gomock.Any()).Return(suite.taskRuntime, nil)
	suite.taskConfig = &pbtask.TaskConfig{
		RestartPolicy: &pbtask.RestartPolicy{
			MaxFailures: 3,
		},
	}
	suite.taskConfigV2Ops.EXPECT().GetTaskConfig(
		gomock.Any(),
		suite.jobID,
		suite.instanceID,
		gomock.Any()).Return(suite.taskConfig, &models.ConfigAddOn{}, nil)

	suite.cachedJob.EXPECT().
		ID().Return(suite.jobID)

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Do(func(ctx context.Context,
			runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff,
			_ bool) {
			runtimeDiff := runtimeDiffs[suite.instanceID]
			suite.True(
				runtimeDiff[jobmgrcommon.MesosTaskIDField].(*mesosv1.TaskID).GetValue() != suite.mesosTaskID)
			suite.True(
				runtimeDiff[jobmgrcommon.PrevMesosTaskIDField].(*mesosv1.TaskID).GetValue() == suite.mesosTaskID)
			suite.True(
				runtimeDiff[jobmgrcommon.StateField].(pbtask.TaskState) == pbtask.TaskState_INITIALIZED)
		}).Return(nil, nil, nil)

	suite.cachedJob.EXPECT().
		GetJobType().Return(pbjob.JobType_BATCH)

	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := TaskTerminatedRetry(context.Background(), suite.taskEnt)
	suite.Nil(err)
}

// TestTaskTerminatedRetryNoFailure tests restart when there is no failure
func (suite *TaskTerminatedRetryTestSuite) TestTaskTerminatedRetryNoFailure() {
	suite.taskRuntime.FailureCount = 0

	suite.cachedTask.EXPECT().
		ID().
		Return(uint32(0)).
		AnyTimes()

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddTask(gomock.Any(), suite.instanceID).Return(suite.cachedTask, nil)
	suite.taskRuntime.Revision = &peloton.ChangeLog{UpdatedAt: uint64(time.Now().UnixNano())}
	suite.cachedTask.EXPECT().
		GetRuntime(gomock.Any()).Return(suite.taskRuntime, nil)
	suite.taskConfigV2Ops.EXPECT().GetTaskConfig(
		gomock.Any(),
		suite.jobID,
		suite.instanceID,
		gomock.Any()).Return(suite.taskConfig, &models.ConfigAddOn{}, nil)
	suite.cachedJob.EXPECT().
		ID().Return(suite.jobID)

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Do(func(ctx context.Context,
			runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff,
			_ bool) {
			runtimeDiff := runtimeDiffs[suite.instanceID]
			suite.True(
				runtimeDiff[jobmgrcommon.MesosTaskIDField].(*mesosv1.TaskID).GetValue() != suite.mesosTaskID)
			suite.True(
				runtimeDiff[jobmgrcommon.PrevMesosTaskIDField].(*mesosv1.TaskID).GetValue() == suite.mesosTaskID)
			suite.True(
				runtimeDiff[jobmgrcommon.StateField].(pbtask.TaskState) == pbtask.TaskState_INITIALIZED)
		}).Return(nil, nil, nil)

	suite.cachedJob.EXPECT().
		GetJobType().Return(pbjob.JobType_BATCH)

	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := TaskTerminatedRetry(context.Background(), suite.taskEnt)
	suite.Nil(err)
}

// TestLostTaskRetry tests that a lost task is retried
func (suite *TaskTerminatedRetryTestSuite) TestLostTaskRetry() {
	suite.lostTaskRuntime.FailureCount = 0

	suite.cachedTask.EXPECT().
		ID().
		Return(uint32(0)).
		AnyTimes()

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).Return(suite.cachedJob)
	suite.cachedJob.EXPECT().
		AddTask(gomock.Any(), suite.instanceID).Return(suite.cachedTask, nil)
	suite.lostTaskRuntime.Revision = &peloton.ChangeLog{UpdatedAt: uint64(time.Now().UnixNano())}
	suite.cachedTask.EXPECT().
		GetRuntime(gomock.Any()).Return(suite.lostTaskRuntime, nil)
	suite.taskConfigV2Ops.EXPECT().GetTaskConfig(
		gomock.Any(),
		suite.jobID,
		suite.instanceID,
		gomock.Any()).Return(suite.taskConfig, &models.ConfigAddOn{}, nil)

	suite.cachedJob.EXPECT().
		ID().Return(suite.jobID)

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Do(func(ctx context.Context,
			runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff,
			_ bool) {
			runtimeDiff := runtimeDiffs[suite.instanceID]
			suite.True(
				runtimeDiff[jobmgrcommon.MesosTaskIDField].(*mesosv1.TaskID).GetValue() != suite.mesosTaskID)
			suite.True(
				runtimeDiff[jobmgrcommon.PrevMesosTaskIDField].(*mesosv1.TaskID).GetValue() == suite.mesosTaskID)
			suite.True(
				runtimeDiff[jobmgrcommon.StateField].(pbtask.TaskState) == pbtask.TaskState_INITIALIZED)
		}).Return(nil, nil, nil)

	suite.cachedJob.EXPECT().
		GetJobType().Return(pbjob.JobType_BATCH)

	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := TaskTerminatedRetry(context.Background(), suite.taskEnt)
	suite.Nil(err)
}

// TestGetBackoffOverflow tests getBackoff function would not overflow
// when failure count is huge
func (suite *TaskTerminatedRetryTestSuite) TestGetBackoffOverflow() {
	maxFails := uint32(math.MaxUint32)

	for i := maxFails; i > 0; i = i / 2 {
		backOff := getBackoff(
			&pbtask.RuntimeInfo{FailureCount: i},
			30*time.Second,
			60*time.Minute,
		)
		suite.True(backOff >= time.Duration(0))
		suite.True(backOff <= 60*time.Minute)
	}
}
