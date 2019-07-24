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
	"errors"
	"testing"
	"time"

	mesos_v1 "github.com/uber/peloton/.gen/mesos/v1"
	pb_job "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pb_task "github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/models"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
	res_mocks "github.com/uber/peloton/.gen/peloton/private/resmgrsvc/mocks"
	goalstatemocks "github.com/uber/peloton/pkg/common/goalstate/mocks"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	lmmocks "github.com/uber/peloton/pkg/jobmgr/task/lifecyclemgr/mocks"
	store_mocks "github.com/uber/peloton/pkg/storage/mocks"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"

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
	taskConfigV2Ops     *objectmocks.MockTaskConfigV2Ops
	lm                  *lmmocks.MockManager
	jobConfig           *cachedmocks.MockJobConfigCache
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
	suite.jobConfig = cachedmocks.NewMockJobConfigCache(suite.mockCtrl)
	suite.lm = lmmocks.NewMockManager(suite.mockCtrl)
	suite.taskConfigV2Ops = objectmocks.NewMockTaskConfigV2Ops(suite.mockCtrl)

	suite.goalStateDriver = &driver{
		jobEngine:       suite.jobGoalStateEngine,
		taskEngine:      suite.taskGoalStateEngine,
		jobStore:        suite.jobStore,
		taskStore:       suite.taskStore,
		taskConfigV2Ops: suite.taskConfigV2Ops,
		jobFactory:      suite.jobFactory,
		lm:              suite.lm,
		resmgrClient:    suite.resmgrClient,
		mtx:             NewMetrics(tally.NoopScope),
		cfg:             &Config{},
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
	runtime.Revision = &peloton.ChangeLog{
		UpdatedAt: uint64(time.Now().Add(-suite.goalStateDriver.cfg.LaunchTimeout).UnixNano()),
	}
	config := &pb_task.TaskConfig{}

	for i := 0; i < 2; i++ {
		suite.jobFactory.EXPECT().
			GetJob(suite.jobID).Return(suite.cachedJob)

		suite.cachedJob.EXPECT().
			GetTask(suite.instanceID).Return(suite.cachedTask)

		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).Return(runtime, nil)

		suite.jobFactory.EXPECT().
			GetJob(suite.jobID).Return(suite.cachedJob)

		suite.cachedJob.EXPECT().
			GetTask(suite.instanceID).Return(suite.cachedTask)

		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).Return(runtime, nil)

		suite.jobConfig.EXPECT().GetType().Return(pb_job.JobType_BATCH)

		suite.cachedJob.EXPECT().PatchTasks(gomock.Any(), gomock.Any(), false).Do(
			func(_ context.Context,
				runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff,
				_ bool) {
				for _, runtimeDiff := range runtimeDiffs {
					suite.Equal(oldMesosTaskID, runtimeDiff[jobmgrcommon.PrevMesosTaskIDField])
					suite.NotEqual(oldMesosTaskID, runtimeDiff[jobmgrcommon.MesosTaskIDField])
					suite.Equal(pb_task.TaskState_INITIALIZED, runtimeDiff[jobmgrcommon.StateField])
				}
			}).Return(nil, nil, nil)

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
			suite.taskConfigV2Ops.EXPECT().GetTaskConfig(
				gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(config, &models.ConfigAddOn{}, nil).AnyTimes()
			suite.lm.EXPECT().Kill(
				gomock.Any(),
				oldMesosTaskID.GetValue(),
				"",
				nil,
			).Return(nil).AnyTimes()
		} else {
			// test skip task kill
			suite.taskConfigV2Ops.EXPECT().
				GetTaskConfig(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(nil, nil, errors.New(""))
			suite.lm.EXPECT()
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
		GetRuntime(gomock.Any()).Return(runtime, nil)

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
		GetRuntime(gomock.Any()).Return(runtime, nil)

	suite.resmgrClient.EXPECT().
		UpdateTasksState(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.UpdateTasksStateResponse{}, errors.New("error"))

	err := TaskLaunchRetry(context.Background(),
		suite.getTaskEntity(suite.jobID, suite.instanceID))
	suite.Error(err)
	suite.Equal(err.Error(), "error")
}

func (suite *TestTaskLaunchRetrySuite) TestTaskStartTimeoutForBatchJob() {
	oldMesosTaskID := &mesos_v1.TaskID{
		Value: &[]string{uuid.New()}[0],
	}
	runtime := suite.getRunTime(
		pb_task.TaskState_STARTING,
		pb_task.TaskState_SUCCEEDED,
		oldMesosTaskID)
	runtime.Revision = &peloton.ChangeLog{
		UpdatedAt: uint64(time.Now().Add(-suite.goalStateDriver.cfg.LaunchTimeout).UnixNano()),
	}
	config := &pb_task.TaskConfig{}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetTask(suite.instanceID).Return(suite.cachedTask)

	suite.taskConfigV2Ops.EXPECT().GetTaskConfig(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(config, &models.ConfigAddOn{}, nil).Times(2)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&pb_job.JobConfig{
			Type: pb_job.JobType_BATCH,
		}, nil)

	suite.cachedJob.EXPECT().
		GetTask(suite.instanceID).Return(suite.cachedTask)

	suite.cachedTask.EXPECT().
		GetRuntime(gomock.Any()).Return(runtime, nil)

	suite.cachedTask.EXPECT().
		GetRuntime(gomock.Any()).Return(runtime, nil)

	suite.jobConfig.EXPECT().GetType().Return(pb_job.JobType_BATCH)

	suite.cachedJob.EXPECT().PatchTasks(gomock.Any(), gomock.Any(), false).Do(
		func(_ context.Context,
			runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff,
			_ bool) {
			for _, runtimeDiff := range runtimeDiffs {
				suite.Equal(oldMesosTaskID, runtimeDiff[jobmgrcommon.PrevMesosTaskIDField])
				suite.NotEqual(oldMesosTaskID, runtimeDiff[jobmgrcommon.MesosTaskIDField])
				suite.Equal(pb_task.TaskState_INITIALIZED, runtimeDiff[jobmgrcommon.StateField])
			}
		}).Return(nil, nil, nil)

	suite.cachedJob.EXPECT().
		GetJobType().Return(pb_job.JobType_BATCH)

	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	suite.lm.EXPECT().Kill(
		gomock.Any(),
		oldMesosTaskID.GetValue(),
		"",
		nil,
	).Return(nil).AnyTimes()

	suite.NoError(TaskLaunchRetry(
		context.Background(),
		suite.getTaskEntity(suite.jobID, suite.instanceID)))
}

// TestTaskStartTimeoutForServiceJob tests the case that a job of type service
// get stuck at starting state.
func (suite *TestTaskLaunchRetrySuite) TestTaskStartTimeoutForServiceJob() {
	oldMesosTaskID := &mesos_v1.TaskID{
		Value: &[]string{uuid.New()}[0],
	}
	runtime := suite.getRunTime(
		pb_task.TaskState_STARTING,
		pb_task.TaskState_RUNNING,
		oldMesosTaskID)
	runtime.Revision = &peloton.ChangeLog{
		UpdatedAt: uint64(time.Now().Add(-suite.goalStateDriver.cfg.LaunchTimeout).UnixNano()),
	}
	config := &pb_task.TaskConfig{}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetTask(suite.instanceID).Return(suite.cachedTask)

	suite.taskConfigV2Ops.EXPECT().GetTaskConfig(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(config, &models.ConfigAddOn{}, nil).Times(2)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&pb_job.JobConfig{
			Type: pb_job.JobType_SERVICE,
		}, nil)

	suite.cachedJob.EXPECT().
		GetTask(suite.instanceID).Return(suite.cachedTask)

	suite.cachedTask.EXPECT().
		GetRuntime(gomock.Any()).Return(runtime, nil)

	suite.cachedTask.EXPECT().
		GetRuntime(gomock.Any()).Return(runtime, nil)

	suite.jobConfig.EXPECT().GetType().Return(pb_job.JobType_BATCH)

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

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&pb_job.JobConfig{
			Type: pb_job.JobType_BATCH,
		}, nil)

	suite.cachedTask.EXPECT().
		GetRuntime(gomock.Any()).Return(runtime, nil)

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
		GetRuntime(gomock.Any()).Return(runtime, nil)

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
		Revision: &peloton.ChangeLog{
			UpdatedAt: uint64(time.Now().UnixNano()),
		},
	}
}
