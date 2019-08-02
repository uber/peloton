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
	"fmt"
	"testing"

	"github.com/uber/peloton/.gen/mesos/v1"
	job2 "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v0/volume"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
	resmocks "github.com/uber/peloton/.gen/peloton/private/resmgrsvc/mocks"

	goalstatemocks "github.com/uber/peloton/pkg/common/goalstate/mocks"
	taskutil "github.com/uber/peloton/pkg/common/util/task"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	storemocks "github.com/uber/peloton/pkg/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type TaskStartTestSuite struct {
	suite.Suite

	ctrl                *gomock.Controller
	jobStore            *storemocks.MockJobStore
	taskStore           *storemocks.MockTaskStore
	jobGoalStateEngine  *goalstatemocks.MockEngine
	taskGoalStateEngine *goalstatemocks.MockEngine
	jobFactory          *cachedmocks.MockJobFactory
	cachedJob           *cachedmocks.MockJob
	cachedConfig        *cachedmocks.MockJobConfigCache
	cachedTask          *cachedmocks.MockTask
	mockVolumeStore     *storemocks.MockPersistentVolumeStore
	goalStateDriver     *driver
	resmgrClient        *resmocks.MockResourceManagerServiceYARPCClient
	jobID               *peloton.JobID
	instanceID          uint32
	taskEnt             *taskEntity
}

func TestTaskStart(t *testing.T) {
	suite.Run(t, new(TaskStartTestSuite))
}

func (suite *TaskStartTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.jobStore = storemocks.NewMockJobStore(suite.ctrl)
	suite.taskStore = storemocks.NewMockTaskStore(suite.ctrl)

	suite.resmgrClient = resmocks.NewMockResourceManagerServiceYARPCClient(suite.ctrl)
	suite.jobGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.taskGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.cachedConfig = cachedmocks.NewMockJobConfigCache(suite.ctrl)
	suite.cachedTask = cachedmocks.NewMockTask(suite.ctrl)
	suite.mockVolumeStore = storemocks.NewMockPersistentVolumeStore(suite.ctrl)

	suite.goalStateDriver = &driver{
		jobEngine:    suite.jobGoalStateEngine,
		taskEngine:   suite.taskGoalStateEngine,
		jobStore:     suite.jobStore,
		taskStore:    suite.taskStore,
		jobFactory:   suite.jobFactory,
		resmgrClient: suite.resmgrClient,
		volumeStore:  suite.mockVolumeStore,
		mtx:          NewMetrics(tally.NoopScope),
		cfg:          &Config{},
	}
	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.instanceID = 0
	suite.taskEnt = &taskEntity{
		jobID:      suite.jobID,
		instanceID: suite.instanceID,
		driver:     suite.goalStateDriver,
	}

	suite.goalStateDriver.cfg.normalize()
}

func (suite *TaskStartTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func (suite *TaskStartTestSuite) TestTaskStartStateless() {
	jobConfig := &job2.JobConfig{
		RespoolID: &peloton.ResourcePoolID{
			Value: "my-respool-id",
		},
	}
	taskInfo := &pbtask.TaskInfo{
		InstanceId: suite.instanceID,
		Config: &pbtask.TaskConfig{
			Volume: &pbtask.PersistentVolumeConfig{},
		},
		Runtime: &pbtask.RuntimeInfo{},
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(suite.cachedConfig, nil)

	suite.cachedConfig.EXPECT().
		GetSLA().
		Return(&job2.SlaConfig{}).
		AnyTimes()

	suite.cachedConfig.EXPECT().
		GetRespoolID().
		Return(jobConfig.RespoolID)

	suite.cachedConfig.EXPECT().
		GetType().
		Return(job2.JobType_SERVICE).
		AnyTimes()

	suite.cachedConfig.EXPECT().
		GetPlacementStrategy().
		Return(job2.PlacementStrategy_PLACEMENT_STRATEGY_INVALID)

	suite.taskStore.EXPECT().
		GetTaskByID(gomock.Any(), fmt.Sprintf("%s-%d", suite.jobID.GetValue(), suite.instanceID)).
		Return(taskInfo, nil)

	request := &resmgrsvc.EnqueueGangsRequest{
		Gangs:   taskutil.ConvertToResMgrGangs([]*pbtask.TaskInfo{taskInfo}, jobConfig),
		ResPool: jobConfig.RespoolID,
	}

	suite.resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), request).
		Return(nil, nil)

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Do(func(ctx context.Context,
			runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff,
			_ bool) {
			suite.Equal(runtimeDiffs[suite.instanceID], jobmgrcommon.RuntimeDiff{
				jobmgrcommon.StateField:   pbtask.TaskState_PENDING,
				jobmgrcommon.MessageField: "Task sent for placement",
			})
		}).Return(nil, nil, nil)

	err := TaskStart(context.Background(), suite.taskEnt)
	suite.NoError(err)
}

func (suite *TaskStartTestSuite) TestTaskStartWithSlaMaxRunningInstances() {
	jobConfig := &job2.JobConfig{
		InstanceCount: 2,
		SLA: &job2.SlaConfig{
			MaximumRunningInstances: 1,
		},
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(suite.cachedConfig, nil)

	suite.cachedConfig.EXPECT().
		GetSLA().
		Return(jobConfig.SLA)

	suite.cachedConfig.EXPECT().
		GetType().
		Return(job2.JobType_BATCH)

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := TaskStart(context.Background(), suite.taskEnt)
	suite.NoError(err)
}

func (suite *TaskStartTestSuite) generateRuntime() *pbtask.RuntimeInfo {
	return &pbtask.RuntimeInfo{
		MesosTaskId: &mesos_v1.TaskID{
			Value: &[]string{"3c8a3c3e-71e3-49c5-9aed-2929823f595c-1-3c8a3c3e-71e3-49c5-9aed-2929823f5957"}[0],
		},
		VolumeID: &peloton.VolumeID{
			Value: "my-volume-id",
		},
	}
}

func (suite *TaskStartTestSuite) generateTaskInfo(
	runtime *pbtask.RuntimeInfo) *pbtask.TaskInfo {
	return &pbtask.TaskInfo{
		InstanceId: suite.instanceID,
		Config: &pbtask.TaskConfig{
			Volume: &pbtask.PersistentVolumeConfig{},
		},
		Runtime: runtime,
	}
}

func (suite *TaskStartTestSuite) prepareTest() {
	runtime := suite.generateRuntime()
	taskInfo := suite.generateTaskInfo(runtime)

	taskID := fmt.Sprintf("%s-%d", suite.jobID.GetValue(), suite.instanceID)
	taskInfos := make(map[string]*pbtask.TaskInfo)
	taskInfos[taskID] = taskInfo

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(suite.cachedConfig, nil)

	suite.cachedConfig.EXPECT().
		GetSLA().
		Return(&job2.SlaConfig{})

	suite.taskStore.EXPECT().
		GetTaskByID(gomock.Any(), fmt.Sprintf("%s-%d", suite.jobID.GetValue(), suite.instanceID)).
		Return(taskInfo, nil)

	suite.mockVolumeStore.EXPECT().
		GetPersistentVolume(gomock.Any(), runtime.VolumeID).Return(&volume.PersistentVolumeInfo{
		State: volume.VolumeState_CREATED,
	}, nil)
}

func (suite *TaskStartTestSuite) TestTaskStartNoJob() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(nil)
	err := TaskStart(context.Background(), suite.taskEnt)
	suite.Nil(err)
}

func (suite *TaskStartTestSuite) TestTaskStartNoConfig() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)
	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(nil, errors.New(""))
	err := TaskStart(context.Background(), suite.taskEnt)
	suite.Error(err)
}

func (suite *TaskStartTestSuite) TestTaskStartNoTaskInfo() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(suite.cachedConfig, nil)

	suite.cachedConfig.EXPECT().
		GetSLA().
		Return(&job2.SlaConfig{})

	suite.taskStore.EXPECT().
		GetTaskByID(gomock.Any(), fmt.Sprintf("%s-%d", suite.jobID.GetValue(), suite.instanceID)).
		Return(nil, errors.New(""))

	err := TaskStart(context.Background(), suite.taskEnt)
	suite.Error(err)

}

func (suite *TaskStartTestSuite) TestTaskStartEnqueueExist() {
	jobConfig := &job2.JobConfig{
		RespoolID: &peloton.ResourcePoolID{
			Value: "my-respool-id",
		},
	}
	taskInfo := &pbtask.TaskInfo{
		InstanceId: suite.instanceID,
		Config: &pbtask.TaskConfig{
			Volume: &pbtask.PersistentVolumeConfig{},
		},
		Runtime: &pbtask.RuntimeInfo{},
	}
	resmgrTask := taskutil.ConvertTaskToResMgrTask(taskInfo, jobConfig)
	resmgrEnqueueFailures := map[string]*resmgrsvc.EnqueueGangsResponse{
		"already_exists": {
			Error: &resmgrsvc.EnqueueGangsResponse_Error{
				Failure: &resmgrsvc.EnqueueGangsFailure{
					Failed: []*resmgrsvc.EnqueueGangsFailure_FailedTask{
						{
							Task:      resmgrTask,
							Message:   "task failed due to alreay exists",
							Errorcode: resmgrsvc.EnqueueGangsFailure_ENQUEUE_GANGS_FAILURE_ERROR_CODE_ALREADY_EXIST,
						},
					},
				},
			},
		},
		"internal_error": {
			Error: &resmgrsvc.EnqueueGangsResponse_Error{
				Failure: &resmgrsvc.EnqueueGangsFailure{
					Failed: []*resmgrsvc.EnqueueGangsFailure_FailedTask{
						{
							Task:      resmgrTask,
							Message:   "task failed due to internal error",
							Errorcode: resmgrsvc.EnqueueGangsFailure_ENQUEUE_GANGS_FAILURE_ERROR_CODE_INTERNAL,
						},
					},
				},
			},
		},
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(suite.cachedConfig, nil).
		AnyTimes()

	suite.cachedConfig.EXPECT().
		GetSLA().
		Return(&job2.SlaConfig{}).
		AnyTimes()

	suite.cachedConfig.EXPECT().
		GetRespoolID().
		Return(jobConfig.RespoolID).
		AnyTimes()

	suite.cachedConfig.EXPECT().
		GetType().
		Return(job2.JobType_SERVICE).
		AnyTimes()

	suite.cachedConfig.EXPECT().
		GetPlacementStrategy().
		Return(job2.PlacementStrategy_PLACEMENT_STRATEGY_INVALID).
		AnyTimes()

	suite.taskStore.EXPECT().
		GetTaskByID(gomock.Any(), fmt.Sprintf("%s-%d", suite.jobID.GetValue(), suite.instanceID)).
		Return(taskInfo, nil).
		AnyTimes()

	for cause, response := range resmgrEnqueueFailures {
		suite.resmgrClient.EXPECT().
			EnqueueGangs(gomock.Any(), gomock.Any()).
			Return(response, nil)
		switch cause {
		case "internal_error":
			err := TaskStart(context.Background(), suite.taskEnt)
			suite.Error(err)
		case "already_exists":
			suite.cachedJob.EXPECT().
				PatchTasks(gomock.Any(), gomock.Any(), false).
				Do(func(ctx context.Context,
					runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff,
					_ bool) {
					suite.Equal(runtimeDiffs[suite.instanceID], jobmgrcommon.RuntimeDiff{
						jobmgrcommon.StateField:   pbtask.TaskState_PENDING,
						jobmgrcommon.MessageField: "Task sent for placement",
					})
				}).Return(nil, nil, nil)

			err := TaskStart(context.Background(), suite.taskEnt)
			suite.NoError(err)
		}
	}
}

// TestTaskStartTaskNotInCache test TaskStart action
// when PatchTasks fails due to task not present in the cache
func (suite *TaskStartTestSuite) TestTaskStartTaskNotInCache() {
	jobConfig := &job2.JobConfig{
		RespoolID: &peloton.ResourcePoolID{
			Value: "my-respool-id",
		},
	}
	taskInfo := &pbtask.TaskInfo{
		InstanceId: suite.instanceID,
		Config: &pbtask.TaskConfig{
			Volume: &pbtask.PersistentVolumeConfig{},
		},
		Runtime: &pbtask.RuntimeInfo{},
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(suite.cachedConfig, nil)

	suite.cachedConfig.EXPECT().
		GetSLA().
		Return(&job2.SlaConfig{}).
		AnyTimes()

	suite.cachedConfig.EXPECT().
		GetRespoolID().
		Return(jobConfig.RespoolID)

	suite.cachedConfig.EXPECT().
		GetType().
		Return(job2.JobType_SERVICE).
		AnyTimes()

	suite.cachedConfig.EXPECT().
		GetPlacementStrategy().
		Return(job2.PlacementStrategy_PLACEMENT_STRATEGY_INVALID)

	suite.taskStore.EXPECT().
		GetTaskByID(gomock.Any(), fmt.Sprintf("%s-%d", suite.jobID.GetValue(), suite.instanceID)).
		Return(taskInfo, nil)

	request := &resmgrsvc.EnqueueGangsRequest{
		Gangs:   taskutil.ConvertToResMgrGangs([]*pbtask.TaskInfo{taskInfo}, jobConfig),
		ResPool: jobConfig.RespoolID,
	}

	suite.resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), request).
		Return(nil, nil)

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Do(func(ctx context.Context,
			runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff,
			_ bool) {
			suite.Equal(runtimeDiffs[suite.instanceID], jobmgrcommon.RuntimeDiff{
				jobmgrcommon.StateField:   pbtask.TaskState_PENDING,
				jobmgrcommon.MessageField: "Task sent for placement",
			})
		}).Return(nil, []uint32{suite.instanceID}, nil)

	err := TaskStart(context.Background(), suite.taskEnt)
	suite.Equal(_errTasksNotInCache, err)
}
