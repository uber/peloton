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
	"testing"

	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/private/models"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	resmocks "github.com/uber/peloton/.gen/peloton/private/resmgrsvc/mocks"
	goalstatemocks "github.com/uber/peloton/pkg/common/goalstate/mocks"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	storemocks "github.com/uber/peloton/pkg/storage/mocks"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	taskutil "github.com/uber/peloton/pkg/common/util/task"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type JobCreateTestSuite struct {
	suite.Suite
	ctrl                *gomock.Controller
	taskStore           *storemocks.MockTaskStore
	jobGoalStateEngine  *goalstatemocks.MockEngine
	taskGoalStateEngine *goalstatemocks.MockEngine
	jobFactory          *cachedmocks.MockJobFactory
	jobID               *peloton.JobID
	cachedJob           *cachedmocks.MockJob
	cachedTask          *cachedmocks.MockTask
	resmgrClient        *resmocks.MockResourceManagerServiceYARPCClient
	goalStateDriver     *driver
	jobEnt              *jobEntity
	instanceCount       uint32
	jobConfig           *pbjob.JobConfig
	jobConfigOps        *objectmocks.MockJobConfigOps
}

func TestJobCreateRun(t *testing.T) {
	suite.Run(t, new(JobCreateTestSuite))
}

func (suite *JobCreateTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.jobConfigOps = objectmocks.NewMockJobConfigOps(suite.ctrl)
	suite.taskStore = storemocks.NewMockTaskStore(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.jobGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.taskGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.resmgrClient = resmocks.NewMockResourceManagerServiceYARPCClient(suite.ctrl)
	suite.goalStateDriver = &driver{
		jobFactory:   suite.jobFactory,
		jobEngine:    suite.jobGoalStateEngine,
		taskEngine:   suite.taskGoalStateEngine,
		taskStore:    suite.taskStore,
		mtx:          NewMetrics(tally.NoopScope),
		cfg:          &Config{},
		resmgrClient: suite.resmgrClient,
		jobConfigOps: suite.jobConfigOps,
	}
	suite.goalStateDriver.cfg.normalize()

	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.cachedTask = cachedmocks.NewMockTask(suite.ctrl)
	suite.jobEnt = &jobEntity{
		id:     suite.jobID,
		driver: suite.goalStateDriver,
	}
	suite.instanceCount = uint32(4)
	suite.jobConfig = &pbjob.JobConfig{
		OwningTeam:    "team6",
		LdapGroups:    []string{"team1", "team2", "team3"},
		InstanceCount: suite.instanceCount,
		Type:          pbjob.JobType_BATCH,
	}
}

func (suite *JobCreateTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func (suite *JobCreateTestSuite) TestJobCreateTasks() {
	emptyTaskInfo := make(map[uint32]*pbtask.TaskInfo)

	suite.taskStore.EXPECT().
		GetTasksForJob(gomock.Any(), suite.jobID).
		Return(emptyTaskInfo, nil)

	suite.jobConfigOps.EXPECT().
		GetResultCurrentVersion(gomock.Any(), suite.jobID).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig:   suite.jobConfig,
			ConfigAddOn: &models.ConfigAddOn{},
		}, nil)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			nil).
		Return(nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		CreateTaskRuntimes(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	suite.cachedJob.EXPECT().
		Update(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			nil,
			cached.UpdateCacheAndDB).
		Do(func(_ context.Context,
			jobInfo *pbjob.JobInfo,
			_ *models.ConfigAddOn,
			_ *stateless.JobSpec,
			_ cached.UpdateRequest) {
			suite.Equal(jobInfo.Runtime.State, pbjob.JobState_PENDING)
		}).
		Return(nil)

	suite.resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		Times(2)

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, nil, nil)

	err := JobCreateTasks(context.Background(), suite.jobEnt)
	suite.NoError(err)
}

func (suite *JobCreateTestSuite) TestJobCreateGetConfigFailure() {
	suite.jobConfigOps.EXPECT().
		GetResultCurrentVersion(gomock.Any(), suite.jobID).
		Return(nil, fmt.Errorf("fake db error"))

	err := JobCreateTasks(context.Background(), suite.jobEnt)
	suite.Error(err)
}

func (suite *JobCreateTestSuite) TestJobCreateTaskConfigCreateFailure() {
	suite.jobConfigOps.EXPECT().
		GetResultCurrentVersion(gomock.Any(), suite.jobID).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig:   suite.jobConfig,
			ConfigAddOn: &models.ConfigAddOn{},
		}, nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			nil).
		Return(fmt.Errorf("fake db error"))

	err := JobCreateTasks(context.Background(), suite.jobEnt)
	suite.Error(err)
}

func (suite *JobCreateTestSuite) TestJobCreateGetTasksFailure() {
	suite.jobConfigOps.EXPECT().
		GetResultCurrentVersion(gomock.Any(), suite.jobID).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig:   suite.jobConfig,
			ConfigAddOn: &models.ConfigAddOn{},
		}, nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			nil).
		Return(nil)

	suite.taskStore.EXPECT().
		GetTasksForJob(gomock.Any(), suite.jobID).
		Return(nil, fmt.Errorf("fake db error"))

	err := JobCreateTasks(context.Background(), suite.jobEnt)
	suite.Error(err)
}

func (suite *JobCreateTestSuite) TestJobCreateResmgrFailure() {
	emptyTaskInfo := make(map[uint32]*pbtask.TaskInfo)

	suite.taskStore.EXPECT().
		GetTasksForJob(gomock.Any(), suite.jobID).
		Return(emptyTaskInfo, nil)

	suite.jobConfigOps.EXPECT().
		GetResultCurrentVersion(gomock.Any(), suite.jobID).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig:   suite.jobConfig,
			ConfigAddOn: &models.ConfigAddOn{},
		}, nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			nil).
		Return(nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		CreateTaskRuntimes(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	suite.resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("fake db error"))

	err := JobCreateTasks(context.Background(), suite.jobEnt)
	suite.Error(err)
}

func (suite *JobCreateTestSuite) TestJobCreateUpdateFailure() {
	emptyTaskInfo := make(map[uint32]*pbtask.TaskInfo)

	suite.taskStore.EXPECT().
		GetTasksForJob(gomock.Any(), suite.jobID).
		Return(emptyTaskInfo, nil)

	suite.jobConfigOps.EXPECT().
		GetResultCurrentVersion(gomock.Any(), suite.jobID).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig:   suite.jobConfig,
			ConfigAddOn: &models.ConfigAddOn{},
		}, nil)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			nil).
		Return(nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		CreateTaskRuntimes(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	suite.cachedJob.EXPECT().
		Update(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			nil,
			cached.UpdateCacheAndDB).
		Return(fmt.Errorf("fake db error"))

	suite.resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		Times(2)

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, nil, nil)

	err := JobCreateTasks(context.Background(), suite.jobEnt)
	suite.Error(err)
}

func (suite *JobCreateTestSuite) TestJobRecover() {
	taskInfos := make(map[uint32]*pbtask.TaskInfo)
	taskInfos[0] = &pbtask.TaskInfo{
		Runtime: &pbtask.RuntimeInfo{
			State:     pbtask.TaskState_RUNNING,
			GoalState: pbtask.TaskState_SUCCEEDED,
		},
		InstanceId: 0,
		JobId:      suite.jobID,
	}
	taskInfos[1] = &pbtask.TaskInfo{
		Runtime: &pbtask.RuntimeInfo{
			State:     pbtask.TaskState_INITIALIZED,
			GoalState: pbtask.TaskState_SUCCEEDED,
		},
		InstanceId: 1,
		JobId:      suite.jobID,
	}

	suite.taskStore.EXPECT().
		GetTasksForJob(gomock.Any(), suite.jobID).
		Return(taskInfos, nil)

	suite.jobConfigOps.EXPECT().
		GetResultCurrentVersion(gomock.Any(), suite.jobID).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig:   suite.jobConfig,
			ConfigAddOn: &models.ConfigAddOn{},
		}, nil)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			nil).
		Return(nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		Update(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			nil,
			cached.UpdateCacheAndDB).
		Do(func(_ context.Context,
			jobInfo *pbjob.JobInfo,
			_ *models.ConfigAddOn,
			_ *stateless.JobSpec,
			_ cached.UpdateRequest) {
			suite.Equal(jobInfo.Runtime.State, pbjob.JobState_PENDING)
		}).
		Return(nil)

	suite.cachedJob.EXPECT().
		GetTask(uint32(1)).Return(nil)

	suite.cachedJob.EXPECT().
		ReplaceTasks(gomock.Any(), false).
		Return(nil)

	suite.cachedJob.EXPECT().
		CreateTaskRuntimes(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	suite.resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		Times(2)

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, nil, nil)

	err := JobCreateTasks(context.Background(), suite.jobEnt)
	suite.NoError(err)
}

func (suite *JobCreateTestSuite) TestJobMaxRunningInstances() {
	suite.jobConfig.SLA = &pbjob.SlaConfig{
		MaximumRunningInstances: 1,
	}
	emptyTaskInfo := make(map[uint32]*pbtask.TaskInfo)

	suite.taskStore.EXPECT().
		GetTasksForJob(gomock.Any(), suite.jobID).
		Return(emptyTaskInfo, nil)

	suite.jobConfigOps.EXPECT().
		GetResultCurrentVersion(gomock.Any(), suite.jobID).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig:   suite.jobConfig,
			ConfigAddOn: &models.ConfigAddOn{},
		}, nil)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			nil).
		Return(nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		CreateTaskRuntimes(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	suite.resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		Times(2)

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Do(func(ctx context.Context,
			runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff,
			_ bool) {
			suite.Equal(uint32(len(runtimeDiffs)), suite.jobConfig.SLA.MaximumRunningInstances)
			for _, runtimeDiff := range runtimeDiffs {
				suite.Equal(runtimeDiff[jobmgrcommon.StateField], pbtask.TaskState_PENDING)
			}
		}).Return(nil, nil, nil)

	suite.cachedJob.EXPECT().
		Update(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			nil,
			cached.UpdateCacheAndDB).
		Do(func(_ context.Context,
			jobInfo *pbjob.JobInfo,
			_ *models.ConfigAddOn,
			_ *stateless.JobSpec,
			_ cached.UpdateRequest) {
			suite.Equal(jobInfo.Runtime.State, pbjob.JobState_PENDING)
		}).
		Return(nil)

	err := JobCreateTasks(context.Background(), suite.jobEnt)
	suite.NoError(err)
}

func (suite *JobCreateTestSuite) TestJobRecoverMaxRunningInstances() {
	suite.jobConfig.SLA = &pbjob.SlaConfig{
		MaximumRunningInstances: 1,
	}

	taskInfos := make(map[uint32]*pbtask.TaskInfo)
	taskInfos[0] = &pbtask.TaskInfo{
		Runtime: &pbtask.RuntimeInfo{
			State:     pbtask.TaskState_RUNNING,
			GoalState: pbtask.TaskState_SUCCEEDED,
		},
		InstanceId: 0,
		JobId:      suite.jobID,
	}
	taskInfos[1] = &pbtask.TaskInfo{
		Runtime: &pbtask.RuntimeInfo{
			State:     pbtask.TaskState_INITIALIZED,
			GoalState: pbtask.TaskState_SUCCEEDED,
		},
		InstanceId: 1,
		JobId:      suite.jobID,
	}

	suite.taskStore.EXPECT().
		GetTasksForJob(gomock.Any(), suite.jobID).
		Return(taskInfos, nil)

	suite.jobConfigOps.EXPECT().
		GetResultCurrentVersion(gomock.Any(), suite.jobID).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig:   suite.jobConfig,
			ConfigAddOn: &models.ConfigAddOn{},
		}, nil)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			nil).
		Return(nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetTask(uint32(1)).Return(nil)

	suite.cachedJob.EXPECT().
		ReplaceTasks(gomock.Any(), false).
		Return(nil)

	suite.cachedJob.EXPECT().
		Update(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			nil,
			cached.UpdateCacheAndDB).
		Do(func(_ context.Context,
			jobInfo *pbjob.JobInfo,
			_ *models.ConfigAddOn,
			_ *stateless.JobSpec,
			_ cached.UpdateRequest) {
			suite.Equal(jobInfo.Runtime.State, pbjob.JobState_PENDING)
		}).
		Return(nil)

	suite.cachedJob.EXPECT().
		GetJobType().Return(pbjob.JobType_BATCH)

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	suite.cachedJob.EXPECT().
		CreateTaskRuntimes(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	suite.cachedJob.EXPECT().
		GetJobType().Return(pbjob.JobType_BATCH)

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := JobCreateTasks(context.Background(), suite.jobEnt)
	suite.NoError(err)
}

func (suite *JobCreateTestSuite) TestJobCreateExistTasks() {
	emptyTaskInfo := make(map[uint32]*pbtask.TaskInfo)
	maxRunningInstances := 4
	suite.jobConfig = &pbjob.JobConfig{
		OwningTeam:    "team6",
		LdapGroups:    []string{"team1", "team2", "team3"},
		InstanceCount: uint32(maxRunningInstances),
		Type:          pbjob.JobType_BATCH,
	}
	suite.jobConfig.SLA = &pbjob.SlaConfig{
		MaximumRunningInstances: uint32(maxRunningInstances),
	}

	resmgrTasks := []*resmgr.Task{}
	for i := 0; i < maxRunningInstances; i++ {
		taskInfo := &pbtask.TaskInfo{
			Runtime: &pbtask.RuntimeInfo{
				State:     pbtask.TaskState_INITIALIZED,
				GoalState: pbtask.TaskState_SUCCEEDED,
			},
			InstanceId: uint32(i),
			JobId:      suite.jobID,
		}
		resmgrTasks = append(resmgrTasks, taskutil.ConvertTaskToResMgrTask(taskInfo, suite.jobConfig))
	}

	wrongTask := &pbtask.TaskInfo{
		Runtime: &pbtask.RuntimeInfo{
			State:     pbtask.TaskState_INITIALIZED,
			GoalState: pbtask.TaskState_SUCCEEDED,
		},
		InstanceId: uint32(0),
		JobId:      &peloton.JobID{Value: uuid.NewRandom().String()},
	}
	resmgrTasks = append(resmgrTasks, taskutil.ConvertTaskToResMgrTask(wrongTask, suite.jobConfig))

	failedGangs := []*resmgrsvc.EnqueueGangsFailure_FailedTask{
		{
			Task:      resmgrTasks[0],
			Message:   "task0 failed due to gang fail",
			Errorcode: resmgrsvc.EnqueueGangsFailure_ENQUEUE_GANGS_FAILURE_ERROR_CODE_FAILED_DUE_TO_GANG_FAILED,
		},
		{
			Task:      resmgrTasks[1],
			Message:   "task1 failed due to already exist",
			Errorcode: resmgrsvc.EnqueueGangsFailure_ENQUEUE_GANGS_FAILURE_ERROR_CODE_ALREADY_EXIST,
		},
		{
			Task:      resmgrTasks[2],
			Message:   "task2 failed due to gang fail",
			Errorcode: resmgrsvc.EnqueueGangsFailure_ENQUEUE_GANGS_FAILURE_ERROR_CODE_FAILED_DUE_TO_GANG_FAILED,
		},
		{
			Task:      resmgrTasks[3],
			Message:   "task3 failed due to already exist",
			Errorcode: resmgrsvc.EnqueueGangsFailure_ENQUEUE_GANGS_FAILURE_ERROR_CODE_ALREADY_EXIST,
		},
		{
			Task:      resmgrTasks[4],
			Message:   "task4 wrong task failed due to gang failure",
			Errorcode: resmgrsvc.EnqueueGangsFailure_ENQUEUE_GANGS_FAILURE_ERROR_CODE_ALREADY_EXIST,
		},
	}
	resmgrResponse := &resmgrsvc.EnqueueGangsResponse{
		Error: &resmgrsvc.EnqueueGangsResponse_Error{
			Failure: &resmgrsvc.EnqueueGangsFailure{
				Failed: failedGangs,
			},
		},
	}

	suite.jobConfigOps.EXPECT().
		GetResultCurrentVersion(gomock.Any(), suite.jobID).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig:   suite.jobConfig,
			ConfigAddOn: &models.ConfigAddOn{},
		}, nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		Times(2)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			nil).
		Return(nil)

	suite.taskStore.EXPECT().
		GetTasksForJob(gomock.Any(), suite.jobID).
		Return(emptyTaskInfo, nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		CreateTaskRuntimes(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	suite.resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(resmgrResponse, nil)

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Do(func(ctx context.Context,
			runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff,
			_ bool) {
			instIDs := []uint32{}
			suite.Equal(len(runtimeDiffs), 2)
			for i, runtimeDiff := range runtimeDiffs {
				suite.Equal(runtimeDiff[jobmgrcommon.StateField], pbtask.TaskState_PENDING)
				instIDs = append(instIDs, i)
			}
			suite.ElementsMatch(instIDs, []uint32{uint32(1), uint32(3)})
		}).Return(nil, nil, nil)

	err := JobCreateTasks(context.Background(), suite.jobEnt)
	suite.Error(err)
}

func (suite *JobCreateTestSuite) TestJobCreateResmgrFailureResponse() {
	emptyTaskInfo := make(map[uint32]*pbtask.TaskInfo)
	resmgrResponse := &resmgrsvc.EnqueueGangsResponse{
		Error: &resmgrsvc.EnqueueGangsResponse_Error{
			NotFound: &resmgrsvc.ResourcePoolNotFound{
				Id:      nil,
				Message: "resource pool ID can't be nil",
			},
		},
	}

	suite.taskStore.EXPECT().
		GetTasksForJob(gomock.Any(), suite.jobID).
		Return(emptyTaskInfo, nil)

	suite.jobConfigOps.EXPECT().
		GetResultCurrentVersion(gomock.Any(), suite.jobID).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig:   suite.jobConfig,
			ConfigAddOn: &models.ConfigAddOn{},
		}, nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			nil).
		Return(nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		CreateTaskRuntimes(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	suite.resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(resmgrResponse, nil)

	err := JobCreateTasks(context.Background(), suite.jobEnt)
	suite.Error(err)
}

// TestJobRecoverTaskNotInCache tests JobRecover action when PatchTasks
// for a few tasks fails due to some tasks not present in the cache
func (suite *JobCreateTestSuite) TestJobRecoverTaskNotInCache() {
	taskInfos := make(map[uint32]*pbtask.TaskInfo)
	taskInfos[0] = &pbtask.TaskInfo{
		Runtime: &pbtask.RuntimeInfo{
			State:     pbtask.TaskState_RUNNING,
			GoalState: pbtask.TaskState_SUCCEEDED,
		},
		InstanceId: 0,
		JobId:      suite.jobID,
	}
	taskInfos[1] = &pbtask.TaskInfo{
		Runtime: &pbtask.RuntimeInfo{
			State:     pbtask.TaskState_INITIALIZED,
			GoalState: pbtask.TaskState_SUCCEEDED,
		},
		InstanceId: 1,
		JobId:      suite.jobID,
	}

	suite.taskStore.EXPECT().
		GetTasksForJob(gomock.Any(), suite.jobID).
		Return(taskInfos, nil)

	suite.jobConfigOps.EXPECT().
		GetResultCurrentVersion(gomock.Any(), suite.jobID).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig:   suite.jobConfig,
			ConfigAddOn: &models.ConfigAddOn{},
		}, nil)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			nil).
		Return(nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetTask(uint32(1)).Return(nil)

	suite.cachedJob.EXPECT().
		ReplaceTasks(gomock.Any(), false).
		Return(nil)

	suite.cachedJob.EXPECT().
		CreateTaskRuntimes(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	suite.resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		Times(2)

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, []uint32{1}, nil)

	err := JobCreateTasks(context.Background(), suite.jobEnt)
	suite.Equal(_errTasksNotInCache, errors.Cause(err))
}
