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
	"github.com/uber/peloton/.gen/peloton/private/models"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
	resmocks "github.com/uber/peloton/.gen/peloton/private/resmgrsvc/mocks"

	goalstatemocks "github.com/uber/peloton/pkg/common/goalstate/mocks"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	//"github.com/uber/peloton/pkg/storage/cassandra"
	storemocks "github.com/uber/peloton/pkg/storage/mocks"

	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	//log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

/*var csStore *cassandra.Store

func init() {
	conf := cassandra.MigrateForTest()
	var err error
	csStore, err = cassandra.NewStore(conf, tally.NoopScope)
	if err != nil {
		log.Fatal(err)
	}
}*/

type JobCreateTestSuite struct {
	suite.Suite
	ctrl                *gomock.Controller
	jobStore            *storemocks.MockJobStore
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
}

func TestJobCreateRun(t *testing.T) {
	suite.Run(t, new(JobCreateTestSuite))
}

func (suite *JobCreateTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.jobStore = storemocks.NewMockJobStore(suite.ctrl)
	suite.taskStore = storemocks.NewMockTaskStore(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.jobGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.taskGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.resmgrClient = resmocks.NewMockResourceManagerServiceYARPCClient(suite.ctrl)
	suite.goalStateDriver = &driver{
		jobFactory:   suite.jobFactory,
		jobEngine:    suite.jobGoalStateEngine,
		taskEngine:   suite.taskGoalStateEngine,
		jobStore:     suite.jobStore,
		taskStore:    suite.taskStore,
		mtx:          NewMetrics(tally.NoopScope),
		cfg:          &Config{},
		resmgrClient: suite.resmgrClient,
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

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID.GetValue()).
		Return(suite.jobConfig, &models.ConfigAddOn{}, nil)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		CreateTaskRuntimes(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	suite.cachedJob.EXPECT().
		Update(gomock.Any(), gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Do(func(_ context.Context,
			jobInfo *pbjob.JobInfo,
			_ *models.ConfigAddOn,
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
		PatchTasks(gomock.Any(), gomock.Any()).
		Return(nil)

	err := JobCreateTasks(context.Background(), suite.jobEnt)
	suite.NoError(err)
}

func (suite *JobCreateTestSuite) TestJobCreateGetConfigFailure() {
	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID.GetValue()).
		Return(nil, nil, fmt.Errorf("fake db error"))

	err := JobCreateTasks(context.Background(), suite.jobEnt)
	suite.Error(err)
}

func (suite *JobCreateTestSuite) TestJobCreateTaskConfigCreateFailure() {
	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID.GetValue()).
		Return(suite.jobConfig, &models.ConfigAddOn{}, nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err := JobCreateTasks(context.Background(), suite.jobEnt)
	suite.Error(err)
}

func (suite *JobCreateTestSuite) TestJobCreateGetTasksFailure() {
	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID.GetValue()).
		Return(suite.jobConfig, &models.ConfigAddOn{}, nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
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

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID.GetValue()).
		Return(suite.jobConfig, &models.ConfigAddOn{}, nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
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

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID.GetValue()).
		Return(suite.jobConfig, &models.ConfigAddOn{}, nil)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		CreateTaskRuntimes(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	suite.cachedJob.EXPECT().
		Update(gomock.Any(), gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Return(fmt.Errorf("fake db error"))

	suite.resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		Times(2)

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any()).
		Return(nil)

	err := JobCreateTasks(context.Background(), suite.jobEnt)
	suite.Error(err)
}

// These are integration tests within job manager.
//TODO find a place to put them.
/*func TestJobCreateTasksWithStore(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var jobID = &peloton.JobID{Value: uuid.New()}
	var sla = pb_job.SlaConfig{
		Priority:    22,
		Preemptible: false,
	}
	var taskConfig = pb_task.TaskConfig{
		Resource: &pb_task.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
		},
	}
	var jobConfig = pb_job.JobConfig{
		Name:          "TestValidatorWithStore",
		OwningTeam:    "team6",
		LdapGroups:    []string{"money", "team6", "gsg9"},
		SLA:           &sla,
		DefaultConfig: &taskConfig,
		InstanceCount: 10,
	}

	err := csStore.CreateJob(context.Background(), jobID, &jobConfig, "gsg9")

	resmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)

	j := &job{
		id: jobID,
		m: &manager{
			mtx:          NewMetrics(tally.NoopScope),
			resmgrClient: resmgrClient,
			jobStore:     csStore,
			taskStore:    csStore,
			jobs:         map[string]*job{},
		},
		tasks: map[uint32]*task{},
	}

	resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil)

	reschedule, err := j.RunAction(context.Background(), JobCreateTasks)
	assert.False(t, reschedule)
	assert.NoError(t, err)
	assert.Equal(t, jobConfig.InstanceCount, uint32(len(j.tasks)))

	jobRuntime, err := csStore.GetJobRuntime(context.Background(), jobID)
	assert.Nil(t, err)
	assert.Equal(t, pb_job.JobState_PENDING, jobRuntime.GetState())

	for i := uint32(0); i < jobConfig.InstanceCount; i++ {
		taskInfo, err := csStore.GetTaskForJob(context.Background(), jobID, i)
		assert.Nil(t, err)
		assert.Equal(t, pb_task.TaskState_PENDING, taskInfo[i].Runtime.GetState())
	}
}*/

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

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID.GetValue()).
		Return(suite.jobConfig, &models.ConfigAddOn{}, nil)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		Update(gomock.Any(), gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Do(func(_ context.Context,
			jobInfo *pbjob.JobInfo,
			_ *models.ConfigAddOn,
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
		PatchTasks(gomock.Any(), gomock.Any()).
		Return(nil)

	err := JobCreateTasks(context.Background(), suite.jobEnt)
	suite.NoError(err)
}

// These are integration tests within job manager.
//TODO find a place to put them.
/*func TestJobRecoverWithStore(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var jobID = &peloton.JobID{Value: uuid.New()}
	var sla = pb_job.SlaConfig{
		Priority:    22,
		Preemptible: false,
	}
	var taskConfig = pb_task.TaskConfig{
		Resource: &pb_task.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
		},
	}
	var jobConfig = pb_job.JobConfig{
		Name:          "TestValidatorWithStore",
		OwningTeam:    "team6",
		LdapGroups:    []string{"money", "team6", "gsg9"},
		SLA:           &sla,
		DefaultConfig: &taskConfig,
		InstanceCount: 10,
	}
	ctx := context.Background()

	err := csStore.CreateJob(ctx, jobID, &jobConfig, "gsg9")
	resmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	j := &job{
		id: jobID,
		m: &manager{
			mtx:          NewMetrics(tally.NoopScope),
			resmgrClient: resmgrClient,
			jobStore:     csStore,
			taskStore:    csStore,
			jobs:         map[string]*job{},
		},
		tasks: map[uint32]*task{},
	}

	runtimes := make(map[uint32]*pb_task.RuntimeInfo)
	for i := uint32(0); i < uint32(3); i++ {
		runtime := jobmgr_task.CreateInitializingTask(jobID, i, &jobConfig)
		err := csStore.CreateTaskRuntime(ctx, jobID, i, runtime, jobConfig.OwningTeam)
		assert.NoError(t, err)
		runtimes[i] = runtime
	}

	for i := uint32(7); i < uint32(9); i++ {
		runtime := jobmgr_task.CreateInitializingTask(jobID, i, &jobConfig)
		err := csStore.CreateTaskRuntime(ctx, jobID, i, runtime, jobConfig.OwningTeam)
		assert.NoError(t, err)
		runtimes[i] = runtime
	}

	j.m.SetTasks(j.id, runtimes, UpdateAndSchedule)

	resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil)

	reschedule, err := j.RunAction(context.Background(), JobCreateTasks)
	assert.False(t, reschedule)
	assert.NoError(t, err)

	jobRuntime, err := csStore.GetJobRuntime(context.Background(), jobID)
	assert.Nil(t, err)
	assert.Equal(t, pb_job.JobState_PENDING, jobRuntime.GetState())

	for i := uint32(0); i < jobConfig.InstanceCount; i++ {
		taskInfo, err := csStore.GetTaskForJob(context.Background(), jobID, i)
		assert.Nil(t, err)
		assert.Equal(t, pb_task.TaskState_PENDING, taskInfo[i].GetRuntime().GetState())
	}
}*/

func (suite *JobCreateTestSuite) TestJobMaxRunningInstances() {
	suite.jobConfig.SLA = &pbjob.SlaConfig{
		MaximumRunningInstances: 1,
	}
	emptyTaskInfo := make(map[uint32]*pbtask.TaskInfo)

	suite.taskStore.EXPECT().
		GetTasksForJob(gomock.Any(), suite.jobID).
		Return(emptyTaskInfo, nil)

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID.GetValue()).
		Return(suite.jobConfig, &models.ConfigAddOn{}, nil)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
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
		PatchTasks(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff) {
			suite.Equal(uint32(len(runtimeDiffs)), suite.jobConfig.SLA.MaximumRunningInstances)
			for _, runtimeDiff := range runtimeDiffs {
				suite.Equal(runtimeDiff[jobmgrcommon.StateField], pbtask.TaskState_PENDING)
			}
		}).
		Return(nil)

	suite.cachedJob.EXPECT().
		Update(gomock.Any(), gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Do(func(_ context.Context,
			jobInfo *pbjob.JobInfo,
			_ *models.ConfigAddOn,
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

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID.GetValue()).
		Return(suite.jobConfig, &models.ConfigAddOn{}, nil)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
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
		Update(gomock.Any(), gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Do(func(_ context.Context,
			jobInfo *pbjob.JobInfo,
			_ *models.ConfigAddOn,
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
