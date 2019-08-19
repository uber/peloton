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

package private

import (
	"context"
	"strconv"
	"testing"

	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	pbupdate "github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	v1alphapeloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/private/jobmgrsvc"
	"github.com/uber/peloton/.gen/peloton/private/models"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"

	"github.com/uber/peloton/pkg/common/api"
	leadermocks "github.com/uber/peloton/pkg/common/leader/mocks"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	goalstatemocks "github.com/uber/peloton/pkg/jobmgr/goalstate/mocks"
	storemocks "github.com/uber/peloton/pkg/storage/mocks"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	testJobName  = "test-job"
	testJobID    = "481d565e-28da-457d-8434-f6bb7faa0e95"
	testUpdateID = "941ff353-ba82-49fe-8f80-fb5bc649b04d"
)

var (
	testPelotonJobID = &peloton.JobID{Value: testJobID}
)

type privateHandlerTestSuite struct {
	suite.Suite

	handler *serviceHandler

	ctrl            *gomock.Controller
	cachedJob       *cachedmocks.MockJob
	cachedWorkflow  *cachedmocks.MockUpdate
	jobFactory      *cachedmocks.MockJobFactory
	candidate       *leadermocks.MockCandidate
	goalStateDriver *goalstatemocks.MockDriver
	jobStore        *storemocks.MockJobStore
	updateStore     *storemocks.MockUpdateStore
	taskStore       *storemocks.MockTaskStore
	jobIndexOps     *objectmocks.MockJobIndexOps
	jobConfigOps    *objectmocks.MockJobConfigOps
	jobRuntimeOps   *objectmocks.MockJobRuntimeOps
}

func (suite *privateHandlerTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.cachedWorkflow = cachedmocks.NewMockUpdate(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.candidate = leadermocks.NewMockCandidate(suite.ctrl)
	suite.goalStateDriver = goalstatemocks.NewMockDriver(suite.ctrl)
	suite.jobStore = storemocks.NewMockJobStore(suite.ctrl)
	suite.updateStore = storemocks.NewMockUpdateStore(suite.ctrl)
	suite.taskStore = storemocks.NewMockTaskStore(suite.ctrl)
	suite.jobIndexOps = objectmocks.NewMockJobIndexOps(suite.ctrl)
	suite.jobConfigOps = objectmocks.NewMockJobConfigOps(suite.ctrl)
	suite.jobRuntimeOps = objectmocks.NewMockJobRuntimeOps(suite.ctrl)
	suite.handler = &serviceHandler{
		jobFactory:      suite.jobFactory,
		candidate:       suite.candidate,
		goalStateDriver: suite.goalStateDriver,
		jobStore:        suite.jobStore,
		updateStore:     suite.updateStore,
		taskStore:       suite.taskStore,
		jobIndexOps:     suite.jobIndexOps,
		jobConfigOps:    suite.jobConfigOps,
		jobRuntimeOps:   suite.jobRuntimeOps,
		rootCtx:         context.Background(),
	}
}

func (suite *privateHandlerTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func TestPrivateServiceHandler(t *testing.T) {
	suite.Run(t, new(privateHandlerTestSuite))
}

// TestGetThrottledPods tests getting the list of throttled pods
func (suite *privateHandlerTestSuite) TestGetThrottledPods() {
	totalJobs := 3
	totalTasks := 3
	cachedJobs := make(map[string]cached.Job)
	cachedMockJobs := make([]*cachedmocks.MockJob, totalJobs)
	cachedConfigs := make([]*cachedmocks.MockJobConfigCache, totalJobs)
	cachedTasks := make(map[uint32]cached.Task)
	cachedMockTasks := make([]*cachedmocks.MockTask, totalTasks)

	for i := 0; i < totalJobs; i++ {
		cachedJob := cachedmocks.NewMockJob(suite.ctrl)
		cachedJobs[strconv.Itoa(i)] = cachedJob
		cachedMockJobs[i] = cachedJob
		cachedConfigs[i] = cachedmocks.NewMockJobConfigCache(suite.ctrl)
	}

	for i := 0; i < totalTasks; i++ {
		cachedTask := cachedmocks.NewMockTask(suite.ctrl)
		cachedTasks[uint32(i)] = cachedTask
		cachedMockTasks[i] = cachedTask
	}

	suite.jobFactory.EXPECT().
		GetAllJobs().
		Return(cachedJobs)

	cachedMockJobs[0].EXPECT().
		GetConfig(gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	for i := 1; i < totalJobs; i++ {
		cachedMockJobs[i].EXPECT().
			GetConfig(gomock.Any()).
			Return(cachedConfigs[i], nil)
	}

	cachedConfigs[1].EXPECT().
		GetType().
		Return(pbjob.JobType_BATCH)

	cachedConfigs[2].EXPECT().
		GetType().
		Return(pbjob.JobType_SERVICE)

	cachedMockJobs[2].EXPECT().
		GetAllTasks().
		Return(cachedTasks)

	cachedMockTasks[0].EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	cachedMockTasks[1].EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbtask.RuntimeInfo{
			State:   pbtask.TaskState_KILLED,
			Message: "not throttled",
		}, nil)

	cachedMockTasks[2].EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbtask.RuntimeInfo{
			State:   pbtask.TaskState_KILLED,
			Message: common.TaskThrottleMessage,
		}, nil)

	resp, err := suite.handler.GetThrottledPods(
		context.Background(),
		&jobmgrsvc.GetThrottledPodsRequest{},
	)
	suite.NoError(err)
	suite.Equal(1, len(resp.GetThrottledPods()))
}

// TestRefreshJobSuccess tests the case of successfully refreshing job
func (suite *privateHandlerTestSuite) TestRefreshJobSuccess() {
	jobConfig := &pbjob.JobConfig{
		InstanceCount: 10,
	}
	configAddOn := &models.ConfigAddOn{}
	jobRuntime := &pbjob.RuntimeInfo{
		State: pbjob.JobState_RUNNING,
	}

	suite.candidate.EXPECT().
		IsLeader().
		Return(true)

	suite.jobRuntimeOps.EXPECT().
		Get(context.Background(), testPelotonJobID).
		Return(jobRuntime, nil)

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), testPelotonJobID, gomock.Any()).
		Return(jobConfig, configAddOn, nil)

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		Update(gomock.Any(), &pbjob.JobInfo{
			Config:  jobConfig,
			Runtime: jobRuntime,
		}, configAddOn,
			nil,
			cached.UpdateCacheOnly).
		Return(nil)

	suite.goalStateDriver.EXPECT().
		EnqueueJob(&peloton.JobID{Value: testJobID}, gomock.Any())

	resp, err := suite.handler.RefreshJob(context.Background(), &jobmgrsvc.RefreshJobRequest{
		JobId: &v1alphapeloton.JobID{Value: testJobID},
	})
	suite.NotNil(resp)
	suite.NoError(err)
}

// TestRefreshJobFailNonLeader tests the failure case of refreshing job
// due to JobMgr is not leader
func (suite *privateHandlerTestSuite) TestRefreshJobFailNonLeader() {
	suite.candidate.EXPECT().
		IsLeader().
		Return(false)
	resp, err := suite.handler.RefreshJob(context.Background(), &jobmgrsvc.RefreshJobRequest{
		JobId: &v1alphapeloton.JobID{Value: testJobID},
	})
	suite.Nil(resp)
	suite.Error(err)
}

// TestRefreshJobGetConfigFail tests the case of failure due to
// failure of getting job config
func (suite *privateHandlerTestSuite) TestRefreshJobGetConfigFail() {
	suite.candidate.EXPECT().
		IsLeader().
		Return(true)

	suite.jobRuntimeOps.EXPECT().
		Get(gomock.Any(), testPelotonJobID).
		Return(&pbjob.RuntimeInfo{}, nil)

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), testPelotonJobID, gomock.Any()).
		Return(nil, nil, yarpcerrors.InternalErrorf("test error"))

	resp, err := suite.handler.RefreshJob(context.Background(), &jobmgrsvc.RefreshJobRequest{
		JobId: &v1alphapeloton.JobID{Value: testJobID},
	})
	suite.Nil(resp)
	suite.Error(err)
}

// TestRefreshJobGetRuntimeFail tests the case of failure due to
// failure of getting job runtime
func (suite *privateHandlerTestSuite) TestRefreshJobGetRuntimeFail() {
	suite.candidate.EXPECT().
		IsLeader().
		Return(true)

	suite.jobRuntimeOps.EXPECT().
		Get(context.Background(), testPelotonJobID).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	resp, err := suite.handler.RefreshJob(context.Background(), &jobmgrsvc.RefreshJobRequest{
		JobId: &v1alphapeloton.JobID{Value: testJobID},
	})
	suite.Nil(resp)
	suite.Error(err)
}

// TestGetJobCacheWithUpdateSuccess test the success case of getting job
// cache which has update
func (suite *privateHandlerTestSuite) TestGetJobCacheWithUpdateSuccess() {
	instanceCount := uint32(10)
	curJobVersion := uint64(1)
	targetJobVersion := uint64(2)
	instancesDone := []uint32{0, 1, 2}
	instancesFailed := []uint32{3, 4}
	instancesCurrent := []uint32{5}
	totalInstances := []uint32{0, 1, 2, 3, 4, 5, 6, 7}
	priority := uint32(2)
	preemptible := true
	revocable := false
	maximumUnavailableInstances := uint32(2)

	suite.jobFactory.EXPECT().
		GetJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			State:     pbjob.JobState_PENDING,
			GoalState: pbjob.JobState_RUNNING,
			UpdateID:  &peloton.UpdateID{Value: testUpdateID},
		}, nil)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&pbjob.JobConfig{
			InstanceCount: instanceCount,
			SLA: &pbjob.SlaConfig{
				Priority:                    priority,
				Preemptible:                 preemptible,
				Revocable:                   revocable,
				MaximumUnavailableInstances: maximumUnavailableInstances,
			},
		}, nil)

	suite.cachedJob.EXPECT().
		GetWorkflow(&peloton.UpdateID{Value: testUpdateID}).
		Return(suite.cachedWorkflow)

	suite.cachedWorkflow.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE)

	suite.cachedWorkflow.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedWorkflow.EXPECT().
		GetPrevState().
		Return(pbupdate.State_INITIALIZED)

	suite.cachedWorkflow.EXPECT().
		GetInstancesDone().
		Return(instancesDone).
		AnyTimes()

	suite.cachedWorkflow.EXPECT().
		GetInstancesFailed().
		Return(instancesFailed).
		AnyTimes()

	suite.cachedWorkflow.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances: totalInstances,
		})

	suite.cachedWorkflow.EXPECT().
		GetInstancesCurrent().
		Return(instancesCurrent)

	suite.cachedWorkflow.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			JobVersion: curJobVersion,
		})

	suite.cachedWorkflow.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			JobVersion: targetJobVersion,
		})

	resp, err := suite.handler.GetJobCache(context.Background(),
		&jobmgrsvc.GetJobCacheRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		})

	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(resp.GetSpec().GetInstanceCount(), instanceCount)
	suite.Equal(resp.GetSpec().GetSla().GetPriority(), priority)
	suite.Equal(resp.GetSpec().GetSla().GetPreemptible(), preemptible)
	suite.Equal(resp.GetSpec().GetSla().GetRevocable(), revocable)
	suite.Equal(resp.GetSpec().GetSla().GetMaximumUnavailableInstances(),
		maximumUnavailableInstances)

	suite.Equal(resp.GetStatus().GetState(), stateless.JobState_JOB_STATE_PENDING)
	suite.Equal(resp.GetStatus().GetDesiredState(), stateless.JobState_JOB_STATE_RUNNING)
	suite.Equal(resp.GetStatus().GetWorkflowStatus().GetState(),
		stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD)
	suite.Equal(resp.GetStatus().GetWorkflowStatus().GetPrevState(),
		stateless.WorkflowState_WORKFLOW_STATE_INITIALIZED)
	suite.Equal(resp.GetStatus().GetWorkflowStatus().GetType(),
		stateless.WorkflowType_WORKFLOW_TYPE_UPDATE)
	suite.Equal(resp.GetStatus().GetWorkflowStatus().GetNumInstancesCompleted(),
		uint32(len(instancesDone)))
	suite.Equal(resp.GetStatus().GetWorkflowStatus().GetNumInstancesFailed(),
		uint32(len(instancesFailed)))
	suite.Equal(resp.GetStatus().GetWorkflowStatus().GetNumInstancesRemaining(),
		uint32(len(totalInstances)-len(instancesFailed)-len(instancesDone)))
	suite.Equal(resp.GetStatus().GetWorkflowStatus().GetInstancesCurrent(),
		instancesCurrent)
	suite.Equal(
		resp.GetStatus().GetWorkflowStatus().GetPrevVersion().GetValue(),
		"1-0-0")
	suite.Equal(
		resp.GetStatus().GetWorkflowStatus().GetVersion().GetValue(),
		"2-0-0")
}

// TestGetJobCacheGetJobFail test the failure case of getting job cache due to
// failure of getting runtime
func (suite *privateHandlerTestSuite) TestGetJobCacheGetRuntimeFail() {
	suite.jobFactory.EXPECT().
		GetJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	resp, err := suite.handler.GetJobCache(context.Background(),
		&jobmgrsvc.GetJobCacheRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		})
	suite.Error(err)
	suite.Nil(resp)
}

// TestGetJobCacheNotFound test the failure case of getting job cache due to
// job cache not found
func (suite *privateHandlerTestSuite) TestGetJobCacheNotFound() {
	suite.jobFactory.EXPECT().
		GetJob(&peloton.JobID{Value: testJobID}).
		Return(nil)

	resp, err := suite.handler.GetJobCache(context.Background(),
		&jobmgrsvc.GetJobCacheRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		})
	suite.Error(err)
	suite.Nil(resp)
	suite.True(yarpcerrors.IsNotFound(err))
}

// TestQueryJobCacheSuccess tests the success case of querying
// job cache
func (suite *privateHandlerTestSuite) TestQueryJobCacheSuccess() {
	job1 := cachedmocks.NewMockJob(suite.ctrl)
	job2 := cachedmocks.NewMockJob(suite.ctrl)
	job3 := cachedmocks.NewMockJob(suite.ctrl)
	job4 := cachedmocks.NewMockJob(suite.ctrl)

	labels1 := []*peloton.Label{{Key: "key1", Value: "val1"}}
	labels2 := []*peloton.Label{{Key: "key2", Value: "val2"}}
	jobName1 := "jobName1"
	jobName2 := "jobName2"

	job1.EXPECT().ID().Return(&peloton.JobID{Value: "job1"}).AnyTimes()
	job2.EXPECT().ID().Return(&peloton.JobID{Value: "job2"}).AnyTimes()
	job3.EXPECT().ID().Return(&peloton.JobID{Value: "job3"}).AnyTimes()
	job4.EXPECT().ID().Return(&peloton.JobID{Value: "job4"}).AnyTimes()

	suite.goalStateDriver.EXPECT().Started().Return(true).AnyTimes()
	suite.jobFactory.EXPECT().GetAllJobs().Return(map[string]cached.Job{
		job1.ID().GetValue(): job1,
		job2.ID().GetValue(): job2,
		job3.ID().GetValue(): job3,
		job4.ID().GetValue(): job4,
	}).AnyTimes()

	//job1 has labels1, name1
	//job2 has labels1, name2
	//job3 has labels2, name1
	//job4 has labels2, name2
	job1.EXPECT().
		GetConfig(gomock.Any()).
		Return(&pbjob.JobConfig{
			Name:   jobName1,
			Labels: labels1,
		}, nil).AnyTimes()
	job2.EXPECT().
		GetConfig(gomock.Any()).
		Return(&pbjob.JobConfig{
			Name:   jobName2,
			Labels: labels1,
		}, nil).AnyTimes()
	job3.EXPECT().
		GetConfig(gomock.Any()).
		Return(&pbjob.JobConfig{
			Name:   jobName1,
			Labels: labels2,
		}, nil).AnyTimes()
	job4.EXPECT().
		GetConfig(gomock.Any()).
		Return(&pbjob.JobConfig{
			Name:   jobName2,
			Labels: labels2,
		}, nil).AnyTimes()

	result, err := suite.handler.QueryJobCache(
		context.Background(),
		&jobmgrsvc.QueryJobCacheRequest{
			Spec: &jobmgrsvc.QueryJobCacheRequest_CacheQuerySpec{
				Labels: api.ConvertLabels(labels1),
			},
		})
	suite.NoError(err)
	suite.Len(result.GetResult(), 2)

	result, err = suite.handler.QueryJobCache(
		context.Background(),
		&jobmgrsvc.QueryJobCacheRequest{
			Spec: &jobmgrsvc.QueryJobCacheRequest_CacheQuerySpec{
				Labels: api.ConvertLabels(labels2),
			},
		})
	suite.NoError(err)
	suite.Len(result.GetResult(), 2)

	result, err = suite.handler.QueryJobCache(
		context.Background(),
		&jobmgrsvc.QueryJobCacheRequest{
			Spec: &jobmgrsvc.QueryJobCacheRequest_CacheQuerySpec{
				Name: jobName1,
			},
		})
	suite.NoError(err)
	suite.Len(result.GetResult(), 2)

	result, err = suite.handler.QueryJobCache(
		context.Background(),
		&jobmgrsvc.QueryJobCacheRequest{
			Spec: &jobmgrsvc.QueryJobCacheRequest_CacheQuerySpec{
				Name: jobName2,
			},
		})
	suite.NoError(err)
	suite.Len(result.GetResult(), 2)

	result, err = suite.handler.QueryJobCache(
		context.Background(),
		&jobmgrsvc.QueryJobCacheRequest{},
	)
	suite.NoError(err)
	suite.Len(result.GetResult(), 4)

	result, err = suite.handler.QueryJobCache(
		context.Background(),
		&jobmgrsvc.QueryJobCacheRequest{
			Spec: &jobmgrsvc.QueryJobCacheRequest_CacheQuerySpec{
				Labels: api.ConvertLabels(labels1),
				Name:   jobName1,
			},
		})
	suite.NoError(err)
	suite.Len(result.GetResult(), 1)
}

// TestQueryJobCacheGoalStateEngineNotStartedFailure tests the case
// cache query fails due to goal state engine not started
func (suite *privateHandlerTestSuite) TestQueryJobCacheGoalStateEngineNotStartedFailure() {
	suite.goalStateDriver.EXPECT().Started().Return(false)
	result, err := suite.handler.QueryJobCache(
		context.Background(),
		&jobmgrsvc.QueryJobCacheRequest{},
	)
	suite.Nil(result)
	suite.Error(err)
}

// TestGetInstanceAvailabilityInfoForJobSuccess tests the success
// case of getting instance availability information for a job
func (suite *privateHandlerTestSuite) TestGetInstanceAvailabilityInfoForJobSuccess() {
	job := cachedmocks.NewMockJob(suite.ctrl)
	instanceAvailabilityMap := map[uint32]jobmgrcommon.InstanceAvailability_Type{
		0: jobmgrcommon.InstanceAvailability_AVAILABLE,
		1: jobmgrcommon.InstanceAvailability_UNAVAILABLE,
		2: jobmgrcommon.InstanceAvailability_KILLED,
		3: jobmgrcommon.InstanceAvailability_DELETED,
		4: jobmgrcommon.InstanceAvailability_INVALID,
	}

	suite.jobFactory.EXPECT().GetJob(testPelotonJobID).Return(job)
	job.EXPECT().
		GetInstanceAvailabilityType(gomock.Any()).
		Return(instanceAvailabilityMap)

	response, err := suite.handler.GetInstanceAvailabilityInfoForJob(
		context.Background(),
		&jobmgrsvc.GetInstanceAvailabilityInfoForJobRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		},
	)
	suite.NoError(err)

	for i, a := range instanceAvailabilityMap {
		suite.Equal(
			jobmgrcommon.InstanceAvailability_name[a],
			response.InstanceAvailabilityMap[i],
		)
	}
}
