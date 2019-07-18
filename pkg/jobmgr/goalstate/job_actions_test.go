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

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/private/models"

	"github.com/uber/peloton/pkg/jobmgr/cached"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"

	goalstatemocks "github.com/uber/peloton/pkg/common/goalstate/mocks"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	storemocks "github.com/uber/peloton/pkg/storage/mocks"
	ormmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

type jobActionsTestSuite struct {
	suite.Suite

	ctrl                  *gomock.Controller
	jobGoalStateEngine    *goalstatemocks.MockEngine
	taskGoalStateEngine   *goalstatemocks.MockEngine
	updateGoalStateEngine *goalstatemocks.MockEngine
	jobFactory            *cachedmocks.MockJobFactory
	goalStateDriver       *driver
	jobID                 *peloton.JobID
	updateID              *peloton.UpdateID
	jobEnt                *jobEntity
	cachedJob             *cachedmocks.MockJob
	cachedTask            *cachedmocks.MockTask
	jobStore              *storemocks.MockJobStore
	updateStore           *storemocks.MockUpdateStore
	activeJobsOps         *ormmocks.MockActiveJobsOps
	jobIndexOps           *ormmocks.MockJobIndexOps
	jobRuntimeOps         *ormmocks.MockJobRuntimeOps
}

func (suite *jobActionsTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())

	suite.jobGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.taskGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.jobStore = storemocks.NewMockJobStore(suite.ctrl)
	suite.updateStore = storemocks.NewMockUpdateStore(suite.ctrl)
	suite.updateGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.activeJobsOps = ormmocks.NewMockActiveJobsOps(suite.ctrl)
	suite.jobIndexOps = ormmocks.NewMockJobIndexOps(suite.ctrl)
	suite.jobRuntimeOps = ormmocks.NewMockJobRuntimeOps(suite.ctrl)

	suite.goalStateDriver = &driver{
		updateStore:   suite.updateStore,
		updateEngine:  suite.updateGoalStateEngine,
		jobStore:      suite.jobStore,
		jobEngine:     suite.jobGoalStateEngine,
		taskEngine:    suite.taskGoalStateEngine,
		jobFactory:    suite.jobFactory,
		activeJobsOps: suite.activeJobsOps,
		jobIndexOps:   suite.jobIndexOps,
		jobRuntimeOps: suite.jobRuntimeOps,
		mtx:           NewMetrics(tally.NoopScope),
		cfg:           &Config{},
	}
	suite.goalStateDriver.cfg.normalize()

	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.updateID = &peloton.UpdateID{Value: uuid.NewRandom().String()}
	suite.jobEnt = &jobEntity{
		id:     suite.jobID,
		driver: suite.goalStateDriver,
	}
	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.cachedTask = cachedmocks.NewMockTask(suite.ctrl)
}

func (suite *jobActionsTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func (suite *jobActionsTestSuite) TestUntrackJobBatch() {
	taskMap := make(map[uint32]cached.Task)
	taskMap[0] = suite.cachedTask

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&job.JobConfig{
			Type: job.JobType_BATCH,
		}, nil)

	suite.cachedJob.EXPECT().
		GetAllTasks().
		Return(taskMap)

	suite.taskGoalStateEngine.EXPECT().
		Delete(gomock.Any()).
		Return()

	suite.jobGoalStateEngine.EXPECT().
		Delete(gomock.Any()).
		Return()

	suite.jobFactory.EXPECT().
		ClearJob(suite.jobID).Return()

	err := JobUntrack(context.Background(), suite.jobEnt)
	suite.NoError(err)
}

func (suite *jobActionsTestSuite) TestUntrackJobStateless() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&job.JobConfig{
			Type: job.JobType_SERVICE,
		}, nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{
			State:     job.JobState_KILLED,
			GoalState: job.JobState_KILLED,
		}, nil)

	// enough call to verify JobUntrack calls JobRuntimeUpdater
	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(nil, errors.New("test error"))

	err := JobUntrack(context.Background(), suite.jobEnt)
	suite.Error(err)
}

// Test JobStateInvalid workflow is as expected
func (suite *jobActionsTestSuite) TestJobStateInvalidAction() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(context.Background()).
		Return(&job.RuntimeInfo{
			State:     job.JobState_KILLING,
			GoalState: job.JobState_RUNNING,
		}, nil)

	err := JobStateInvalid(context.Background(), suite.jobEnt)
	suite.NoError(err)
}

// TestJobRecoverBachJobSuccess tests the success case of recovering
// batch job from UNINITIALIZED state
func (suite *jobActionsTestSuite) TestJobRecoverBachJobSuccess() {
	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&job.JobConfig{
			Type: job.JobType_BATCH,
		}, nil)

	suite.cachedJob.EXPECT().
		Update(
			gomock.Any(),
			&job.JobInfo{Runtime: &job.RuntimeInfo{State: job.JobState_INITIALIZED}},
			nil,
			nil,
			cached.UpdateCacheAndDB).
		Return(nil)

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any())

	err := JobRecover(context.Background(), suite.jobEnt)
	suite.NoError(err)
}

// TestJobRecoverBachJobSuccess tests the success case of recovering
// service job from UNINITIALIZED state
func (suite *jobActionsTestSuite) TestJobRecoverServiceJobSuccess() {
	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&job.JobConfig{
			Type: job.JobType_SERVICE,
		}, nil)

	suite.cachedJob.EXPECT().
		Update(
			gomock.Any(),
			&job.JobInfo{Runtime: &job.RuntimeInfo{State: job.JobState_PENDING}},
			nil,
			nil,
			cached.UpdateCacheAndDB).
		Return(nil)

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any())

	err := JobRecover(context.Background(), suite.jobEnt)
	suite.NoError(err)
}

func (suite *jobActionsTestSuite) TestJobRecoverActionFailToRecover() {
	var configurationVersion uint64 = 1
	taskMap := make(map[uint32]cached.Task)
	taskMap[0] = suite.cachedTask

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&job.JobConfig{}, yarpcerrors.NotFoundErrorf("config not found")).
		Times(2)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{
			UpdateID:             suite.updateID,
			ConfigurationVersion: configurationVersion,
		}, nil)

	suite.updateStore.EXPECT().
		DeleteUpdate(gomock.Any(), suite.updateID, suite.jobID, configurationVersion).
		Return(nil)

	suite.jobStore.EXPECT().
		DeleteJob(gomock.Any(), suite.jobID.GetValue()).
		Return(nil)

	suite.jobIndexOps.EXPECT().
		Delete(gomock.Any(), suite.jobID).
		Return(nil)

	suite.activeJobsOps.EXPECT().
		Delete(gomock.Any(), suite.jobID).
		Return(nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetAllTasks().
		Return(taskMap)

	suite.taskGoalStateEngine.EXPECT().
		Delete(gomock.Any()).
		Return()

	suite.jobGoalStateEngine.EXPECT().
		Delete(gomock.Any()).
		Return()

	suite.jobFactory.EXPECT().
		ClearJob(suite.jobID).Return()

	err := JobRecover(context.Background(), suite.jobEnt)
	suite.NoError(err)
}

// TestDeleteJobFromActiveJobs tests DeleteJobFromActiveJobs goalstate
// action results
func (suite *jobActionsTestSuite) TestDeleteJobFromActiveJobs() {
	tt := []struct {
		typ          job.JobType
		state        job.JobState
		shouldDelete bool
	}{
		{
			typ:          job.JobType_BATCH,
			state:        job.JobState_RUNNING,
			shouldDelete: false,
		},

		{
			typ:          job.JobType_SERVICE,
			state:        job.JobState_RUNNING,
			shouldDelete: false,
		},

		{
			typ:          job.JobType_SERVICE,
			state:        job.JobState_FAILED,
			shouldDelete: false,
		},
		{
			typ:          job.JobType_BATCH,
			state:        job.JobState_SUCCEEDED,
			shouldDelete: true,
		},
		{
			typ:          job.JobType_BATCH,
			state:        job.JobState_FAILED,
			shouldDelete: true,
		},
	}
	for _, test := range tt {
		suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(suite.cachedJob)
		suite.cachedJob.EXPECT().GetRuntime(context.Background()).
			Return(&job.RuntimeInfo{
				State: test.state,
			}, nil)
		suite.cachedJob.EXPECT().
			GetConfig(gomock.Any()).
			Return(&job.JobConfig{
				Type: test.typ,
			}, nil)

		// cachedJob.EXPECT().GetJobType().Return(test.typ)
		if test.shouldDelete {
			suite.activeJobsOps.EXPECT().Delete(gomock.Any(), suite.jobID).Return(nil)
		}
		err := DeleteJobFromActiveJobs(context.Background(), suite.jobEnt)
		suite.NoError(err)
	}
}

// TestDeleteJobFromActiveJobsFailures tests failure scenarios for
// DeleteJobFromActiveJobs goalstate action
func (suite *jobActionsTestSuite) TestDeleteJobFromActiveJobsFailures() {
	// set cached job to nil. this should not return error
	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(nil)
	err := DeleteJobFromActiveJobs(context.Background(), suite.jobEnt)
	suite.NoError(err)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	// simulate GetRuntime error
	suite.cachedJob.EXPECT().
		GetRuntime(context.Background()).
		Return(nil, fmt.Errorf("runtime error"))
	err = DeleteJobFromActiveJobs(context.Background(), suite.jobEnt)
	suite.Error(err)

	// Simulate GetConfig error
	suite.cachedJob.EXPECT().GetRuntime(context.Background()).
		Return(&job.RuntimeInfo{
			State: job.JobState_FAILED,
		}, nil)
	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(nil, fmt.Errorf("config error"))
	err = DeleteJobFromActiveJobs(context.Background(), suite.jobEnt)
	suite.Error(err)

	// Simulate storage error
	suite.cachedJob.EXPECT().
		GetRuntime(context.Background()).
		Return(&job.RuntimeInfo{
			State: job.JobState_FAILED,
		}, nil)
	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&job.JobConfig{
			Type: job.JobType_BATCH,
		}, nil)
	suite.activeJobsOps.EXPECT().Delete(gomock.Any(), suite.jobID).
		Return(fmt.Errorf("DB error"))
	err = DeleteJobFromActiveJobs(context.Background(), suite.jobEnt)
	suite.Error(err)
}

// TestStartJobSuccess tests the success case of start job action
func (suite *jobActionsTestSuite) TestStartJobSuccess() {
	taskMap := make(map[uint32]cached.Task)
	taskMap[0] = suite.cachedTask
	jobRuntime := &job.RuntimeInfo{
		StateVersion: 1,
		State:        job.JobState_RUNNING,
	}

	suite.cachedJob.EXPECT().ID().Return(suite.jobID).AnyTimes()
	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(suite.cachedJob)
	suite.cachedJob.EXPECT().GetAllTasks().Return(taskMap)
	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, nil, nil)
	suite.taskGoalStateEngine.EXPECT().Enqueue(gomock.Any(), gomock.Any())
	suite.cachedJob.EXPECT().GetRuntime(gomock.Any()).Return(jobRuntime, nil)
	suite.cachedJob.EXPECT().
		CompareAndSetRuntime(gomock.Any(), jobRuntime).
		Return(nil, nil)
	suite.jobGoalStateEngine.EXPECT().Enqueue(gomock.Any(), gomock.Any())

	suite.NoError(JobStart(context.Background(), suite.jobEnt))
}

// TestStartJobGetJobFailure tests the failure case of start job action
// due to failure to get job from the job factory.
func (suite *jobActionsTestSuite) TestStartJobGetJobFailure() {
	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(nil)

	suite.NoError(JobStart(context.Background(), suite.jobEnt))
}

// TestStartJobPatchTasksFailure tests the failure case
// of start job action due to error while patching tasks
func (suite *jobActionsTestSuite) TestStartJobPatchTasksFailure() {
	cachedJob := cachedmocks.NewMockJob(suite.ctrl)
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
	taskMap := make(map[uint32]cached.Task)
	taskMap[0] = cachedTask

	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(cachedJob)
	cachedJob.EXPECT().GetAllTasks().Return(taskMap)
	cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(JobStart(context.Background(), suite.jobEnt))
}

// TestStartJobGetRuntimeFailure tests the failure case
// of start job action due to error getting job runtime
func (suite *jobActionsTestSuite) TestStartJobGetRuntimeFailure() {
	cachedJob := cachedmocks.NewMockJob(suite.ctrl)
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
	taskMap := make(map[uint32]cached.Task)
	taskMap[0] = cachedTask

	cachedJob.EXPECT().ID().Return(suite.jobID).AnyTimes()
	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(cachedJob)
	cachedJob.EXPECT().GetAllTasks().Return(taskMap)
	cachedJob.EXPECT().PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, nil, nil)
	suite.taskGoalStateEngine.EXPECT().Enqueue(gomock.Any(), gomock.Any())
	cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(JobStart(context.Background(), suite.jobEnt))
}

// TestStartJobRuntimeUpdateFailure tests the failure case
// of start job action due to error updating job runtime
func (suite *jobActionsTestSuite) TestStartJobRuntimeUpdateFailure() {
	cachedJob := cachedmocks.NewMockJob(suite.ctrl)
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
	taskMap := make(map[uint32]cached.Task)
	taskMap[0] = cachedTask
	jobRuntime := &job.RuntimeInfo{
		StateVersion: 1,
		State:        job.JobState_RUNNING,
	}

	cachedJob.EXPECT().ID().Return(suite.jobID).AnyTimes()
	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(cachedJob)
	cachedJob.EXPECT().GetAllTasks().Return(taskMap)
	cachedJob.EXPECT().PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, nil, nil)
	suite.taskGoalStateEngine.EXPECT().Enqueue(gomock.Any(), gomock.Any())
	cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil).
		Times(jobmgrcommon.MaxConcurrencyErrorRetry)
	cachedJob.EXPECT().
		CompareAndSetRuntime(gomock.Any(), jobRuntime).
		Return(nil, jobmgrcommon.UnexpectedVersionError).
		Times(jobmgrcommon.MaxConcurrencyErrorRetry)

	suite.Error(JobStart(context.Background(), suite.jobEnt))
}

func TestJobActions(t *testing.T) {
	suite.Run(t, new(jobActionsTestSuite))
}

// TestJobDeleteSuccess tests success case of deleting a running job
func (suite *jobActionsTestSuite) TestJobDeleteSuccess() {
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
	cachedTasks := make(map[uint32]cached.Task)
	cachedTasks[0] = cachedTask

	suite.cachedJob.EXPECT().ID().Return(suite.jobID).AnyTimes()

	gomock.InOrder(
		suite.jobFactory.EXPECT().
			GetJob(suite.jobID).
			Return(suite.cachedJob),

		suite.cachedJob.EXPECT().
			Delete(gomock.Any()).
			Return(nil),

		suite.cachedJob.EXPECT().
			GetAllTasks().
			Return(cachedTasks),

		suite.taskGoalStateEngine.EXPECT().
			Delete(gomock.Any()),

		suite.jobGoalStateEngine.EXPECT().
			Delete(suite.jobEnt),

		suite.jobFactory.EXPECT().
			ClearJob(suite.jobID),
	)

	suite.NoError(JobDelete(context.Background(), suite.jobEnt))
}

// TestJobDeleteGetJobFailure tests the failure case of deleting
// job due to failure getting the job from cache
func (suite *jobActionsTestSuite) TestJobDeleteGetJobFailure() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(nil)

	suite.NoError(JobDelete(context.Background(), suite.jobEnt))
}

// TestJobDeleteStoreError tests the failure case of
// deleting job due to error while deleting job from store
func (suite *jobActionsTestSuite) TestJobDeleteStoreError() {
	suite.cachedJob.EXPECT().ID().Return(suite.jobID).AnyTimes()

	gomock.InOrder(
		suite.jobFactory.EXPECT().
			GetJob(suite.jobID).
			Return(suite.cachedJob),
		suite.cachedJob.EXPECT().
			Delete(gomock.Any()).
			Return(yarpcerrors.InternalErrorf("test error")),
	)

	suite.Error(JobDelete(context.Background(), suite.jobEnt))
}

// TestJobReloadRuntimeSuccess tests the success
// case of reloading job runtime into cache
func (suite *jobActionsTestSuite) TestJobReloadRuntimeSuccess() {
	jobRuntime := &job.RuntimeInfo{
		State:     job.JobState_RUNNING,
		GoalState: job.JobState_SUCCEEDED,
	}

	gomock.InOrder(
		suite.jobFactory.EXPECT().
			AddJob(suite.jobID).
			Return(suite.cachedJob),

		suite.jobRuntimeOps.EXPECT().
			Get(context.Background(), suite.jobID).
			Return(jobRuntime, nil),

		suite.cachedJob.EXPECT().
			Update(
				gomock.Any(),
				&job.JobInfo{
					Runtime: jobRuntime,
				},
				nil,
				nil,
				cached.UpdateCacheOnly,
			).Return(nil),

		suite.jobGoalStateEngine.EXPECT().
			Enqueue(gomock.Any(), gomock.Any()),
	)

	suite.NoError(JobReloadRuntime(context.Background(), suite.jobEnt))
}

// TestJobReloadGetJobRuntimeFailure tests the failure case of reloading
// job runtime into cache due to error while getting job runtime from store
func (suite *jobActionsTestSuite) TestJobReloadGetJobRuntimeFailure() {
	gomock.InOrder(
		suite.jobFactory.EXPECT().
			AddJob(suite.jobID).
			Return(suite.cachedJob),

		suite.jobRuntimeOps.EXPECT().
			Get(context.Background(), suite.jobID).
			Return(nil, yarpcerrors.InternalErrorf("test error")),

		suite.cachedJob.EXPECT().
			Update(
				gomock.Any(),
				&job.JobInfo{},
				nil,
				nil,
				cached.UpdateCacheOnly,
			).Return(nil),

		suite.jobGoalStateEngine.EXPECT().
			Enqueue(gomock.Any(), gomock.Any()),
	)

	suite.NoError(JobReloadRuntime(context.Background(), suite.jobEnt))
}

// TestJobReloadRuntimeUpdateFailure tests the failure case of
// reloading job runtime into cache due to error while updating cache
func (suite *jobActionsTestSuite) TestJobReloadRuntimeUpdateFailure() {
	jobRuntime := &job.RuntimeInfo{
		State:     job.JobState_RUNNING,
		GoalState: job.JobState_SUCCEEDED,
	}

	gomock.InOrder(
		suite.jobFactory.EXPECT().
			AddJob(suite.jobID).
			Return(suite.cachedJob),

		suite.jobRuntimeOps.EXPECT().
			Get(context.Background(), suite.jobID).
			Return(jobRuntime, nil),

		suite.cachedJob.EXPECT().
			Update(
				gomock.Any(),
				&job.JobInfo{
					Runtime: jobRuntime,
				}, nil,
				nil,
				cached.UpdateCacheOnly,
			).Return(yarpcerrors.InternalErrorf("test error")),
	)

	suite.Error(JobReloadRuntime(context.Background(), suite.jobEnt))
}

// TestJobReloadRuntimeJobNotFound tests the failure case of
// reloading job runtime into cache due to runtime not found error
func (suite *jobActionsTestSuite) TestJobReloadRuntimeJobNotFound() {
	taskMap := make(map[uint32]cached.Task)
	taskMap[0] = suite.cachedTask

	gomock.InOrder(
		suite.jobFactory.EXPECT().
			AddJob(suite.jobID).
			Return(suite.cachedJob),

		suite.jobRuntimeOps.EXPECT().
			Get(context.Background(), suite.jobID).
			Return(nil, yarpcerrors.NotFoundErrorf("test error")),

		suite.jobFactory.EXPECT().
			AddJob(suite.jobID).
			Return(suite.cachedJob),

		suite.cachedJob.EXPECT().
			GetConfig(gomock.Any()).
			Return(&job.JobConfig{
				Type: job.JobType_SERVICE,
			}, nil),

		suite.cachedJob.EXPECT().
			Update(
				gomock.Any(),
				&job.JobInfo{Runtime: &job.RuntimeInfo{State: job.JobState_PENDING}},
				nil,
				nil,
				cached.UpdateCacheAndDB).
			Return(nil),

		suite.jobGoalStateEngine.EXPECT().
			Enqueue(gomock.Any(), gomock.Any()),
	)

	suite.NoError(JobReloadRuntime(context.Background(), suite.jobEnt))
}

// TestEnqueueJobUpdateStatelessSuccess tests the success
// case of enqueuing a job update for a stateless job
func (suite *jobActionsTestSuite) TestEnqueueJobUpdateStatelessSuccess() {
	testUpdateID := &peloton.UpdateID{
		Value: uuid.NewRandom().String(),
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&job.JobConfig{
			Type: job.JobType_SERVICE,
		}, nil)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{
			UpdateID: testUpdateID,
		}, nil)

	suite.updateStore.EXPECT().
		GetUpdateProgress(gomock.Any(), testUpdateID).
		Return(&models.UpdateModel{
			State: update.State_ROLLING_FORWARD,
		}, nil)

	suite.updateGoalStateEngine.
		EXPECT().
		Enqueue(gomock.Any(), gomock.Any())

	suite.NoError(EnqueueJobUpdate(context.Background(), suite.jobEnt))
}

// TestEnqueueJobUpdateStatelessSuccess tests the
// EnqueueJobUpdate action for a batch job
func (suite *jobActionsTestSuite) TestEnqueueJobUpdateBatchNoop() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&job.JobConfig{
			Type: job.JobType_BATCH,
		}, nil)

	suite.NoError(EnqueueJobUpdate(context.Background(), suite.jobEnt))
}

// TestEnqueueJobUpdateGetJobFailure tests the failure case of
// enqueuing job update due to failure to get job from cache
func (suite *jobActionsTestSuite) TestEnqueueJobUpdateGetJobFailure() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(nil)

	suite.NoError(EnqueueJobUpdate(context.Background(), suite.jobEnt))
}

// TestEnqueueJobUpdateGetConfigFailure tests the failure case
// of enqueuing job update due to error while getting job config
func (suite *jobActionsTestSuite) TestEnqueueJobUpdateGetConfigFailure() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(EnqueueJobUpdate(context.Background(), suite.jobEnt))
}

// TestEnqueueJobUpdateStatelessGetRuntimeFailure tests the failure case of
// enqueuing a job update for a stateless job due to error while getting job runtime
func (suite *jobActionsTestSuite) TestEnqueueJobUpdateStatelessGetRuntimeFailure() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&job.JobConfig{
			Type: job.JobType_SERVICE,
		}, nil)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(EnqueueJobUpdate(context.Background(), suite.jobEnt))
}

// TestEnqueueJobUpdateStatelessNoUpdate tests the enqueue-job-update
// action for a stateless job when the job does not have any update
func (suite *jobActionsTestSuite) TestEnqueueJobUpdateStatelessNoUpdate() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&job.JobConfig{
			Type: job.JobType_SERVICE,
		}, nil)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{}, nil)

	suite.NoError(EnqueueJobUpdate(context.Background(), suite.jobEnt))
}

// TestEnqueueJobUpdateStatelessUpdateGetFailure tests the failure case
// of enqueuing job update due to error while reading update info from DB
func (suite *jobActionsTestSuite) TestEnqueueJobUpdateStatelessUpdateGetFailure() {
	testUpdateID := &peloton.UpdateID{
		Value: uuid.NewRandom().String(),
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&job.JobConfig{
			Type: job.JobType_SERVICE,
		}, nil)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{
			UpdateID: testUpdateID,
		}, nil)

	suite.updateStore.EXPECT().
		GetUpdateProgress(gomock.Any(), testUpdateID).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(EnqueueJobUpdate(context.Background(), suite.jobEnt))
}

// TestEnqueueJobUpdateStatelessTerminalUpdate tests the enqueue-job-update
// action for a stateless job when there is no active update
func (suite *jobActionsTestSuite) TestEnqueueJobUpdateStatelessTerminalUpdate() {
	testUpdateID := &peloton.UpdateID{
		Value: uuid.NewRandom().String(),
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&job.JobConfig{
			Type: job.JobType_SERVICE,
		}, nil)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{
			UpdateID: testUpdateID,
		}, nil)

	suite.updateStore.EXPECT().
		GetUpdateProgress(gomock.Any(), testUpdateID).
		Return(&models.UpdateModel{
			State: update.State_SUCCEEDED,
		}, nil)

	suite.NoError(EnqueueJobUpdate(context.Background(), suite.jobEnt))
}

// TestJobKillAndDeleteTerminatedJobWithRunningTasks tests delete
// a terminated job with tasks that may still be started
func (suite *jobActionsTestSuite) TestJobKillAndDeleteTerminatedJobWithRunningTasks() {
	instanceCount := uint32(2)
	cachedTasks := make(map[uint32]cached.Task)
	mockTasks := make(map[uint32]*cachedmocks.MockTask)
	for i := uint32(0); i < instanceCount; i++ {
		cachedTask := cachedmocks.NewMockTask(suite.ctrl)
		mockTasks[i] = cachedTask
		cachedTasks[i] = cachedTask
	}

	jobRuntime := &job.RuntimeInfo{
		State:     job.JobState_SUCCEEDED,
		GoalState: job.JobState_DELETED,
	}

	runtimes := make(map[uint32]*task.RuntimeInfo)
	runtimes[0] = &task.RuntimeInfo{
		State:     task.TaskState_SUCCEEDED,
		GoalState: task.TaskState_RUNNING,
	}
	runtimes[1] = &task.RuntimeInfo{
		State:     task.TaskState_SUCCEEDED,
		GoalState: task.TaskState_RUNNING,
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedJob.EXPECT().
		GetAllTasks().
		Return(cachedTasks).
		AnyTimes()

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	suite.cachedJob.EXPECT().
		Update(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			cached.UpdateCacheAndDB).
		Do(func(_ context.Context,
			jobInfo *job.JobInfo,
			_ *models.ConfigAddOn,
			_ *stateless.JobSpec,
			_ cached.UpdateRequest) {
			suite.Equal(jobInfo.Runtime.State, job.JobState_KILLED)
		}).
		Return(nil)

	for i := uint32(0); i < instanceCount; i++ {
		mockTasks[i].EXPECT().
			GetRuntime(gomock.Any()).
			Return(runtimes[i], nil).
			AnyTimes()
	}

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, nil, nil)

	suite.cachedJob.EXPECT().
		GetJobType().
		Return(job.JobType_SERVICE)

	suite.cachedJob.EXPECT().
		Delete(gomock.Any()).
		Return(nil)

	suite.taskGoalStateEngine.EXPECT().
		Delete(gomock.Any()).
		Times(int(instanceCount))

	suite.jobGoalStateEngine.EXPECT().
		Delete(suite.jobEnt)

	suite.jobFactory.EXPECT().
		ClearJob(suite.jobID)

	err := JobKillAndDelete(context.Background(), suite.jobEnt)
	suite.NoError(err)
}

// TestJobKillAndUntrackTerminatedJobWithNonTerminatedTasks tests untracks
// a terminated job with tasks that may still be started
func (suite *jobActionsTestSuite) TestJobKillAndUntrackTerminatedJobWithNonTerminatedTasks() {
	instanceCount := uint32(2)
	cachedTasks := make(map[uint32]cached.Task)
	mockTasks := make(map[uint32]*cachedmocks.MockTask)
	for i := uint32(0); i < instanceCount; i++ {
		cachedTask := cachedmocks.NewMockTask(suite.ctrl)
		mockTasks[i] = cachedTask
		cachedTasks[i] = cachedTask
	}

	runtimes := make(map[uint32]*task.RuntimeInfo)
	runtimes[0] = &task.RuntimeInfo{
		State:     task.TaskState_FAILED,
		GoalState: task.TaskState_RUNNING,
	}
	runtimes[1] = &task.RuntimeInfo{
		State:     task.TaskState_FAILED,
		GoalState: task.TaskState_RUNNING,
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedJob.EXPECT().
		GetAllTasks().
		Return(cachedTasks).
		AnyTimes()

	for i := uint32(0); i < instanceCount; i++ {
		mockTasks[i].EXPECT().
			GetRuntime(gomock.Any()).
			Return(runtimes[i], nil).
			AnyTimes()
	}

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, nil, nil)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&job.JobConfig{
			Type: job.JobType_SERVICE,
		}, nil)

	// enough to verity JobRuntimeUpdater is called
	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, errors.New("test error"))

	err := JobKillAndUntrack(context.Background(), suite.jobEnt)
	suite.Error(err)
}
