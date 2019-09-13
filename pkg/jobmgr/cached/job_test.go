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

package cached

import (
	"context"
	"fmt"
	"testing"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	pbupdate "github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	v1alphapeloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/peloton/private/models"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/api"
	versionutil "github.com/uber/peloton/pkg/common/util/entityversion"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	storemocks "github.com/uber/peloton/pkg/storage/mocks"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

var dbError = yarpcerrors.UnavailableErrorf("db error")

const (
	testMesosTaskID    = "941ff353-ba82-49fe-8f80-fb5bc649b04d-1-2"
	testNewMesosTaskID = "941ff353-ba82-49fe-8f80-fb5bc649b04d-1-3"
	testUpdateID       = "941ff353-ba82-49fe-8f80-fb5bc649b04d"
)

type jobTestSuite struct {
	suite.Suite

	ctrl               *gomock.Controller
	jobStore           *storemocks.MockJobStore
	taskStore          *storemocks.MockTaskStore
	updateStore        *storemocks.MockUpdateStore
	activeJobsOps      *objectmocks.MockActiveJobsOps
	jobIndexOps        *objectmocks.MockJobIndexOps
	jobNameToIDOps     *objectmocks.MockJobNameToIDOps
	jobConfigOps       *objectmocks.MockJobConfigOps
	jobRuntimeOps      *objectmocks.MockJobRuntimeOps
	jobUpdateEventsOps *objectmocks.MockJobUpdateEventsOps
	taskConfigV2Ops    *objectmocks.MockTaskConfigV2Ops

	jobID     *peloton.JobID
	job       *job
	listeners []*FakeJobListener
}

func TestJob(t *testing.T) {
	suite.Run(t, new(jobTestSuite))
}

func (suite *jobTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.jobStore = storemocks.NewMockJobStore(suite.ctrl)
	suite.taskStore = storemocks.NewMockTaskStore(suite.ctrl)
	suite.updateStore = storemocks.NewMockUpdateStore(suite.ctrl)
	suite.activeJobsOps = objectmocks.NewMockActiveJobsOps(suite.ctrl)
	suite.jobIndexOps = objectmocks.NewMockJobIndexOps(suite.ctrl)
	suite.jobNameToIDOps = objectmocks.NewMockJobNameToIDOps(suite.ctrl)
	suite.jobConfigOps = objectmocks.NewMockJobConfigOps(suite.ctrl)
	suite.jobRuntimeOps = objectmocks.NewMockJobRuntimeOps(suite.ctrl)
	suite.jobUpdateEventsOps = objectmocks.NewMockJobUpdateEventsOps(suite.ctrl)
	suite.taskConfigV2Ops = objectmocks.NewMockTaskConfigV2Ops(suite.ctrl)
	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.listeners = append(suite.listeners,
		new(FakeJobListener),
		new(FakeJobListener))
	suite.job = suite.initializeJob(
		suite.jobStore,
		suite.taskStore,
		suite.updateStore,
		suite.activeJobsOps,
		suite.jobIndexOps,
		suite.jobNameToIDOps,
		suite.jobConfigOps,
		suite.jobRuntimeOps,
		suite.jobUpdateEventsOps,
		suite.taskConfigV2Ops,
		suite.jobID)
}

func (suite *jobTestSuite) TearDownTest() {
	suite.listeners = nil
	suite.ctrl.Finish()
}

func (suite *jobTestSuite) initializeJob(
	jobStore *storemocks.MockJobStore,
	taskStore *storemocks.MockTaskStore,
	updateStore *storemocks.MockUpdateStore,
	activeJobsOps *objectmocks.MockActiveJobsOps,
	jobIndexOps *objectmocks.MockJobIndexOps,
	jobNameToIDOps *objectmocks.MockJobNameToIDOps,
	jobConfigOps *objectmocks.MockJobConfigOps,
	jobRuntimeOps *objectmocks.MockJobRuntimeOps,
	jobUpdateEventsOps *objectmocks.MockJobUpdateEventsOps,
	taskConfigV2Ops *objectmocks.MockTaskConfigV2Ops,
	jobID *peloton.JobID) *job {
	j := &job{
		id: jobID,
		jobFactory: &jobFactory{
			mtx:                NewMetrics(tally.NoopScope),
			jobStore:           jobStore,
			taskStore:          taskStore,
			updateStore:        updateStore,
			activeJobsOps:      activeJobsOps,
			jobIndexOps:        jobIndexOps,
			jobNameToIDOps:     jobNameToIDOps,
			jobConfigOps:       jobConfigOps,
			jobRuntimeOps:      jobRuntimeOps,
			jobUpdateEventsOps: jobUpdateEventsOps,
			taskConfigV2Ops:    taskConfigV2Ops,
			running:            true,
			jobs:               map[string]*job{},
		},
		tasks: map[uint32]*task{},
		config: &cachedConfig{
			changeLog: &peloton.ChangeLog{
				CreatedAt: uint64(time.Now().UnixNano()),
				UpdatedAt: uint64(time.Now().UnixNano()),
				Version:   1,
			},
		},
		runtime: &pbjob.RuntimeInfo{
			ConfigurationVersion: 1,
			WorkflowVersion:      2,
			State:                pbjob.JobState_INITIALIZED,
			GoalState:            pbjob.JobState_RUNNING,
			StateVersion:         1,
			DesiredStateVersion:  2,
		},
		workflows: map[string]*update{},
	}
	for _, l := range suite.listeners {
		j.jobFactory.listeners = append(j.jobFactory.listeners, l)
	}
	j.jobFactory.jobs[j.id.GetValue()] = j
	return j
}

func initializeRuntimes(instanceCount uint32, state pbtask.TaskState) map[uint32]*pbtask.RuntimeInfo {
	runtimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < instanceCount; i++ {
		runtime := &pbtask.RuntimeInfo{
			State: state,
		}
		runtimes[i] = runtime
	}
	return runtimes
}

func initializeCurrentRuntime(state pbtask.TaskState) *pbtask.RuntimeInfo {
	runtime := &pbtask.RuntimeInfo{
		State: state,
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}
	return runtime
}

func initializeTaskInfos(instanceCount uint32, state pbtask.TaskState) map[uint32]*pbtask.TaskInfo {
	var labels []*peloton.Label

	labels = append(labels, initializeLabel("key", "value"))
	taskInfos := make(map[uint32]*pbtask.TaskInfo)
	for i := uint32(0); i < instanceCount; i++ {
		taskInfo := &pbtask.TaskInfo{
			Runtime: &pbtask.RuntimeInfo{
				State: state,
				Revision: &peloton.ChangeLog{
					CreatedAt: uint64(time.Now().UnixNano()),
					UpdatedAt: uint64(time.Now().UnixNano()),
					Version:   1,
				},
			},
			Config: &pbtask.TaskConfig{
				Labels: labels,
			},
		}
		taskInfos[i] = taskInfo
	}
	return taskInfos
}

func initializeDiffs(instanceCount uint32, state pbtask.TaskState) map[uint32]jobmgrcommon.RuntimeDiff {
	diffs := make(map[uint32]jobmgrcommon.RuntimeDiff)
	for i := uint32(0); i < instanceCount; i++ {
		diff := jobmgrcommon.RuntimeDiff{
			jobmgrcommon.StateField: state,
		}
		diffs[i] = diff
	}
	return diffs
}

// checkListeners verifies that listeners received the correct data
func (suite *jobTestSuite) checkListeners() {
	suite.NotZero(len(suite.listeners))
	for i, l := range suite.listeners {
		msg := fmt.Sprintf("Listener %d", i)
		jobSummary, updateInfo := suite.job.generateJobSummaryFromCache(suite.job.runtime, suite.job.runtime.GetUpdateID())
		s := api.ConvertJobSummary(jobSummary, updateInfo)

		if l.jobType == pbjob.JobType_SERVICE {
			suite.Equal(s, l.statelessJobSummary, msg)
		}

		if l.jobType == pbjob.JobType_BATCH {
			suite.Equal(suite.jobID, l.jobID, msg)
			suite.Equal(jobSummary, l.jobSummary, msg)
		}
	}
}

// checkListenersNotCalled verifies that listeners did not get invoked
func (suite *jobTestSuite) checkListenersNotCalled() {
	suite.NotZero(len(suite.listeners))
	for i, l := range suite.listeners {
		msg := fmt.Sprintf("Listener %d", i)
		suite.Nil(l.jobID, msg)
		suite.Nil(l.jobSummary, msg)
		suite.Nil(l.statelessJobSummary, msg)
	}
}

// TestJobFetchID tests fetching job ID.
func (suite *jobTestSuite) TestJobFetchID() {
	// Test fetching ID
	suite.Equal(suite.jobID, suite.job.ID())
}

// TestJobAddTask tests adding a task and recovering it as well
func (suite *jobTestSuite) TestJobAddTask() {
	instID := uint32(1)
	runtime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_RUNNING,
	}

	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, instID).
		Return(runtime, nil)

	t, err := suite.job.AddTask(context.Background(), instID)
	suite.NoError(err)
	suite.Equal(runtime, (t.(*task)).runtime)
	suite.Equal(t, suite.job.tasks[instID])
}

// TestJobAddTaskDBError tests returning error from DB while fetching the
// task runtime when adding a task and recovering it
func (suite *jobTestSuite) TestJobAddTaskDBError() {
	instID := uint32(1)
	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, instID).
		Return(nil, fmt.Errorf("fake db error"))

	t, err := suite.job.AddTask(context.Background(), instID)
	suite.Error(err)
	suite.EqualError(err, "fake db error")
	suite.Nil(t)
}

// TestJobAddTaskNotFound tests adding a task not in DB
func (suite *jobTestSuite) TestJobAddTaskNotFound() {
	instID := uint32(5)
	suite.job.config = nil
	jobConfig := &pbjob.JobConfig{
		InstanceCount: 2,
		Type:          pbjob.JobType_SERVICE,
	}
	suite.jobConfigOps.EXPECT().Get(
		gomock.Any(),
		suite.jobID,
		suite.job.runtime.GetConfigurationVersion()).
		Return(jobConfig, &models.ConfigAddOn{}, nil)
	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, instID).
		Return(nil, yarpcerrors.NotFoundErrorf("not found"))

	t, err := suite.job.AddTask(context.Background(), instID)
	suite.True(yarpcerrors.IsInvalidArgument(err))
	suite.Equal(err, InstanceIDExceedsInstanceCountError)
	suite.Nil(t)
}

// TestJobAddTaskJobConfigGetError tests returning error from DB during
// the fetch of job configuration when adding and recovering a task
func (suite *jobTestSuite) TestJobAddTaskJobConfigGetError() {
	instID := uint32(5)
	suite.job.config = nil
	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, instID).
		Return(nil, yarpcerrors.NotFoundErrorf("not found"))
	suite.jobConfigOps.EXPECT().Get(
		gomock.Any(),
		suite.jobID,
		suite.job.runtime.GetConfigurationVersion()).
		Return(nil, nil, fmt.Errorf("fake db error"))

	t, err := suite.job.AddTask(context.Background(), instID)
	suite.Error(err)
	suite.EqualError(err, "fake db error")
	suite.Nil(t)
}

// TestJobSetAndFetchConfigAndRuntime tests setting and fetching
// job configuration and runtime.
func (suite *jobTestSuite) TestJobSetAndFetchConfigAndRuntime() {
	// Test setting and fetching job config and runtime
	instanceCount := uint32(10)
	maxRunningInstances := uint32(2)
	maxRunningTime := uint32(5)
	jobConfig := &pbjob.JobConfig{
		SLA: &pbjob.SlaConfig{
			MaximumRunningInstances: maxRunningInstances,
			MaxRunningTime:          maxRunningTime,
		},
		InstanceCount: instanceCount,
		Type:          pbjob.JobType_BATCH,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		ChangeLog: &peloton.ChangeLog{
			Version: 1,
		},
	}
	jobRuntime := &pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		GoalState:            pbjob.JobState_SUCCEEDED,
		UpdateID:             &peloton.UpdateID{Value: uuid.NewRandom().String()},
		ConfigurationVersion: jobConfig.GetChangeLog().GetVersion(),
	}
	jobInfo := &pbjob.JobInfo{
		Runtime: jobRuntime,
		Config:  jobConfig,
	}

	suite.job.config = nil
	suite.job.runtime = nil

	suite.job.Update(
		context.Background(),
		jobInfo,
		&models.ConfigAddOn{},
		nil,
		UpdateCacheOnly)
	actJobRuntime, _ := suite.job.GetRuntime(context.Background())
	actJobConfig, _ := suite.job.GetConfig(context.Background())
	suite.Equal(jobRuntime, actJobRuntime)
	suite.Equal(instanceCount, actJobConfig.GetInstanceCount())
	suite.Equal(pbjob.JobType_BATCH, suite.job.GetJobType())
	suite.Equal(maxRunningInstances, actJobConfig.GetSLA().GetMaximumRunningInstances())
	suite.Equal(maxRunningTime, actJobConfig.GetSLA().GetMaxRunningTime())
	suite.Equal(pbjob.JobType_BATCH, actJobConfig.GetType())
	suite.Equal(jobConfig.RespoolID.Value, actJobConfig.GetRespoolID().Value)
	suite.Equal(jobRuntime.UpdateID.Value, actJobRuntime.GetUpdateID().Value)
	suite.checkListenersNotCalled()
}

// TestJobDBError tests DB errors during job operations.
func (suite *jobTestSuite) TestJobDBError() {
	jobRuntime := &pbjob.RuntimeInfo{
		State:     pbjob.JobState_RUNNING,
		GoalState: pbjob.JobState_SUCCEEDED,
	}

	// Test db error in fetching job runtime
	suite.jobRuntimeOps.EXPECT().
		Get(context.Background(), suite.jobID).
		Return(nil, dbError)
	// reset runtime to trigger load from db
	suite.job.runtime = nil
	actJobRuntime, err := suite.job.GetRuntime(context.Background())
	suite.Error(err)

	suite.jobRuntimeOps.EXPECT().
		Get(context.Background(), suite.jobID).
		Return(&pbjob.RuntimeInfo{
			State:     pbjob.JobState_INITIALIZED,
			GoalState: pbjob.JobState_SUCCEEDED,
		}, nil)
	// Test updating job runtime in DB and cache
	suite.jobRuntimeOps.EXPECT().
		Upsert(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.State, jobRuntime.State)
			suite.Equal(runtime.GoalState, jobRuntime.GoalState)
		}).
		Return(nil)
	suite.jobIndexOps.EXPECT().
		Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)
	err = suite.job.Update(
		context.Background(),
		&pbjob.JobInfo{Runtime: jobRuntime},
		nil,
		nil,
		UpdateCacheAndDB)
	suite.NoError(err)
	actJobRuntime, err = suite.job.GetRuntime(context.Background())
	suite.Equal(jobRuntime.State, actJobRuntime.GetState())
	suite.Equal(jobRuntime.GoalState, actJobRuntime.GetGoalState())
	suite.NoError(err)
	suite.checkListeners()

	// Test error in DB while update job runtime
	suite.jobRuntimeOps.EXPECT().
		Upsert(gomock.Any(), suite.jobID, gomock.Any()).
		Return(dbError)
	jobRuntime.State = pbjob.JobState_SUCCEEDED
	err = suite.job.Update(
		context.Background(),
		&pbjob.JobInfo{Runtime: jobRuntime},
		nil,
		nil,
		UpdateCacheAndDB)
	suite.Error(err)
	suite.Nil(suite.job.runtime)
	suite.Nil(suite.job.config)
}

// TestJobUpdateRuntimeWithNoRuntimeCache tests update job which has
// no existing cache
func (suite *jobTestSuite) TestJobUpdateRuntimeWithNoCache() {
	jobRuntime := &pbjob.RuntimeInfo{
		State:     pbjob.JobState_RUNNING,
		GoalState: pbjob.JobState_SUCCEEDED,
	}

	suite.jobRuntimeOps.EXPECT().
		Get(context.Background(), suite.jobID).
		Return(&pbjob.RuntimeInfo{
			State:     pbjob.JobState_INITIALIZED,
			GoalState: pbjob.JobState_SUCCEEDED,
		}, nil)

	suite.jobRuntimeOps.EXPECT().
		Upsert(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.State, jobRuntime.State)
			suite.Equal(runtime.GoalState, jobRuntime.GoalState)
		}).
		Return(nil)
	suite.jobIndexOps.EXPECT().
		Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	suite.job.runtime = nil
	err := suite.job.Update(
		context.Background(),
		&pbjob.JobInfo{Runtime: jobRuntime},
		nil,
		nil,
		UpdateCacheAndDB)
	suite.NoError(err)
	suite.Equal(suite.job.runtime.State, jobRuntime.State)
	suite.Equal(suite.job.runtime.GoalState, jobRuntime.GoalState)
	suite.checkListeners()
}

// TestJobUpdateRuntimeWithNoRuntimeCache tests update job which has
// existing cache
func (suite *jobTestSuite) TestJobUpdateRuntimeWithCache() {
	jobRuntime := &pbjob.RuntimeInfo{
		State:     pbjob.JobState_RUNNING,
		GoalState: pbjob.JobState_SUCCEEDED,
	}

	suite.job.runtime = &pbjob.RuntimeInfo{
		State:     pbjob.JobState_INITIALIZED,
		GoalState: pbjob.JobState_SUCCEEDED,
	}

	err := suite.job.Update(
		context.Background(),
		&pbjob.JobInfo{Runtime: jobRuntime},
		nil,
		nil,
		UpdateCacheOnly)
	suite.NoError(err)
	suite.Equal(suite.job.runtime.State, jobRuntime.State)
	suite.Equal(suite.job.runtime.GoalState, jobRuntime.GoalState)
	suite.checkListenersNotCalled()
}

// TestJobCompareAndSetRuntimeWithCache tests replace job runtime which has
// existing cache
func (suite *jobTestSuite) TestJobCompareAndSetRuntimeWithCache() {
	revision := &peloton.ChangeLog{
		Version: 1,
	}

	suite.job.runtime = &pbjob.RuntimeInfo{
		State:     pbjob.JobState_INITIALIZED,
		GoalState: pbjob.JobState_SUCCEEDED,
		Revision:  revision,
	}

	jobRuntime, err := suite.job.GetRuntime(context.Background())
	suite.NoError(err)
	suite.Equal(suite.job.runtime.State, jobRuntime.GetState())
	suite.Equal(suite.job.runtime.GoalState, jobRuntime.GetGoalState())

	suite.jobRuntimeOps.EXPECT().
		Upsert(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.State, jobRuntime.GetState())
			suite.Equal(runtime.GoalState, jobRuntime.GetGoalState())
		}).
		Return(nil)
	suite.jobIndexOps.EXPECT().
		Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	_, err = suite.job.CompareAndSetRuntime(context.Background(), jobRuntime)
	suite.NoError(err)
	suite.Equal(suite.job.runtime.State, jobRuntime.GetState())
	suite.Equal(suite.job.runtime.GoalState, jobRuntime.GetGoalState())
	suite.Equal(suite.job.runtime.GetRevision().GetVersion(), revision.GetVersion()+1)
	suite.checkListeners()
	for _, l := range suite.listeners {
		l.Reset()
	}

	// second call with the same jobRuntime should fail due to concurrency check
	_, err = suite.job.CompareAndSetRuntime(context.Background(), jobRuntime)
	suite.Error(err)
	suite.checkListenersNotCalled()
}

// TestJobUpdateCompareAndSetRuntimeMixedUsage tests using CompareAndSetRuntime
// and update together
func (suite *jobTestSuite) TestJobUpdateCompareAndSetRuntimeMixedUsage() {
	revision := &peloton.ChangeLog{
		Version: 1,
	}

	jobRuntimeUpdate := &pbjob.RuntimeInfo{
		State: pbjob.JobState_PENDING,
	}

	suite.job.runtime = &pbjob.RuntimeInfo{
		State:     pbjob.JobState_INITIALIZED,
		GoalState: pbjob.JobState_SUCCEEDED,
		Revision:  revision,
	}

	suite.jobRuntimeOps.EXPECT().
		Upsert(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.State, jobRuntimeUpdate.State)
		}).
		Return(nil)
	suite.jobIndexOps.EXPECT().
		Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	jobRuntime, err := suite.job.GetRuntime(context.Background())
	suite.NoError(err)
	err = suite.job.Update(
		context.Background(),
		&pbjob.JobInfo{Runtime: jobRuntimeUpdate},
		nil,
		nil,
		UpdateCacheAndDB)
	suite.NoError(err)
	suite.checkListeners()
	for _, l := range suite.listeners {
		l.Reset()
	}

	// should fail because jobRuntime is already outdated due to Update
	jobRuntime.State = pbjob.JobState_KILLED
	_, err = suite.job.CompareAndSetRuntime(context.Background(), jobRuntime)
	suite.Error(err)
	suite.checkListenersNotCalled()
}

// TestJobCompareAndSetRuntimeUnexpectedVersionError tests replace job runtime
// which fails due to incorrect version number
func (suite *jobTestSuite) TestJobCompareAndSetRuntimeUnexpectedVersionError() {
	jobRuntime := &pbjob.RuntimeInfo{
		State:     pbjob.JobState_RUNNING,
		GoalState: pbjob.JobState_KILLED,
		Revision: &peloton.ChangeLog{
			Version: 2,
		},
	}

	suite.job.runtime = &pbjob.RuntimeInfo{
		State:     pbjob.JobState_INITIALIZED,
		GoalState: pbjob.JobState_SUCCEEDED,
		Revision: &peloton.ChangeLog{
			Version: 1,
		},
	}

	_, err := suite.job.CompareAndSetRuntime(context.Background(), jobRuntime)
	suite.Nil(suite.job.runtime)
	suite.Nil(suite.job.config)
	suite.Error(err)
	suite.checkListenersNotCalled()
}

// TestJobCompareAndSetRuntimeNoCache tests replace job runtime which has
// no existing cache
func (suite *jobTestSuite) TestJobCompareAndSetRuntimeNoCache() {
	revision := &peloton.ChangeLog{
		Version: 1,
	}

	jobRuntime := &pbjob.RuntimeInfo{
		State:     pbjob.JobState_RUNNING,
		GoalState: pbjob.JobState_KILLED,
		Revision:  revision,
	}

	suite.job.runtime = nil

	suite.jobRuntimeOps.EXPECT().
		Get(context.Background(), suite.jobID).
		Return(&pbjob.RuntimeInfo{
			State:     pbjob.JobState_INITIALIZED,
			GoalState: pbjob.JobState_SUCCEEDED,
			Revision:  revision,
		}, nil)

	suite.jobRuntimeOps.EXPECT().
		Upsert(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.State, jobRuntime.State)
			suite.Equal(runtime.GoalState, jobRuntime.GoalState)
		}).
		Return(nil)
	suite.jobIndexOps.EXPECT().
		Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	_, err := suite.job.CompareAndSetRuntime(context.Background(), jobRuntime)
	suite.NoError(err)
	suite.Equal(suite.job.runtime.State, jobRuntime.State)
	suite.Equal(suite.job.runtime.GoalState, jobRuntime.GoalState)
	suite.Equal(suite.job.runtime.GetRevision().GetVersion(), revision.GetVersion()+1)
	suite.checkListeners()
}

// TestJobUpdateCompareAndSetRuntimeNilInput tests the case of calling
// CompareAndSetRuntime with nil input
func (suite *jobTestSuite) TestJobUpdateCompareAndSetRuntimeNilInput() {
	_, err := suite.job.CompareAndSetRuntime(context.Background(), nil)
	suite.Error(err)
}

// TestJobCompareAndSetRuntimeUpdateRuntimeFailure tests replace
// job runtime failed due to job_runtime update failure
func (suite *jobTestSuite) TestJobCompareAndSetRuntimeUpdateRuntimeFailure() {
	revision := &peloton.ChangeLog{
		Version: 1,
	}

	jobRuntime := &pbjob.RuntimeInfo{
		State:     pbjob.JobState_RUNNING,
		GoalState: pbjob.JobState_KILLED,
		Revision:  revision,
	}

	suite.job.runtime = nil

	suite.jobRuntimeOps.EXPECT().
		Get(context.Background(), suite.jobID).
		Return(&pbjob.RuntimeInfo{
			State:     pbjob.JobState_INITIALIZED,
			GoalState: pbjob.JobState_SUCCEEDED,
			Revision:  revision,
		}, nil)

	suite.jobRuntimeOps.EXPECT().
		Upsert(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.State, jobRuntime.State)
			suite.Equal(runtime.GoalState, jobRuntime.GoalState)
		}).
		Return(yarpcerrors.InternalErrorf("test error"))

	_, err := suite.job.CompareAndSetRuntime(context.Background(), jobRuntime)
	suite.Error(err)
	suite.Nil(suite.job.runtime)
}

// TestJobCompareAndSetRuntimeWithGoalStateDeleted tests replace job runtime
// which fails due to job goal state is deleted
func (suite *jobTestSuite) TestJobCompareAndSetRuntimeWithGoalStateDeleted() {
	jobRuntime := &pbjob.RuntimeInfo{
		State:     pbjob.JobState_RUNNING,
		GoalState: pbjob.JobState_RUNNING,
		Revision: &peloton.ChangeLog{
			Version: 1,
		},
	}

	suite.job.runtime = &pbjob.RuntimeInfo{
		State:     pbjob.JobState_RUNNING,
		GoalState: pbjob.JobState_DELETED,
		Revision: &peloton.ChangeLog{
			Version: 1,
		},
	}

	_, err := suite.job.CompareAndSetRuntime(context.Background(), jobRuntime)
	suite.Error(err)
	suite.checkListenersNotCalled()
}

// TestJobUpdateConfig tests update job which new config
func (suite *jobTestSuite) TestJobUpdateConfig() {
	jobConfig := &pbjob.JobConfig{
		InstanceCount: 10,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		ChangeLog: &peloton.ChangeLog{
			Version: 1,
		},
	}

	suite.job.config = &cachedConfig{
		instanceCount: 5,
		changeLog: &peloton.ChangeLog{
			Version: 1,
		},
	}
	suite.job.runtime = &pbjob.RuntimeInfo{
		Revision: &peloton.ChangeLog{
			Version: 1,
		},
		ConfigurationVersion: suite.job.config.changeLog.Version,
	}

	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID.GetValue()).
		Return(suite.job.config.changeLog.Version, nil)

	suite.jobConfigOps.EXPECT().
		Create(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Do(func(_ context.Context,
			_ *peloton.JobID,
			config *pbjob.JobConfig,
			_ *models.ConfigAddOn,
			_ *stateless.JobSpec,
			_ uint64,
		) {
			suite.Equal(config.InstanceCount, uint32(10))
			suite.Equal(config.ChangeLog.Version, uint64(2))
		}).
		Return(nil)

	suite.jobRuntimeOps.EXPECT().
		Upsert(gomock.Any(), suite.jobID, gomock.Any()).
		Return(nil)
	suite.jobIndexOps.EXPECT().
		Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil).Times(2)

	err := suite.job.Update(
		context.Background(),
		&pbjob.JobInfo{Config: jobConfig},
		&models.ConfigAddOn{},
		nil,
		UpdateCacheAndDB)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, jobConfig.InstanceCount)
	_, err = suite.job.GetRuntime(context.Background())
	suite.NoError(err)
	suite.checkListenersNotCalled()
	// update runtime to point to the new config
	err = suite.job.Update(context.Background(),
		&pbjob.JobInfo{Runtime: &pbjob.RuntimeInfo{
			ConfigurationVersion: 2,
		}}, nil,
		nil,
		UpdateCacheAndDB)
	suite.NoError(err)
	suite.checkListeners()
	config, err := suite.job.GetConfig(context.Background())
	suite.NoError(err)
	suite.Equal(config.GetChangeLog().Version, uint64(2))
}

// TestJobUpdateConfig tests update job which new config
func (suite *jobTestSuite) TestJobUpdateConfigIncorectChangeLog() {
	jobConfig := &pbjob.JobConfig{
		InstanceCount: 10,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		ChangeLog: &peloton.ChangeLog{
			Version: 3,
		},
	}

	suite.job.config = &cachedConfig{
		instanceCount: 5,
		changeLog: &peloton.ChangeLog{
			Version: 1,
		},
	}

	err := suite.job.Update(
		context.Background(),
		&pbjob.JobInfo{Config: jobConfig},
		&models.ConfigAddOn{},
		nil,
		UpdateCacheAndDB)
	suite.True(yarpcerrors.IsInvalidArgument(err))
	suite.EqualError(err,
		"code:invalid-argument message:invalid job configuration version")
	suite.NotEqual(suite.job.config.instanceCount, jobConfig.InstanceCount)
	suite.checkListenersNotCalled()
}

// TestJobUpdateRuntimeAndConfig tests update both runtime
// and config at the same time
func (suite *jobTestSuite) TestJobUpdateRuntimeAndConfig() {
	instanceCount := uint32(10)
	configVersion := uint64(1)
	configStateStats := make(map[uint64]*pbjob.RuntimeInfo_TaskStateStats)
	configStateStats[configVersion] = &pbjob.RuntimeInfo_TaskStateStats{
		StateStats: map[string]uint32{
			pbjob.JobState_RUNNING.String(): instanceCount,
		},
	}

	jobRuntime := &pbjob.RuntimeInfo{
		State:                           pbjob.JobState_RUNNING,
		GoalState:                       pbjob.JobState_SUCCEEDED,
		TaskStatsByConfigurationVersion: configStateStats,
	}

	jobConfig := &pbjob.JobConfig{
		InstanceCount: instanceCount,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		ChangeLog:     &peloton.ChangeLog{Version: 1},
	}

	suite.jobRuntimeOps.EXPECT().
		Get(context.Background(), suite.jobID).
		Return(&pbjob.RuntimeInfo{
			State:     pbjob.JobState_INITIALIZED,
			GoalState: pbjob.JobState_SUCCEEDED,
			Revision: &peloton.ChangeLog{
				CreatedAt: uint64(time.Now().UnixNano()),
				UpdatedAt: uint64(time.Now().UnixNano()),
				Version:   1,
			},
			ConfigurationVersion: configVersion,
		}, nil)

	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID.GetValue()).
		Return(suite.job.config.changeLog.Version, nil)

	suite.jobConfigOps.EXPECT().
		Create(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Do(func(_ context.Context,
			_ *peloton.JobID,
			config *pbjob.JobConfig,
			_ *models.ConfigAddOn,
			_ *stateless.JobSpec,
			_ uint64,
		) {
			suite.Equal(config.InstanceCount, uint32(10))
		}).
		Return(nil)

	suite.jobRuntimeOps.EXPECT().
		Upsert(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.State, jobRuntime.State)
			suite.Equal(runtime.GoalState, jobRuntime.GoalState)
		}).
		Return(nil)
	suite.jobIndexOps.EXPECT().
		Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	suite.job.runtime = nil
	err := suite.job.Update(context.Background(), &pbjob.JobInfo{
		Runtime: jobRuntime,
		Config:  jobConfig,
	}, &models.ConfigAddOn{},
		nil,
		UpdateCacheAndDB)
	suite.NoError(err)
	suite.checkListeners()
	suite.Equal(suite.job.runtime.State, jobRuntime.State)
	suite.Equal(suite.job.runtime.GoalState, jobRuntime.GoalState)
	suite.Equal(suite.job.config.instanceCount, instanceCount)
	suite.Equal(len(suite.job.runtime.GetTaskStatsByConfigurationVersion()), 1)
	suite.Equal(
		suite.job.runtime.
			GetTaskStatsByConfigurationVersion()[configVersion].
			GetStateStats()[pbjob.JobState_RUNNING.String()],
		instanceCount)
}

// Test the case job update config when there is no config in cache,
// and later recover the config in cache which should not
// overwrite updated cache
func (suite *jobTestSuite) TestJobUpdateConfigAndRecoverConfig() {
	suite.job.config = nil
	suite.job.runtime = nil

	initialConfig := pbjob.JobConfig{
		Name:          "test_job",
		Type:          pbjob.JobType_BATCH,
		InstanceCount: 5,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		ChangeLog: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}

	configAddOn := &models.ConfigAddOn{}
	suite.jobRuntimeOps.EXPECT().
		Get(context.Background(), suite.jobID).
		Return(&pbjob.RuntimeInfo{ConfigurationVersion: uint64(1)}, nil)
	suite.jobConfigOps.EXPECT().Get(gomock.Any(), suite.jobID, uint64(1)).
		Return(&initialConfig, configAddOn, nil)
	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID.GetValue()).
		Return(initialConfig.ChangeLog.Version, nil)
	suite.jobConfigOps.EXPECT().
		Create(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Do(func(_ context.Context,
			_ *peloton.JobID,
			config *pbjob.JobConfig,
			addOn *models.ConfigAddOn,
			_ *stateless.JobSpec,
			_ uint64,
		) {
			suite.Equal(config.ChangeLog.Version, uint64(2))
			suite.Equal(*configAddOn, *addOn)
			suite.Equal(config.InstanceCount, uint32(10))
		}).
		Return(nil)
	suite.jobIndexOps.EXPECT().
		Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)
	updatedConfig := initialConfig
	updatedConfig.InstanceCount = 10
	updatedConfig.ChangeLog = nil
	err := suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config: &updatedConfig,
	}, &models.ConfigAddOn{},
		nil,
		UpdateCacheAndDB)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))

	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config: &initialConfig,
	}, &models.ConfigAddOn{},
		nil,
		UpdateCacheOnly)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))
}

// Test the case job update config when there is no config in cache,
// and later recover the runtime which should not interfere with each other
func (suite *jobTestSuite) TestJobUpdateConfigAndRecoverRuntime() {
	suite.job.config = nil
	suite.job.runtime = nil

	initialConfig := pbjob.JobConfig{
		Name:          "test_job",
		Type:          pbjob.JobType_BATCH,
		InstanceCount: 5,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		ChangeLog: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}
	initialRuntime := pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		ConfigurationVersion: 1,
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}

	configAddOn := &models.ConfigAddOn{}
	suite.jobRuntimeOps.EXPECT().
		Get(context.Background(), suite.jobID).
		Return(&pbjob.RuntimeInfo{ConfigurationVersion: uint64(1)}, nil)
	suite.jobConfigOps.EXPECT().Get(gomock.Any(), suite.jobID, uint64(1)).
		Return(&initialConfig, configAddOn, nil)
	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID.GetValue()).
		Return(initialConfig.ChangeLog.Version, nil)
	suite.jobConfigOps.EXPECT().
		Create(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Do(func(_ context.Context,
			_ *peloton.JobID,
			config *pbjob.JobConfig,
			addOn *models.ConfigAddOn,
			_ *stateless.JobSpec,
			_ uint64,
		) {
			suite.Equal(config.ChangeLog.Version, uint64(2))
			suite.Equal(config.InstanceCount, uint32(10))
			suite.Equal(*configAddOn, *addOn)
		}).
		Return(nil)
	suite.jobIndexOps.EXPECT().
		Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)
	updatedConfig := initialConfig
	updatedConfig.InstanceCount = 10
	updatedConfig.ChangeLog = nil
	err := suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config: &updatedConfig,
	}, &models.ConfigAddOn{},
		nil,
		UpdateCacheAndDB)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))

	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Runtime: &initialRuntime,
	}, nil,
		nil,
		UpdateCacheOnly)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(1))
	suite.Equal(suite.job.runtime.Revision.Version, uint64(1))
}

// Test the case job update config when there is no config in cache,
// and later recover the runtime and config
func (suite *jobTestSuite) TestJobUpdateConfigAndRecoverConfigPlusRuntime() {
	suite.job.config = nil
	suite.job.runtime = nil

	initialConfig := pbjob.JobConfig{
		Name:          "test_job",
		Type:          pbjob.JobType_BATCH,
		InstanceCount: 5,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		ChangeLog: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}
	initialRuntime := pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		ConfigurationVersion: 1,
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}

	configAddOn := &models.ConfigAddOn{}
	suite.jobRuntimeOps.EXPECT().
		Get(context.Background(), suite.jobID).
		Return(&pbjob.RuntimeInfo{ConfigurationVersion: uint64(1)}, nil)
	suite.jobConfigOps.EXPECT().Get(gomock.Any(), suite.jobID, uint64(1)).
		Return(&initialConfig, configAddOn, nil)
	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID.GetValue()).
		Return(initialConfig.ChangeLog.Version, nil)
	suite.jobConfigOps.EXPECT().
		Create(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Do(func(_ context.Context,
			_ *peloton.JobID,
			config *pbjob.JobConfig,
			addOn *models.ConfigAddOn,
			_ *stateless.JobSpec,
			_ uint64,
		) {
			suite.Equal(config.ChangeLog.Version, uint64(2))
			suite.Equal(config.InstanceCount, uint32(10))
			suite.Equal(*configAddOn, *addOn)
		}).
		Return(nil)
	suite.jobIndexOps.EXPECT().
		Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)
	updatedConfig := initialConfig
	updatedConfig.InstanceCount = 10
	updatedConfig.ChangeLog = nil
	err := suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config: &updatedConfig,
	}, &models.ConfigAddOn{},
		nil,
		UpdateCacheAndDB)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))

	// Test 'ConfigAddOn cannot be nil error'
	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config:  &initialConfig,
		Runtime: &initialRuntime,
	}, nil,
		nil,
		UpdateCacheOnly)
	suite.Error(err)

	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config:  &initialConfig,
		Runtime: &initialRuntime,
	}, &models.ConfigAddOn{},
		nil,
		UpdateCacheOnly)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(1))
}

// Test the case job update runtime when there is no runtime in cache,
// and later recover the runtime in cache which should not
// overwrite updated cache
func (suite *jobTestSuite) TestJobUpdateRuntimeAndRecoverRuntime() {
	suite.job.config = nil
	suite.job.runtime = nil

	initialRuntime := pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		ConfigurationVersion: 1,
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}

	suite.jobRuntimeOps.EXPECT().
		Get(context.Background(), suite.jobID).
		Return(&initialRuntime, nil)
	suite.jobRuntimeOps.EXPECT().
		Upsert(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.State, pbjob.JobState_SUCCEEDED)
			suite.Equal(runtime.ConfigurationVersion, uint64(1))
			suite.Equal(runtime.Revision.Version, uint64(2))
		}).
		Return(nil)
	suite.jobIndexOps.EXPECT().
		Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	updateRuntime := pbjob.RuntimeInfo{
		State: pbjob.JobState_SUCCEEDED,
	}

	err := suite.job.Update(context.Background(), &pbjob.JobInfo{
		Runtime: &updateRuntime,
	}, nil,
		nil,
		UpdateCacheAndDB)
	suite.NoError(err)
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(1))
	suite.Equal(suite.job.runtime.Revision.Version, uint64(2))
	suite.Equal(suite.job.runtime.State, pbjob.JobState_SUCCEEDED)

	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Runtime: &initialRuntime,
	}, nil,
		nil,
		UpdateCacheOnly)
	suite.NoError(err)
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(1))
	suite.Equal(suite.job.runtime.Revision.Version, uint64(2))
	suite.Equal(suite.job.runtime.State, pbjob.JobState_SUCCEEDED)
}

// Test the case job update runtime when there is no runtime in cache,
// and later recover the config which should not interfere with each other
func (suite *jobTestSuite) TestJobUpdateRuntimeAndRecoverConfig() {
	suite.job.config = nil
	suite.job.runtime = nil

	initialRuntime := pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		ConfigurationVersion: 1,
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}

	initialConfig := pbjob.JobConfig{
		Name:          "test_job",
		Type:          pbjob.JobType_BATCH,
		InstanceCount: 5,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		ChangeLog: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}

	suite.jobRuntimeOps.EXPECT().
		Get(context.Background(), suite.jobID).
		Return(&initialRuntime, nil)
	suite.jobRuntimeOps.EXPECT().
		Upsert(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.State, pbjob.JobState_SUCCEEDED)
			suite.Equal(runtime.ConfigurationVersion, uint64(1))
			suite.Equal(runtime.Revision.Version, uint64(2))
		}).
		Return(nil)
	suite.jobIndexOps.EXPECT().
		Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	updateRuntime := pbjob.RuntimeInfo{
		State: pbjob.JobState_SUCCEEDED,
	}

	err := suite.job.Update(context.Background(), &pbjob.JobInfo{
		Runtime: &updateRuntime,
	}, nil,
		nil,
		UpdateCacheAndDB)
	suite.NoError(err)
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(1))
	suite.Equal(suite.job.runtime.Revision.Version, uint64(2))
	suite.Equal(suite.job.runtime.State, pbjob.JobState_SUCCEEDED)

	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config: &initialConfig,
	}, &models.ConfigAddOn{},
		nil,
		UpdateCacheOnly)
	suite.NoError(err)
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(1))
	suite.Equal(suite.job.runtime.Revision.Version, uint64(2))
	suite.Equal(suite.job.runtime.State, pbjob.JobState_SUCCEEDED)
}

// Test the case job update runtime when there is no runtime in cache,
// and later recover the runtime and config
func (suite *jobTestSuite) TestJobUpdateRuntimeAndRecoverConfigPlusRuntime() {
	suite.job.config = nil
	suite.job.runtime = nil

	initialRuntime := pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		ConfigurationVersion: 1,
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}

	initialConfig := pbjob.JobConfig{
		Name:          "test_job",
		Type:          pbjob.JobType_BATCH,
		InstanceCount: 5,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		ChangeLog: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}

	suite.jobRuntimeOps.EXPECT().
		Get(context.Background(), suite.jobID).
		Return(&initialRuntime, nil)
	suite.jobRuntimeOps.EXPECT().
		Upsert(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.State, pbjob.JobState_SUCCEEDED)
			suite.Equal(runtime.ConfigurationVersion, uint64(1))
			suite.Equal(runtime.Revision.Version, uint64(2))
		}).
		Return(nil)
	suite.jobIndexOps.EXPECT().
		Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	updateRuntime := pbjob.RuntimeInfo{
		State: pbjob.JobState_SUCCEEDED,
	}

	err := suite.job.Update(context.Background(), &pbjob.JobInfo{
		Runtime: &updateRuntime,
	}, nil,
		nil,
		UpdateCacheAndDB)
	suite.NoError(err)
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(1))
	suite.Equal(suite.job.runtime.Revision.Version, uint64(2))
	suite.Equal(suite.job.runtime.State, pbjob.JobState_SUCCEEDED)

	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config:  &initialConfig,
		Runtime: &initialRuntime,
	}, &models.ConfigAddOn{},
		nil,
		UpdateCacheOnly)
	suite.NoError(err)
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(1))
	suite.Equal(suite.job.runtime.Revision.Version, uint64(2))
	suite.Equal(suite.job.runtime.State, pbjob.JobState_SUCCEEDED)
}

func (suite *jobTestSuite) TestJobUpdateRuntimePlusConfigAndRecover() {
	suite.job.config = nil
	suite.job.runtime = nil

	initialConfig := pbjob.JobConfig{
		Name:          "test_job",
		Type:          pbjob.JobType_BATCH,
		InstanceCount: 5,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		ChangeLog: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}
	initialRuntime := pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		ConfigurationVersion: 1,
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}

	configAddOn := &models.ConfigAddOn{}
	suite.jobRuntimeOps.EXPECT().
		Get(context.Background(), suite.jobID).
		Return(&pbjob.RuntimeInfo{ConfigurationVersion: uint64(1)}, nil)
	suite.jobConfigOps.EXPECT().Get(gomock.Any(), suite.jobID, uint64(1)).
		Return(&initialConfig, configAddOn, nil)
	suite.jobRuntimeOps.EXPECT().
		Get(context.Background(), suite.jobID).
		Return(&initialRuntime, nil)
	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID.GetValue()).
		Return(initialConfig.ChangeLog.Version, nil)
	suite.jobRuntimeOps.EXPECT().
		Upsert(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.State, pbjob.JobState_SUCCEEDED)
		}).
		Return(nil)
	suite.jobConfigOps.EXPECT().
		Create(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Do(func(_ context.Context,
			_ *peloton.JobID,
			config *pbjob.JobConfig,
			_ *models.ConfigAddOn,
			_ *stateless.JobSpec,
			_ uint64,
		) {
			suite.Equal(config.ChangeLog.Version, uint64(2))
			suite.Equal(config.InstanceCount, uint32(10))
		}).
		Return(nil)
	suite.jobIndexOps.EXPECT().
		Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)
	updatedConfig := initialConfig
	updatedConfig.InstanceCount = 10
	updatedConfig.ChangeLog = nil

	updateRuntime := pbjob.RuntimeInfo{
		State: pbjob.JobState_SUCCEEDED,
	}

	err := suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config:  &updatedConfig,
		Runtime: &updateRuntime,
	}, &models.ConfigAddOn{},
		nil,
		UpdateCacheAndDB)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))
	suite.Equal(suite.job.runtime.State, pbjob.JobState_SUCCEEDED)

	// recover runtime only
	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Runtime: &initialRuntime,
	}, nil,
		nil,
		UpdateCacheOnly)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))
	suite.Equal(suite.job.runtime.State, pbjob.JobState_SUCCEEDED)

	// recover config only
	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config: &initialConfig,
	}, &models.ConfigAddOn{},
		nil,
		UpdateCacheOnly)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))
	suite.Equal(suite.job.runtime.State, pbjob.JobState_SUCCEEDED)

	// recover both config and runtime
	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config:  &initialConfig,
		Runtime: &initialRuntime,
	}, &models.ConfigAddOn{},
		nil,
		UpdateCacheOnly)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))
	suite.Equal(suite.job.runtime.State, pbjob.JobState_SUCCEEDED)
}

// TestJobCreate tests job create in cache and db
func (suite *jobTestSuite) TestJobCreate() {
	jobConfig := &pbjob.JobConfig{
		InstanceCount:     10,
		Type:              pbjob.JobType_BATCH,
		RespoolID:         &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		PlacementStrategy: pbjob.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD_JOB,
	}

	createdBy := "test"
	configAddOn := &models.ConfigAddOn{
		SystemLabels: []*peloton.Label{
			{
				Key: fmt.Sprintf(
					common.SystemLabelKeyTemplate,
					common.SystemLabelPrefix,
					common.SystemLabelJobOwner),
				Value: createdBy,
			},
			{
				Key: fmt.Sprintf(
					common.SystemLabelKeyTemplate,
					common.SystemLabelPrefix,
					common.SystemLabelJobType),
				Value: jobConfig.Type.String(),
			},
		},
	}

	suite.jobConfigOps.EXPECT().
		Create(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Do(func(_ context.Context,
			_ *peloton.JobID,
			config *pbjob.JobConfig,
			addOn *models.ConfigAddOn,
			_ *stateless.JobSpec,
			version uint64) {
			suite.Equal(version, uint64(1))
			suite.Equal(config.ChangeLog.Version, uint64(1))
			suite.Equal(config.InstanceCount, jobConfig.InstanceCount)
			suite.Equal(config.Type, jobConfig.Type)
			suite.Len(addOn.SystemLabels, len(configAddOn.SystemLabels))
			for i := 0; i < len(configAddOn.SystemLabels); i++ {
				suite.Equal(
					configAddOn.SystemLabels[i].GetKey(),
					addOn.SystemLabels[i].GetKey(),
				)
				suite.Equal(
					configAddOn.SystemLabels[i].GetValue(),
					addOn.SystemLabels[i].GetValue(),
				)
			}
		}).
		Return(nil)

	suite.activeJobsOps.EXPECT().
		Create(gomock.Any(), suite.jobID).Return(nil)

	suite.jobRuntimeOps.EXPECT().
		Upsert(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, initialRuntime *pbjob.RuntimeInfo) {
			suite.Equal(initialRuntime.State, pbjob.JobState_UNINITIALIZED)
			suite.Equal(initialRuntime.GoalState, pbjob.JobState_SUCCEEDED)
			suite.Equal(initialRuntime.Revision.Version, uint64(1))
			suite.Equal(initialRuntime.ConfigurationVersion, uint64(1))
			suite.Equal(initialRuntime.TaskStats[pbjob.JobState_INITIALIZED.String()], jobConfig.InstanceCount)
			suite.Equal(len(initialRuntime.TaskStats), 1)
		}).
		Return(nil)

	suite.jobRuntimeOps.EXPECT().
		Upsert(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.GetState(), pbjob.JobState_INITIALIZED)
		}).Return(nil)

	suite.jobIndexOps.EXPECT().
		Create(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Do(func(
			_ context.Context,
			_ *peloton.JobID,
			config *pbjob.JobConfig,
			runtime *pbjob.RuntimeInfo,
		) {
			suite.Equal(config.InstanceCount, jobConfig.InstanceCount)
			suite.Equal(config.Type, jobConfig.Type)
			suite.Equal(runtime.GetState(), pbjob.JobState_INITIALIZED)
		}).Return(nil)

	err := suite.job.Create(context.Background(), jobConfig, configAddOn, nil)
	suite.NoError(err)
	config, err := suite.job.GetConfig(context.Background())
	suite.NoError(err)
	suite.Equal(config.GetInstanceCount(), uint32(10))
	suite.Equal(config.GetChangeLog().Version, uint64(1))
	suite.Equal(config.GetType(), pbjob.JobType_BATCH)
	suite.Equal(pbjob.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD_JOB,
		config.GetPlacementStrategy())
	runtime, err := suite.job.GetRuntime(context.Background())
	suite.NoError(err)
	suite.Equal(runtime.GetRevision().GetVersion(), uint64(1))
	suite.Equal(runtime.GetConfigurationVersion(), uint64(1))
	suite.Equal(runtime.GetState(), pbjob.JobState_INITIALIZED)
	suite.Equal(runtime.GetGoalState(), pbjob.JobState_SUCCEEDED)
	suite.checkListeners()
}

// TestJobGetRuntimeRefillCache tests job would refill runtime cache
// if cache is missing
func (suite *jobTestSuite) TestJobGetRuntimeRefillCache() {
	jobRuntime := &pbjob.RuntimeInfo{
		State:     pbjob.JobState_INITIALIZED,
		GoalState: pbjob.JobState_SUCCEEDED,
	}
	suite.jobRuntimeOps.EXPECT().
		Get(context.Background(), suite.job.id).
		Return(jobRuntime, nil)
	suite.job.runtime = nil
	runtime, err := suite.job.GetRuntime(context.Background())
	suite.NoError(err)
	suite.Equal(runtime.GetState(), jobRuntime.State)
	suite.Equal(runtime.GetGoalState(), jobRuntime.GoalState)
}

func (suite *jobTestSuite) TestJobGetConfigDBError() {
	suite.job.config = nil
	// Test the case there is no config cache and db returns err
	suite.jobConfigOps.EXPECT().Get(
		gomock.Any(),
		suite.jobID,
		suite.job.runtime.GetConfigurationVersion()).
		Return(nil, nil, fmt.Errorf("test error"))

	config, err := suite.job.GetConfig(context.Background())
	suite.Error(err)
	suite.Nil(config)
}

func (suite *jobTestSuite) TestJobGetConfigSuccess() {
	suite.job.config = nil
	testName := "testName"
	testLabels := []*peloton.Label{{Key: "key1", Value: "val1"}, {Key: "key2", Value: "val2"}}
	// Test the case there is no config cache and db returns no err
	jobConfig := &pbjob.JobConfig{
		InstanceCount: 10,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		ChangeLog: &peloton.ChangeLog{
			Version: suite.job.runtime.ConfigurationVersion,
		},
		Name:   testName,
		Labels: testLabels,
	}

	suite.jobConfigOps.EXPECT().Get(
		gomock.Any(),
		suite.jobID,
		suite.job.runtime.GetConfigurationVersion()).
		Return(jobConfig, &models.ConfigAddOn{}, nil)

	config, err := suite.job.GetConfig(context.Background())
	suite.NoError(err)

	suite.Equal(config.GetInstanceCount(), jobConfig.GetInstanceCount())
	suite.Equal(config.GetName(), jobConfig.GetName())
	suite.Equal(config.GetLabels(), jobConfig.GetLabels())

	suite.Nil(config.GetSLA())

	// Test the case there is config cache after the first call to
	// GetConfig
	config, err = suite.job.GetConfig(context.Background())
	suite.NoError(err)
	suite.Equal(config.GetInstanceCount(), jobConfig.GetInstanceCount())
	suite.Nil(config.GetSLA())
}

func (suite *jobTestSuite) TestJobIsControllerTask() {
	tests := []struct {
		config         *pbjob.JobConfig
		expectedResult bool
	}{
		{&pbjob.JobConfig{
			DefaultConfig: &pbtask.TaskConfig{Controller: false},
		},
			false},
		{&pbjob.JobConfig{
			DefaultConfig: &pbtask.TaskConfig{Controller: true},
		},
			true},
		{&pbjob.JobConfig{
			DefaultConfig: &pbtask.TaskConfig{Controller: false},
			InstanceConfig: map[uint32]*pbtask.TaskConfig{
				0: {Controller: true},
			},
		},
			true},
	}

	for index, test := range tests {
		suite.job.config = nil

		suite.jobConfigOps.EXPECT().Get(
			gomock.Any(),
			suite.jobID,
			suite.job.runtime.GetConfigurationVersion()).
			Return(test.config, &models.ConfigAddOn{}, nil)

		config, err := suite.job.GetConfig(context.Background())
		suite.NoError(err)
		suite.Equal(HasControllerTask(config), test.expectedResult, "test:%d fails", index)
	}
}

// TestJobSetJobUpdateTime tests update the task update time coming from mesos.
func (suite *jobTestSuite) TestJobSetJobUpdateTime() {
	// Test setting and fetching job update time
	updateTime := float64(time.Now().UnixNano())
	suite.job.SetTaskUpdateTime(&updateTime)
	suite.Equal(updateTime, suite.job.GetFirstTaskUpdateTime())
	suite.Equal(updateTime, suite.job.GetLastTaskUpdateTime())
}

// TestJobCreateTaskRuntimes tests creating task runtimes in cache and DB
func (suite *jobTestSuite) TestJobCreateTaskRuntimes() {
	instanceCount := uint32(10)
	runtimes := initializeRuntimes(instanceCount, pbtask.TaskState_INITIALIZED)

	for i := uint32(0); i < instanceCount; i++ {
		suite.taskStore.EXPECT().
			CreateTaskRuntime(
				gomock.Any(),
				suite.jobID,
				i,
				gomock.Any(),
				"peloton",
				gomock.Any()).
			Do(func(
				ctx context.Context,
				jobID *peloton.JobID,
				instanceID uint32,
				runtime *pbtask.RuntimeInfo,
				owner string,
				jobType pbjob.JobType) {
				suite.Equal(pbtask.TaskState_INITIALIZED, runtime.GetState())
				suite.Equal(uint64(1), runtime.GetRevision().GetVersion())
			}).Return(nil)

		suite.taskConfigV2Ops.EXPECT().
			GetTaskConfig(
				gomock.Any(),
				suite.jobID,
				i,
				gomock.Any(),
			).Return(nil, nil, nil)
	}

	err := suite.job.CreateTaskRuntimes(context.Background(), runtimes, "peloton")
	suite.NoError(err)

	// Validate the state of the tasks in cache is correct
	for i := uint32(0); i < instanceCount; i++ {
		tt := suite.job.GetTask(i)
		suite.NotNil(tt)
		actRuntime, _ := tt.GetRuntime(context.Background())
		suite.NotNil(actRuntime)
		suite.Equal(pbtask.TaskState_INITIALIZED, actRuntime.GetState())
		suite.Equal(uint64(1), actRuntime.GetRevision().GetVersion())
	}
}

// TestJobCreateTaskRuntimesWithDBError tests getting DB error while creating task runtimes
func (suite *jobTestSuite) TestJobCreateTaskRuntimesWithDBError() {
	instanceCount := uint32(10)

	runtimes := initializeRuntimes(instanceCount, pbtask.TaskState_INITIALIZED)
	for i := uint32(0); i < instanceCount; i++ {
		suite.taskStore.EXPECT().
			CreateTaskRuntime(gomock.Any(),
				suite.jobID,
				i,
				gomock.Any(),
				"peloton",
				pbjob.JobType_BATCH).
			Return(dbError)
	}

	err := suite.job.CreateTaskRuntimes(context.Background(), runtimes, "peloton")
	suite.Error(err)
}

// TestSetGetTasksInJobInCacheSingle tests setting and getting single task in job in cache.
func (suite *jobTestSuite) TestSetGetTasksInJobInCacheSingle() {
	instanceCount := uint32(10)
	taskInfo := &pbtask.TaskInfo{
		Runtime: &pbtask.RuntimeInfo{
			State: pbtask.TaskState_RUNNING,
			Revision: &peloton.ChangeLog{
				Version: 1,
			},
		},
	}

	// Test updating tasks one at a time in cache
	for i := uint32(0); i < instanceCount; i++ {
		suite.job.ReplaceTasks(map[uint32]*pbtask.TaskInfo{i: taskInfo}, false)
	}
	suite.Equal(instanceCount, uint32(len(suite.job.tasks)))

	// Validate the state of the tasks in cache is correct
	for i := uint32(0); i < instanceCount; i++ {
		tt := suite.job.GetTask(i)
		actRuntime, _ := tt.GetRuntime(context.Background())
		suite.Equal(*taskInfo.GetRuntime(), *actRuntime)
	}
}

// TestSetGetTasksInJobInCacheBlock tests setting and getting tasks as a chunk in a job in cache.
func (suite *jobTestSuite) TestSetGetTasksInJobInCacheBlock() {
	instanceCount := uint32(10)
	// Test updating tasks in one call in cache
	taskInfos := initializeTaskInfos(instanceCount, pbtask.TaskState_SUCCEEDED)
	suite.job.ReplaceTasks(taskInfos, false)

	// Validate the state of the tasks in cache is correct
	for instID, taskInfo := range taskInfos {
		tt := suite.job.GetTask(instID)
		actRuntime, _ := tt.GetRuntime(context.Background())
		suite.Equal(taskInfo.GetRuntime(), actRuntime)
	}
}

// TestTasksGetAllTasks tests getting all tasks.
func (suite *jobTestSuite) TestTasksGetAllTasks() {
	instanceCount := uint32(10)
	taskInfos := initializeTaskInfos(instanceCount, pbtask.TaskState_RUNNING)
	suite.job.ReplaceTasks(taskInfos, false)

	// Test get all tasks
	ttMap := suite.job.GetAllTasks()
	suite.Equal(instanceCount, uint32(len(ttMap)))
}

// TestTasksGetAllWorkflows tests getting all workflows.
func (suite *jobTestSuite) TestTasksGetAllWorkflows() {
	suite.job.AddWorkflow(&peloton.UpdateID{Value: uuid.New()})
	suite.job.AddWorkflow(&peloton.UpdateID{Value: uuid.New()})

	// Test get all workflows
	wlMap := suite.job.GetAllWorkflows()
	suite.Equal(len(wlMap), 2)
}

// TestPartialJobCheck checks the partial job check.
func (suite *jobTestSuite) TestPartialJobCheck() {
	instanceCount := uint32(10)

	taskInfos := initializeTaskInfos(instanceCount, pbtask.TaskState_RUNNING)
	suite.job.ReplaceTasks(taskInfos, false)

	// Test partial job check
	suite.job.config.instanceCount = 20
	suite.True(suite.job.IsPartiallyCreated(suite.job.config))
}

// TestPatchTasks_SetGetTasksSingle tests setting and getting single task in job in cache.
func (suite *jobTestSuite) TestPatchTasks_SetGetTasksSingle() {
	instanceCount := uint32(10)
	RuntimeDiff := jobmgrcommon.RuntimeDiff{
		jobmgrcommon.StateField: pbtask.TaskState_RUNNING,
	}

	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Return(nil).Times(int(instanceCount))
	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Return(&pbtask.RuntimeInfo{
			Revision: &peloton.ChangeLog{Version: 1},
			State:    pbtask.TaskState_INITIALIZED,
		}, nil).Times(int(instanceCount))
	suite.taskConfigV2Ops.EXPECT().
		GetTaskConfig(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil, nil, nil).
		Times(int(instanceCount))

	// Test updating tasks one at a time in cache
	for i := uint32(0); i < instanceCount; i++ {
		suite.job.PatchTasks(
			context.Background(),
			map[uint32]jobmgrcommon.RuntimeDiff{i: RuntimeDiff},
			false,
		)
	}
	suite.Equal(instanceCount, uint32(len(suite.job.tasks)))

	// Validate the state of the tasks in cache is correct
	for i := uint32(0); i < instanceCount; i++ {
		tt := suite.job.GetTask(i)
		actRuntime, _ := tt.GetRuntime(context.Background())
		suite.Equal(pbtask.TaskState_RUNNING, actRuntime.State)
	}
}

// TestReplaceTasks tests setting and getting tasks as a chunk in a job in cache.
func (suite *jobTestSuite) TestReplaceTasks() {
	instanceCount := uint32(10)
	// Test updating tasks in one call in cache
	taskInfos := initializeTaskInfos(instanceCount, pbtask.TaskState_SUCCEEDED)
	suite.job.ReplaceTasks(taskInfos, false)

	// Validate the state of the tasks in cache is correct
	for instID, taskInfo := range taskInfos {
		tt := suite.job.GetTask(instID)
		actRuntime, _ := tt.GetRuntime(context.Background())
		suite.Equal(taskInfo.GetRuntime(), actRuntime)
		actLabels, _ := tt.GetLabels(context.Background())
		suite.Equal(len(taskInfo.GetConfig().GetLabels()), len(actLabels))
		for count, label := range taskInfo.GetConfig().GetLabels() {
			suite.Equal(label.GetKey(), actLabels[count].GetKey())
			suite.Equal(label.GetValue(), actLabels[count].GetValue())
		}
	}
}

//
// TestPatchTasks_SetGetTasksMultiple tests patching runtime in multiple
func (suite *jobTestSuite) TestPatchTasksSetGetTasksMultiple() {
	instanceCount := uint32(10)

	diffs := initializeDiffs(instanceCount, pbtask.TaskState_RUNNING)

	for i := uint32(0); i < instanceCount; i++ {
		oldRuntime := initializeCurrentRuntime(pbtask.TaskState_LAUNCHED)
		suite.taskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), suite.jobID, i).Return(oldRuntime, nil)
		suite.taskStore.EXPECT().
			UpdateTaskRuntime(
				gomock.Any(),
				suite.jobID,
				i,
				gomock.Any(),
				gomock.Any()).Return(nil)
		suite.taskConfigV2Ops.EXPECT().
			GetTaskConfig(gomock.Any(), suite.jobID, i, gomock.Any()).
			Return(nil, nil, nil)
	}

	_, _, err := suite.job.PatchTasks(context.Background(), diffs, false)
	suite.NoError(err)

	// Validate the state of the tasks in cache is correct
	for instID := range diffs {
		tt := suite.job.GetTask(instID)
		suite.NotNil(tt)
		actRuntime, _ := tt.GetRuntime(context.Background())
		suite.NotNil(actRuntime)
		suite.Equal(pbtask.TaskState_RUNNING, actRuntime.GetState())
		suite.Equal(uint64(2), actRuntime.GetRevision().GetVersion())
	}
}

// TestPatchTasks_DBError tests getting DB error during update task runtimes.
func (suite *jobTestSuite) TestPatchTasksDBError() {
	instanceCount := uint32(10)
	diffs := initializeDiffs(instanceCount, pbtask.TaskState_RUNNING)

	for i := uint32(0); i < instanceCount; i++ {
		tt := suite.job.addTaskToJobMap(i)
		tt.runtime = &pbtask.RuntimeInfo{
			State: pbtask.TaskState_LAUNCHED,
		}
		// Simulate fake DB error
		suite.taskStore.EXPECT().
			UpdateTaskRuntime(
				gomock.Any(),
				suite.jobID,
				i,
				gomock.Any(),
				gomock.Any()).
			Return(dbError)
	}
	_, _, err := suite.job.PatchTasks(context.Background(), diffs, false)
	suite.Error(err)
}

// TestPatchTasks_SingleTask tests updating task runtime of a single task in DB.
func (suite *jobTestSuite) TestPatchTasksSingleTask() {
	var labels []*peloton.Label

	labels = append(labels, initializeLabel("key", "value"))
	diffs := initializeDiffs(1, pbtask.TaskState_RUNNING)
	oldRuntime := initializeCurrentRuntime(pbtask.TaskState_LAUNCHED)
	tt := suite.job.addTaskToJobMap(0)
	tt.runtime = oldRuntime
	tt.config = &taskConfigCache{
		labels: labels,
	}

	// Update task runtime of only one task
	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Return(nil)
	_, _, err := suite.job.PatchTasks(context.Background(), diffs, false)
	suite.NoError(err)

	// Validate the state of the task in cache is correct
	att := suite.job.GetTask(0)
	actRuntime, _ := att.GetRuntime(context.Background())
	suite.Equal(pbtask.TaskState_RUNNING, actRuntime.GetState())
	suite.Equal(uint64(2), actRuntime.GetRevision().GetVersion())
}

// TestPatchTasksSLAAwareNoViolation tests the success case of
// patching tasks in SLA aware manner, when SLA is not violated
func (suite *jobTestSuite) TestPatchTasksSLAAwareNoViolation() {
	suite.job.jobType = pbjob.JobType_SERVICE
	suite.job.config.sla = &pbjob.SlaConfig{
		MaximumUnavailableInstances: 2,
	}

	suite.job.instanceAvailabilityInfo = &instanceAvailabilityInfo{
		unavailableInstances: make(map[uint32]bool),
		killedInstances:      make(map[uint32]bool),
	}

	diffs := make(map[uint32]jobmgrcommon.RuntimeDiff)
	diffs[0] = jobmgrcommon.RuntimeDiff{
		jobmgrcommon.DesiredMesosTaskIDField: &mesos.TaskID{
			Value: &[]string{testNewMesosTaskID}[0],
		},
		jobmgrcommon.TerminationStatusField: &pbtask.TerminationStatus{
			Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_HOST_MAINTENANCE,
		},
	}

	oldRuntime := &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_RUNNING,
		GoalState: pbtask.TaskState_RUNNING,
		MesosTaskId: &mesos.TaskID{
			Value: &[]string{testMesosTaskID}[0],
		},
		DesiredMesosTaskId: &mesos.TaskID{
			Value: &[]string{testMesosTaskID}[0],
		},
	}
	tt := suite.job.addTaskToJobMap(0)
	tt.runtime = oldRuntime
	tt.config = &taskConfigCache{}

	// Update task runtime of only one task
	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Return(nil)
	s, r, err := suite.job.PatchTasks(context.Background(), diffs, false)
	suite.NoError(err)
	suite.Nil(r)
	suite.Len(s, 1)
	suite.Equal(uint32(0), s[0])

	// Validate the state of the task in cache is correct
	att := suite.job.GetTask(0)
	actRuntime, err := att.GetRuntime(context.Background())
	suite.NoError(err)
	suite.Equal(testNewMesosTaskID, actRuntime.GetDesiredMesosTaskId().GetValue())
}

// TestPatchTasksForUpdate tests the success case of
// patching tasks for update
func (suite *jobTestSuite) TestPatchTasksForUpdate() {
	suite.job.jobType = pbjob.JobType_SERVICE
	suite.job.config.sla = &pbjob.SlaConfig{
		MaximumUnavailableInstances: 1,
	}

	suite.job.instanceAvailabilityInfo = &instanceAvailabilityInfo{
		unavailableInstances: map[uint32]bool{1: true},
		killedInstances:      make(map[uint32]bool),
	}

	diffs := make(map[uint32]jobmgrcommon.RuntimeDiff)
	diffs[0] = jobmgrcommon.RuntimeDiff{
		jobmgrcommon.DesiredMesosTaskIDField: &mesos.TaskID{
			Value: &[]string{testNewMesosTaskID}[0],
		},
		jobmgrcommon.TerminationStatusField: &pbtask.TerminationStatus{
			Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_UPDATE,
		},
	}

	oldRuntime := &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_RUNNING,
		GoalState: pbtask.TaskState_RUNNING,
		MesosTaskId: &mesos.TaskID{
			Value: &[]string{testMesosTaskID}[0],
		},
		DesiredMesosTaskId: &mesos.TaskID{
			Value: &[]string{testMesosTaskID}[0],
		},
	}
	tt := suite.job.addTaskToJobMap(0)
	tt.runtime = oldRuntime
	tt.config = &taskConfigCache{}

	// Update task runtime of only one task
	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Return(nil)
	s, r, err := suite.job.PatchTasks(context.Background(), diffs, false)
	suite.NoError(err)
	suite.Nil(r)
	suite.Len(s, 1)
	suite.Equal(uint32(0), s[0])
	suite.Len(suite.job.instanceAvailabilityInfo.unavailableInstances, 2)

	// Validate the state of the task in cache is correct
	t := suite.job.GetTask(0)
	tRuntime, err := t.GetRuntime(context.Background())
	suite.NoError(err)
	suite.Equal(testNewMesosTaskID, tRuntime.GetDesiredMesosTaskId().GetValue())
}

// TestPatchTasksSLAAwareViolation tests the case of skipping
// the patching tasks which violate the job SLA
func (suite *jobTestSuite) TestPatchTasksSLAAwareViolation() {
	mesosTaskID0 := "941ff353-ba82-49fe-8f80-fb5bc649b04d-0-2"
	mesosTaskID1 := "941ff353-ba82-49fe-8f80-fb5bc649b04d-1-2"
	newMesosTaskID0 := "941ff353-ba82-49fe-8f80-fb5bc649b04d-0-3"
	newMesosTaskID1 := "941ff353-ba82-49fe-8f80-fb5bc649b04d-1-3"

	suite.job.jobType = pbjob.JobType_SERVICE
	suite.job.config.sla = &pbjob.SlaConfig{
		MaximumUnavailableInstances: 1,
	}

	unavailableInstances := make(map[uint32]bool)
	unavailableInstances[2] = true
	suite.job.instanceAvailabilityInfo = &instanceAvailabilityInfo{
		unavailableInstances: unavailableInstances,
		killedInstances:      make(map[uint32]bool),
	}

	diffs := make(map[uint32]jobmgrcommon.RuntimeDiff)
	diffs[0] = jobmgrcommon.RuntimeDiff{
		jobmgrcommon.DesiredMesosTaskIDField: &mesos.TaskID{
			Value: &mesosTaskID0,
		},
		jobmgrcommon.TerminationStatusField: &pbtask.TerminationStatus{
			Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_HOST_MAINTENANCE,
		},
	}

	diffs[1] = jobmgrcommon.RuntimeDiff{
		jobmgrcommon.DesiredMesosTaskIDField: &mesos.TaskID{
			Value: &mesosTaskID1,
		},
		jobmgrcommon.TerminationStatusField: &pbtask.TerminationStatus{
			Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_SLA_AWARE_RESTART,
		},
	}

	tt := suite.job.addTaskToJobMap(0)
	tt.runtime = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_RUNNING,
		GoalState: pbtask.TaskState_RUNNING,
		MesosTaskId: &mesos.TaskID{
			Value: &mesosTaskID0,
		},
		DesiredMesosTaskId: &mesos.TaskID{
			Value: &newMesosTaskID0,
		},
	}
	tt.config = &taskConfigCache{}

	tt = suite.job.addTaskToJobMap(1)
	tt.runtime = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_RUNNING,
		GoalState: pbtask.TaskState_RUNNING,
		MesosTaskId: &mesos.TaskID{
			Value: &mesosTaskID1,
		},
		DesiredMesosTaskId: &mesos.TaskID{
			Value: &newMesosTaskID1,
		},
	}
	tt.config = &taskConfigCache{}

	s, r, err := suite.job.PatchTasks(context.Background(), diffs, false)
	suite.NoError(err)
	suite.Nil(r)
	suite.Empty(s)

	// Validate the state of the task in cache is correct
	att := suite.job.GetTask(0)
	actRuntime, err := att.GetRuntime(context.Background())
	suite.NoError(err)
	suite.Equal(newMesosTaskID0, actRuntime.GetDesiredMesosTaskId().GetValue())
}

// TestPatchTasksForcePatch tests the case of force
// patching tasks even though the job SLA will be violated
func (suite *jobTestSuite) TestPatchTasksForcePatch() {
	suite.job.jobType = pbjob.JobType_SERVICE
	suite.job.config.sla = &pbjob.SlaConfig{
		MaximumUnavailableInstances: 1,
	}

	unavailableInstances := make(map[uint32]bool)
	unavailableInstances[1] = true
	suite.job.instanceAvailabilityInfo = &instanceAvailabilityInfo{
		unavailableInstances: unavailableInstances,
		killedInstances:      make(map[uint32]bool),
	}

	diffs := make(map[uint32]jobmgrcommon.RuntimeDiff)
	diffs[0] = jobmgrcommon.RuntimeDiff{
		jobmgrcommon.DesiredMesosTaskIDField: &mesos.TaskID{
			Value: &[]string{testNewMesosTaskID}[0],
		},
		jobmgrcommon.TerminationStatusField: &pbtask.TerminationStatus{
			Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_HOST_MAINTENANCE,
		},
	}

	oldRuntime := &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_RUNNING,
		GoalState: pbtask.TaskState_RUNNING,
		MesosTaskId: &mesos.TaskID{
			Value: &[]string{testMesosTaskID}[0],
		},
		DesiredMesosTaskId: &mesos.TaskID{
			Value: &[]string{testMesosTaskID}[0],
		},
	}
	tt := suite.job.addTaskToJobMap(0)
	tt.runtime = oldRuntime
	tt.config = &taskConfigCache{}

	// Update task runtime of only one task
	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Return(nil)
	s, r, err := suite.job.PatchTasks(context.Background(), diffs, true)
	suite.NoError(err)
	suite.Nil(r)
	suite.Len(s, 1)
	suite.Equal(uint32(0), s[0])

	// Validate the state of the task in cache is correct
	att := suite.job.GetTask(0)
	actRuntime, err := att.GetRuntime(context.Background())
	suite.NoError(err)
	suite.Equal(testNewMesosTaskID, actRuntime.GetDesiredMesosTaskId().GetValue())
}

// TestPatchTasksSLAAwareInstanceRestart tests the success case
// of patching tasks when the instances are restarted
func (suite *jobTestSuite) TestPatchTasksSLAAwareInstanceRestart() {
	suite.job.jobType = pbjob.JobType_SERVICE
	suite.job.config.sla = &pbjob.SlaConfig{
		MaximumUnavailableInstances: 1,
	}

	unavailableInstances := make(map[uint32]bool)
	unavailableInstances[1] = true
	suite.job.instanceAvailabilityInfo = &instanceAvailabilityInfo{
		unavailableInstances: unavailableInstances,
		killedInstances:      make(map[uint32]bool),
	}

	diffs := make(map[uint32]jobmgrcommon.RuntimeDiff)
	diffs[0] = jobmgrcommon.RuntimeDiff{
		jobmgrcommon.DesiredMesosTaskIDField: &mesos.TaskID{
			Value: &[]string{testNewMesosTaskID}[0],
		},
		jobmgrcommon.TerminationStatusField: &pbtask.TerminationStatus{
			Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_RESTART,
		},
	}

	oldRuntime := &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_RUNNING,
		GoalState: pbtask.TaskState_RUNNING,
		MesosTaskId: &mesos.TaskID{
			Value: &[]string{testMesosTaskID}[0],
		},
		DesiredMesosTaskId: &mesos.TaskID{
			Value: &[]string{testMesosTaskID}[0],
		},
	}
	tt := suite.job.addTaskToJobMap(0)
	tt.runtime = oldRuntime
	tt.config = &taskConfigCache{}

	// Update task runtime of only one task
	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Return(nil)
	s, r, err := suite.job.PatchTasks(context.Background(), diffs, false)
	suite.NoError(err)
	suite.Nil(r)
	suite.Len(s, 1)
	suite.Equal(uint32(0), s[0])

	// Validate the state of the task in cache is correct
	att := suite.job.GetTask(0)
	actRuntime, err := att.GetRuntime(context.Background())
	suite.NoError(err)
	suite.Equal(testNewMesosTaskID, actRuntime.GetDesiredMesosTaskId().GetValue())
}

// TestPatchTasksSLAAwareMesosTaskIDChange tests the success case
// of patching tasks when the desired mesos task id has changed
func (suite *jobTestSuite) TestPatchTasksSLAAwareMesosTaskIDChange() {
	suite.job.jobType = pbjob.JobType_SERVICE
	suite.job.config.sla = &pbjob.SlaConfig{
		MaximumUnavailableInstances: 1,
	}

	unavailableInstances := make(map[uint32]bool)
	unavailableInstances[1] = true
	suite.job.instanceAvailabilityInfo = &instanceAvailabilityInfo{
		unavailableInstances: unavailableInstances,
		killedInstances:      make(map[uint32]bool),
	}

	diffs := make(map[uint32]jobmgrcommon.RuntimeDiff)
	diffs[0] = jobmgrcommon.RuntimeDiff{
		jobmgrcommon.DesiredMesosTaskIDField: &mesos.TaskID{
			Value: &[]string{testNewMesosTaskID}[0],
		},
	}

	oldRuntime := &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_RUNNING,
		GoalState: pbtask.TaskState_RUNNING,
		MesosTaskId: &mesos.TaskID{
			Value: &[]string{testMesosTaskID}[0],
		},
		DesiredMesosTaskId: &mesos.TaskID{
			Value: &[]string{testMesosTaskID}[0],
		},
	}
	tt := suite.job.addTaskToJobMap(0)
	tt.runtime = oldRuntime
	tt.config = &taskConfigCache{}

	// Update task runtime of only one task
	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Return(nil)
	s, r, err := suite.job.PatchTasks(context.Background(), diffs, false)
	suite.NoError(err)
	suite.Nil(r)
	suite.Len(s, 1)
	suite.Equal(uint32(0), s[0])

	// Validate the state of the task in cache is correct
	att := suite.job.GetTask(0)
	actRuntime, err := att.GetRuntime(context.Background())
	suite.NoError(err)
	suite.Equal(testNewMesosTaskID, actRuntime.GetDesiredMesosTaskId().GetValue())
	suite.True(suite.job.instanceAvailabilityInfo.killedInstances[0])
}

// TestPatchTasksSLAAwareDesiredConfigVersionChange tests the success case
// of patching tasks when the desired config version has changed
func (suite *jobTestSuite) TestPatchTasksSLAAwareDesiredConfigVersionChange() {
	suite.job.jobType = pbjob.JobType_SERVICE
	suite.job.config.sla = &pbjob.SlaConfig{
		MaximumUnavailableInstances: 1,
	}

	unavailableInstances := make(map[uint32]bool)
	unavailableInstances[1] = true
	suite.job.instanceAvailabilityInfo = &instanceAvailabilityInfo{
		unavailableInstances: unavailableInstances,
		killedInstances:      make(map[uint32]bool),
	}

	desiredConfigVersion := uint64(2)
	diffs := make(map[uint32]jobmgrcommon.RuntimeDiff)
	diffs[0] = jobmgrcommon.RuntimeDiff{
		jobmgrcommon.DesiredConfigVersionField: desiredConfigVersion,
	}

	oldRuntime := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_RUNNING,
		GoalState:            pbtask.TaskState_RUNNING,
		ConfigVersion:        1,
		DesiredConfigVersion: 1,
	}
	tt := suite.job.addTaskToJobMap(0)
	tt.runtime = oldRuntime
	tt.config = &taskConfigCache{}

	// Update task runtime of only one task
	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Return(nil)
	suite.taskConfigV2Ops.EXPECT().
		GetTaskConfig(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Return(nil, nil, nil)
	s, r, err := suite.job.PatchTasks(context.Background(), diffs, false)
	suite.NoError(err)
	suite.Nil(r)
	suite.Len(s, 1)
	suite.Equal(uint32(0), s[0])

	// Validate the state of the task in cache is correct
	att := suite.job.GetTask(0)
	actRuntime, err := att.GetRuntime(context.Background())
	suite.NoError(err)
	suite.Equal(desiredConfigVersion, actRuntime.GetDesiredConfigVersion())
	suite.True(suite.job.instanceAvailabilityInfo.killedInstances[0])
}

// TestPatchTasksSLAAwareInstanceDelete tests the success case
// of patching tasks when the instances are deleted
func (suite *jobTestSuite) TestPatchTasksSLAAwareInstanceDelete() {
	suite.job.jobType = pbjob.JobType_SERVICE
	suite.job.config.sla = &pbjob.SlaConfig{
		MaximumUnavailableInstances: 1,
	}

	unavailableInstances := make(map[uint32]bool)
	unavailableInstances[0] = true
	suite.job.instanceAvailabilityInfo = &instanceAvailabilityInfo{
		unavailableInstances: unavailableInstances,
		killedInstances:      make(map[uint32]bool),
	}

	diffs := make(map[uint32]jobmgrcommon.RuntimeDiff)
	diffs[0] = jobmgrcommon.RuntimeDiff{
		jobmgrcommon.GoalStateField: pbtask.TaskState_DELETED,
	}

	oldRuntime := &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_PENDING,
		GoalState: pbtask.TaskState_RUNNING,
		MesosTaskId: &mesos.TaskID{
			Value: &[]string{testMesosTaskID}[0],
		},
		DesiredMesosTaskId: &mesos.TaskID{
			Value: &[]string{testMesosTaskID}[0],
		},
	}
	tt := suite.job.addTaskToJobMap(0)
	tt.runtime = oldRuntime
	tt.config = &taskConfigCache{}

	// Update task runtime of only one task
	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Return(nil)
	s, r, err := suite.job.PatchTasks(context.Background(), diffs, false)
	suite.NoError(err)
	suite.Nil(r)
	suite.Len(s, 1)
	suite.Equal(uint32(0), s[0])

	// Validate the state of the task in cache is correct
	att := suite.job.GetTask(0)
	actRuntime, err := att.GetRuntime(context.Background())
	suite.NoError(err)
	suite.Equal(pbtask.TaskState_DELETED, actRuntime.GetGoalState())
	suite.Empty(suite.job.instanceAvailabilityInfo.unavailableInstances)
	suite.Empty(suite.job.instanceAvailabilityInfo.killedInstances)
}

// TestPatchTasksSLAAwareInstanceKill tests the success case
// of patching tasks when the instances are killed
func (suite *jobTestSuite) TestPatchTasksSLAAwareInstanceKill() {
	suite.job.jobType = pbjob.JobType_SERVICE
	suite.job.config.sla = &pbjob.SlaConfig{
		MaximumUnavailableInstances: 1,
	}

	unavailableInstances := make(map[uint32]bool)
	unavailableInstances[0] = true
	suite.job.instanceAvailabilityInfo = &instanceAvailabilityInfo{
		unavailableInstances: unavailableInstances,
		killedInstances:      make(map[uint32]bool),
	}

	diffs := make(map[uint32]jobmgrcommon.RuntimeDiff)
	diffs[0] = jobmgrcommon.RuntimeDiff{
		jobmgrcommon.GoalStateField: pbtask.TaskState_KILLED,
	}

	oldRuntime := &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_PENDING,
		GoalState: pbtask.TaskState_RUNNING,
		MesosTaskId: &mesos.TaskID{
			Value: &[]string{testMesosTaskID}[0],
		},
		DesiredMesosTaskId: &mesos.TaskID{
			Value: &[]string{testMesosTaskID}[0],
		},
	}
	tt := suite.job.addTaskToJobMap(0)
	tt.runtime = oldRuntime
	tt.config = &taskConfigCache{}

	// Update task runtime of only one task
	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Return(nil)
	s, r, err := suite.job.PatchTasks(context.Background(), diffs, false)
	suite.NoError(err)
	suite.Nil(r)
	suite.Len(s, 1)
	suite.Equal(uint32(0), s[0])

	// Validate the state of the task in cache is correct
	att := suite.job.GetTask(0)
	actRuntime, err := att.GetRuntime(context.Background())
	suite.NoError(err)
	suite.Equal(pbtask.TaskState_KILLED, actRuntime.GetGoalState())
	suite.Empty(suite.job.instanceAvailabilityInfo.unavailableInstances)
	suite.True(suite.job.instanceAvailabilityInfo.killedInstances[0])
}

// TestPatchTasksSLAAwareUnknownTaskState tests the case of patching
// tasks in a SLA aware manner when the instance state is unknown.
func (suite *jobTestSuite) TestPatchTasksSLAAwareUnknownTaskState() {
	suite.job.jobType = pbjob.JobType_SERVICE
	suite.job.config.sla = &pbjob.SlaConfig{
		MaximumUnavailableInstances: 2,
	}

	suite.job.instanceAvailabilityInfo = &instanceAvailabilityInfo{
		unavailableInstances: make(map[uint32]bool),
		killedInstances:      make(map[uint32]bool),
	}

	diffs := make(map[uint32]jobmgrcommon.RuntimeDiff)
	diffs[0] = jobmgrcommon.RuntimeDiff{
		jobmgrcommon.DesiredMesosTaskIDField: &mesos.TaskID{
			Value: &[]string{testNewMesosTaskID}[0],
		},
	}

	oldRuntime := &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_UNKNOWN,
		GoalState: pbtask.TaskState_UNKNOWN,
		MesosTaskId: &mesos.TaskID{
			Value: &[]string{testMesosTaskID}[0],
		},
		DesiredMesosTaskId: &mesos.TaskID{
			Value: &[]string{testMesosTaskID}[0],
		},
	}
	tt := suite.job.addTaskToJobMap(0)
	tt.runtime = oldRuntime
	tt.config = &taskConfigCache{}

	s, r, err := suite.job.PatchTasks(context.Background(), diffs, false)
	suite.NoError(err)
	suite.Empty(s)
	suite.Len(r, 1)
	suite.Equal(uint32(0), r[0])

	// Validate the state of the task in cache is correct
	att := suite.job.GetTask(0)
	actRuntime, err := att.GetRuntime(context.Background())
	suite.NoError(err)
	suite.Equal(testMesosTaskID, actRuntime.GetDesiredMesosTaskId().GetValue())
}

// TestPatchTasksSLAAwareInstanceReinitialize tests the success case
// of patching tasks when the instances are re-initialized
func (suite *jobTestSuite) TestPatchTasksSLAAwareInstanceReinitialize() {
	suite.job.jobType = pbjob.JobType_SERVICE
	suite.job.config.sla = &pbjob.SlaConfig{
		MaximumUnavailableInstances: 1,
	}

	killedInstances := make(map[uint32]bool)
	killedInstances[0] = true
	suite.job.instanceAvailabilityInfo = &instanceAvailabilityInfo{
		unavailableInstances: make(map[uint32]bool),
		killedInstances:      killedInstances,
	}

	diffs := make(map[uint32]jobmgrcommon.RuntimeDiff)
	diffs[0] = jobmgrcommon.RuntimeDiff{
		jobmgrcommon.StateField:     pbtask.TaskState_INITIALIZED,
		jobmgrcommon.GoalStateField: pbtask.TaskState_RUNNING,
		jobmgrcommon.MesosTaskIDField: &mesos.TaskID{
			Value: &[]string{testNewMesosTaskID}[0],
		},
		jobmgrcommon.DesiredMesosTaskIDField: &mesos.TaskID{
			Value: &[]string{testNewMesosTaskID}[0],
		},
	}

	oldRuntime := &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_KILLED,
		GoalState: pbtask.TaskState_RUNNING,
		MesosTaskId: &mesos.TaskID{
			Value: &[]string{testMesosTaskID}[0],
		},
		DesiredMesosTaskId: &mesos.TaskID{
			Value: &[]string{testNewMesosTaskID}[0],
		},
	}
	tt := suite.job.addTaskToJobMap(0)
	tt.runtime = oldRuntime
	tt.config = &taskConfigCache{}

	// Update task runtime of only one task
	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Return(nil)
	s, r, err := suite.job.PatchTasks(context.Background(), diffs, false)
	suite.NoError(err)
	suite.Nil(r)
	suite.Len(s, 1)
	suite.Equal(uint32(0), s[0])

	// Validate the state of the task in cache is correct
	att := suite.job.GetTask(0)
	actRuntime, err := att.GetRuntime(context.Background())
	suite.NoError(err)
	suite.Equal(pbtask.TaskState_INITIALIZED, actRuntime.GetState())
	suite.True(suite.job.instanceAvailabilityInfo.unavailableInstances[0])
	suite.Empty(suite.job.instanceAvailabilityInfo.killedInstances)
}

// TestPatchTasksSLAAwarePopulateJobConfigFailure tests the failure case of patching
// tasks in a SLA aware manner due to error while populating job config.
func (suite *jobTestSuite) TestPatchTasksSLAAwarePopulateJobConfigFailure() {
	suite.job.jobType = pbjob.JobType_SERVICE
	suite.job.config = nil

	diffs := make(map[uint32]jobmgrcommon.RuntimeDiff)
	diffs[0] = jobmgrcommon.RuntimeDiff{
		jobmgrcommon.DesiredMesosTaskIDField: &mesos.TaskID{
			Value: &[]string{testNewMesosTaskID}[0],
		},
		jobmgrcommon.TerminationStatusField: &pbtask.TerminationStatus{
			Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_HOST_MAINTENANCE,
		},
	}

	oldRuntime := &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_UNKNOWN,
		GoalState: pbtask.TaskState_UNKNOWN,
		MesosTaskId: &mesos.TaskID{
			Value: &[]string{testMesosTaskID}[0],
		},
		DesiredMesosTaskId: &mesos.TaskID{
			Value: &[]string{testMesosTaskID}[0],
		},
	}
	tt := suite.job.addTaskToJobMap(0)
	tt.runtime = oldRuntime
	tt.config = &taskConfigCache{}

	suite.jobConfigOps.EXPECT().
		Get(
			gomock.Any(),
			suite.jobID,
			suite.job.runtime.GetConfigurationVersion(),
		).Return(nil, nil, fmt.Errorf("test error"))
	s, r, err := suite.job.PatchTasks(context.Background(), diffs, false)
	suite.Error(err)
	suite.Nil(s)
	suite.Nil(r)
}

// TestPatchTasksSLAAwareGetTaskRuntimeFailure tests the failure case of patching
// tasks in a SLA aware manner due to error while getting task runtime.
func (suite *jobTestSuite) TestPatchTasksSLAAwareGetTaskRuntimeFailure() {
	suite.job.jobType = pbjob.JobType_SERVICE
	suite.job.config.sla = &pbjob.SlaConfig{
		MaximumUnavailableInstances: 2,
	}

	diffs := make(map[uint32]jobmgrcommon.RuntimeDiff)
	diffs[0] = jobmgrcommon.RuntimeDiff{
		jobmgrcommon.DesiredMesosTaskIDField: &mesos.TaskID{
			Value: &[]string{testNewMesosTaskID}[0],
		},
		jobmgrcommon.TerminationStatusField: &pbtask.TerminationStatus{
			Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_HOST_MAINTENANCE,
		},
	}

	suite.job.addTaskToJobMap(0)

	suite.taskStore.EXPECT().GetTaskRuntime(gomock.Any(), suite.jobID, uint32(0)).
		Return(nil, fmt.Errorf("fake error"))
	s, r, err := suite.job.PatchTasks(context.Background(), diffs, false)
	suite.Error(err)
	suite.Nil(s)
	suite.Nil(r)
}

// TestJobUpdateResourceUsage tests updating the resource usage for job
func (suite *jobTestSuite) TestJobUpdateResourceUsage() {
	taskResourceUsage := map[string]float64{
		common.CPU:    float64(1),
		common.GPU:    float64(0),
		common.MEMORY: float64(1)}

	suite.job.resourceUsage = map[string]float64{
		common.CPU:    float64(10),
		common.GPU:    float64(2),
		common.MEMORY: float64(10)}

	suite.job.UpdateResourceUsage(taskResourceUsage)
	updatedResourceUsage := map[string]float64{
		common.CPU:    float64(11),
		common.GPU:    float64(2),
		common.MEMORY: float64(11)}
	suite.Equal(updatedResourceUsage, suite.job.GetResourceUsage())
}

// TestDelete tests deleting a job
func (suite *jobTestSuite) TestDelete() {
	suite.jobStore.EXPECT().
		DeleteJob(gomock.Any(), suite.jobID.GetValue()).
		Return(nil)
	suite.jobIndexOps.EXPECT().
		Delete(gomock.Any(), suite.jobID).
		Return(nil)
	suite.activeJobsOps.EXPECT().
		Delete(gomock.Any(), suite.jobID).
		Return(nil)

	suite.job.Delete(context.Background())
}

// TestDelete tests failure deleting a job
func (suite *jobTestSuite) TestDeleteFailure() {
	// DeleteJob failure
	suite.jobIndexOps.EXPECT().
		Delete(gomock.Any(), suite.jobID).
		Return(nil)
	suite.jobStore.EXPECT().
		DeleteJob(gomock.Any(), suite.jobID.GetValue()).
		Return(yarpcerrors.InternalErrorf("DeleteJob error"))
	err := suite.job.Delete(context.Background())
	suite.Error(err)
	suite.Equal("DeleteJob error", yarpcerrors.ErrorMessage(err))

	// jobIndexOp failure
	suite.jobIndexOps.EXPECT().
		Delete(gomock.Any(), suite.jobID).
		Return(yarpcerrors.InternalErrorf("jobIndexOps error"))
	err = suite.job.Delete(context.Background())
	suite.Error(err)
	suite.Equal("jobIndexOps error", yarpcerrors.ErrorMessage(err))

	// Delete active job error.
	suite.jobStore.EXPECT().
		DeleteJob(gomock.Any(), suite.jobID.GetValue()).
		Return(nil)
	suite.jobIndexOps.EXPECT().
		Delete(gomock.Any(), suite.jobID).
		Return(nil)
	suite.activeJobsOps.EXPECT().
		Delete(gomock.Any(), suite.jobID).
		Return(yarpcerrors.InternalErrorf("Delete active jobs error"))
	err = suite.job.Delete(context.Background())
	suite.Error(err)
	suite.Equal("Delete active jobs error", yarpcerrors.ErrorMessage(err))
}

// TestRecalculateResourceUsage tests recalculating the resource usage for job
// on recovery
func (suite *jobTestSuite) TestRecalculateResourceUsage() {
	var oldRuntime *pbtask.RuntimeInfo

	instanceCount := uint32(5)
	initialResourceMap := map[string]float64{
		common.CPU:    float64(5),
		common.GPU:    float64(0),
		common.MEMORY: float64(5)}

	// Make sure initial resource map points to expected values
	suite.job.resourceUsage = initialResourceMap
	suite.Equal(initialResourceMap, suite.job.GetResourceUsage())

	// Add 5 tasks to the job of which 2 are terminal and have valid
	// resource usage.
	for i := uint32(0); i < instanceCount; i++ {
		if i%2 == 0 {
			// initialize two terminal tasks
			oldRuntime = initializeCurrentRuntime(pbtask.TaskState_RUNNING)
		} else {
			oldRuntime = initializeCurrentRuntime(pbtask.TaskState_SUCCEEDED)
			oldRuntime.ResourceUsage = map[string]float64{
				common.CPU:    float64(3),
				common.GPU:    float64(0),
				common.MEMORY: float64(3)}
		}
		suite.job.addTaskToJobMap(i)
		suite.taskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), suite.jobID, i).Return(oldRuntime, nil)
	}
	suite.job.RecalculateResourceUsage(context.Background())

	// initial resource map for this job should be reset and recalculated
	// by adding resource usage of the two terminal tasks
	updatedResourceUsage := map[string]float64{
		common.CPU:    float64(6),
		common.GPU:    float64(0),
		common.MEMORY: float64(6)}
	suite.Equal(updatedResourceUsage, suite.job.GetResourceUsage())
}

// TestRecalculateResourceUsageError tests DB error in recalculating the
// resource usage for job on recovery
func (suite *jobTestSuite) TestRecalculateResourceUsageError() {
	initialResourceMap := map[string]float64{
		common.CPU:    float64(5),
		common.GPU:    float64(0),
		common.MEMORY: float64(5)}
	suite.job.resourceUsage = initialResourceMap

	suite.job.addTaskToJobMap(uint32(0))
	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, uint32(0)).
		Return(nil, dbError)

	// This will reset the resource usage map to 0 and try to update it with
	// terminal task resource usage. But on getting error in GetTaskRuntime,
	// it will log the error and move on. So the resource usage will stay at 0.
	suite.job.RecalculateResourceUsage(context.Background())
	suite.Equal(createEmptyResourceUsageMap(), suite.job.GetResourceUsage())
}

func (suite *jobTestSuite) TestJobRemoveTask() {
	suite.job.addTaskToJobMap(0)
	suite.NotNil(suite.job.GetTask(0))
	suite.job.RemoveTask(0)
	suite.Nil(suite.job.GetTask(0))
}

// TestJobCompareAndSetConfigSuccess tests the success case of
// CompareAndSetConfig
func (suite *jobTestSuite) TestJobCompareAndSetConfigSuccess() {
	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID.GetValue()).
		Return(suite.job.config.changeLog.Version, nil)

	suite.jobConfigOps.EXPECT().
		Create(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Return(nil)
	suite.jobIndexOps.EXPECT().
		Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	newConfig, err := suite.job.CompareAndSetConfig(
		context.Background(),
		&pbjob.JobConfig{
			InstanceCount: 100,
			ChangeLog: &peloton.ChangeLog{
				Version: suite.job.config.changeLog.Version,
			},
		}, &models.ConfigAddOn{},
		nil,
	)
	suite.NoError(err)
	suite.Equal(newConfig.GetInstanceCount(), uint32(100))
}

// TestJobCompareAndSetTaskSuccess tests the success case of
// CompareAndSetTask
func (suite *jobTestSuite) TestCompareAndSetTaskSuccess() {
	var labels []*peloton.Label

	labels = append(labels, initializeLabel("key", "value"))
	version := uint64(3)
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	runtime.ConfigVersion = version - 1
	tt := &task{
		id:      0,
		jobID:   suite.jobID,
		runtime: runtime,
		jobFactory: &jobFactory{
			mtx:             NewMetrics(tally.NoopScope),
			taskMetrics:     NewTaskMetrics(tally.NoopScope),
			taskStore:       suite.taskStore,
			taskConfigV2Ops: suite.taskConfigV2Ops,
			running:         true,
			jobs:            map[string]*job{},
		},
		jobType: pbjob.JobType_BATCH,
	}
	for _, l := range suite.listeners {
		tt.jobFactory.listeners = append(tt.jobFactory.listeners, l)
	}

	tt.jobFactory.jobs[suite.jobID.GetValue()] = suite.job
	suite.job.tasks[0] = tt

	newRuntime := initializeTaskRuntime(pbtask.TaskState_RUNNING, 2)
	newRuntime.GoalState = pbtask.TaskState_SUCCEEDED
	newRuntime.ConfigVersion = version

	taskConfig := &pbtask.TaskConfig{
		Labels: labels,
	}

	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			suite.jobID,
			uint32(0),
			gomock.Any(),
			gomock.Any()).
		Do(func(
			ctx context.Context,
			jobID *peloton.JobID,
			instanceID uint32,
			runtime *pbtask.RuntimeInfo,
			jobType pbjob.JobType) {
			suite.Equal(runtime.GetState(), pbtask.TaskState_RUNNING)
			suite.Equal(runtime.Revision.Version, uint64(3))
			suite.Equal(runtime.GetGoalState(), pbtask.TaskState_SUCCEEDED)
		}).
		Return(nil)

	suite.taskConfigV2Ops.EXPECT().
		GetTaskConfig(
			gomock.Any(),
			suite.jobID,
			uint32(0),
			version).
		Return(taskConfig, nil, nil)

	_, err := suite.job.CompareAndSetTask(
		context.Background(),
		0,
		newRuntime,
		false,
	)
	suite.NoError(err)
}

// TestCompareAndSetTaskAddTaskError tests failure case of
// CompareAndSetTask due to error getting task runtime from DB
func (suite *jobTestSuite) TestCompareAndSetTaskAddTaskError() {
	newRuntime := initializeTaskRuntime(pbtask.TaskState_RUNNING, 2)
	newRuntime.GoalState = pbtask.TaskState_SUCCEEDED
	newRuntime.ConfigVersion = 0

	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, uint32(0)).
		Return(nil, fmt.Errorf("fake db error"))

	runtimeCopy, err := suite.job.CompareAndSetTask(
		context.Background(),
		0,
		newRuntime,
		false,
	)
	suite.Nil(runtimeCopy)
	suite.Error(err)
}

// TestJobCompareAndSetConfigInvalidConfigVersion tests CompareAndSetConfig
// fails due to invalid version
func (suite *jobTestSuite) TestJobCompareAndSetConfigInvalidConfigVersion() {
	newConfig, err := suite.job.CompareAndSetConfig(
		context.Background(),
		&pbjob.JobConfig{
			InstanceCount: 100,
			ChangeLog: &peloton.ChangeLog{
				Version: suite.job.config.changeLog.Version - 1,
			},
		}, &models.ConfigAddOn{},
		nil,
	)
	suite.Error(err)
	suite.Nil(newConfig)
}

// TestRepopulateSLAInfoStatelessJob tests repopulating
// the instance availability info of a stateless job
func (suite *jobTestSuite) TestRepopulateInstanceAvailabilityInfoStatelessJob() {
	suite.job.jobType = pbjob.JobType_SERVICE
	suite.job.addTaskToJobMap(0)
	suite.job.addTaskToJobMap(1)
	suite.job.addTaskToJobMap(2)

	suite.taskStore.EXPECT().GetTaskRuntime(gomock.Any(), suite.jobID, uint32(0)).
		Return(&pbtask.RuntimeInfo{
			State:     pbtask.TaskState_RUNNING,
			GoalState: pbtask.TaskState_RUNNING,
			TerminationStatus: &pbtask.TerminationStatus{
				Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_HOST_MAINTENANCE,
			},
		}, nil)

	suite.taskStore.EXPECT().GetTaskRuntime(gomock.Any(), suite.jobID, uint32(1)).
		Return(&pbtask.RuntimeInfo{
			State:     pbtask.TaskState_RUNNING,
			GoalState: pbtask.TaskState_RUNNING,
			TerminationStatus: &pbtask.TerminationStatus{
				Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_DEADLINE_TIMEOUT_EXCEEDED,
			},
		}, nil)

	suite.taskStore.EXPECT().GetTaskRuntime(gomock.Any(), suite.jobID, uint32(2)).
		Return(&pbtask.RuntimeInfo{
			State:     pbtask.TaskState_RUNNING,
			GoalState: pbtask.TaskState_KILLED,
		}, nil)

	suite.Nil(suite.job.RepopulateInstanceAvailabilityInfo(context.Background()))
	suite.NotNil(suite.job.instanceAvailabilityInfo)
	suite.Len(suite.job.instanceAvailabilityInfo.unavailableInstances, 1)
	suite.True(suite.job.instanceAvailabilityInfo.unavailableInstances[0])
	suite.Len(suite.job.instanceAvailabilityInfo.killedInstances, 2)
	suite.True(suite.job.instanceAvailabilityInfo.killedInstances[1])
	suite.True(suite.job.instanceAvailabilityInfo.killedInstances[2])
}

// TestRepopulateSLAInfoBatchJob tests repopulating
// the SLA i nstance availability of a batch job
func (suite *jobTestSuite) TestRepopulateInstanceAvailabilityInfoBatchJob() {
	suite.Nil(suite.job.RepopulateInstanceAvailabilityInfo(context.Background()))
	suite.Nil(suite.job.instanceAvailabilityInfo)
}

// TestGetInstanceAvailabilityTypeSpecifyInstances tests getting the instance
// availability type for the given subset of instances
func (suite *jobTestSuite) TestGetInstanceAvailabilityTypeSpecifyInstances() {
	instances := []uint32{0, 2, 3}

	suite.job.addTaskToJobMap(0).runtime = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_RUNNING,
		GoalState: pbtask.TaskState_RUNNING,
		Healthy:   pbtask.HealthState_HEALTHY,
	}

	suite.job.addTaskToJobMap(1).runtime = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_RUNNING,
		GoalState: pbtask.TaskState_RUNNING,
		Healthy:   pbtask.HealthState_DISABLED,
	}

	suite.job.addTaskToJobMap(2).runtime = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_PENDING,
		GoalState: pbtask.TaskState_RUNNING,
		Healthy:   pbtask.HealthState_DISABLED,
	}

	instanceAvailabilityMap := suite.job.GetInstanceAvailabilityType(
		context.Background(),
		instances...,
	)

	suite.Len(instanceAvailabilityMap, len(instances))
	suite.Equal(jobmgrcommon.InstanceAvailability_AVAILABLE, instanceAvailabilityMap[0])
	suite.Equal(jobmgrcommon.InstanceAvailability_UNAVAILABLE, instanceAvailabilityMap[2])
	suite.Equal(jobmgrcommon.InstanceAvailability_INVALID, instanceAvailabilityMap[3])
}

// TestGetInstanceAvailabilityTypeNoInstancesSpecified tests getting the instance
// availability type when no instances are specified
func (suite *jobTestSuite) TestGetInstanceAvailabilityTypeNoInstancesSpecified() {
	suite.job.addTaskToJobMap(0).runtime = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_RUNNING,
		GoalState: pbtask.TaskState_RUNNING,
		Healthy:   pbtask.HealthState_HEALTHY,
	}

	suite.job.addTaskToJobMap(1)

	suite.job.addTaskToJobMap(2).runtime = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_PENDING,
		GoalState: pbtask.TaskState_RUNNING,
		Healthy:   pbtask.HealthState_DISABLED,
	}

	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, uint32(1)).
		Return(nil, fmt.Errorf("fake error"))

	instanceAvailabilityMap := suite.job.GetInstanceAvailabilityType(
		context.Background(),
	)

	suite.Len(instanceAvailabilityMap, len(suite.job.tasks))
	suite.Equal(jobmgrcommon.InstanceAvailability_AVAILABLE, instanceAvailabilityMap[0])
	suite.Equal(jobmgrcommon.InstanceAvailability_INVALID, instanceAvailabilityMap[1])
	suite.Equal(jobmgrcommon.InstanceAvailability_UNAVAILABLE, instanceAvailabilityMap[2])
}

// TestJobCreateWorkflowWithSameConfigSuccess tests create a workflow
// with same config succeeds and is noop
func (suite *jobTestSuite) TestJobCreateWorkflowWithSameConfigSuccess() {
	opaque := "test"
	updateID := &peloton.UpdateID{Value: uuid.NewRandom().String()}

	workflowType := models.WorkflowType_UPDATE
	updateConfig := &pbupdate.UpdateConfig{
		BatchSize: 10,
	}
	suite.job.runtime.UpdateID = updateID
	oldConfigVersion := suite.job.runtime.GetConfigurationVersion()
	oldWorkflowVersion := suite.job.runtime.GetWorkflowVersion()
	desiredStateVersion := suite.job.runtime.GetDesiredStateVersion()
	entityVersion := versionutil.GetJobEntityVersion(
		oldConfigVersion,
		desiredStateVersion,
		oldWorkflowVersion,
	)
	prevConfig := &pbjob.JobConfig{
		DefaultConfig: &pbtask.TaskConfig{},
		ChangeLog:     &peloton.ChangeLog{Version: 1},
	}
	jobConfig := prevConfig
	configAddOn := &models.ConfigAddOn{}
	updateModel := &models.UpdateModel{
		UpdateID:     updateID,
		State:        pbupdate.State_ROLLING_FORWARD,
		Type:         workflowType,
		UpdateConfig: updateConfig,
		OpaqueData:   &peloton.OpaqueData{Data: opaque},
	}

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), updateID).
		Return(updateModel, nil)

	updateID, newEntityVersion, err := suite.job.CreateWorkflow(
		context.Background(),
		workflowType,
		updateConfig,
		entityVersion,
		WithConfig(
			jobConfig,
			prevConfig,
			configAddOn,
			nil,
		),
		WithOpaqueData(&peloton.OpaqueData{Data: opaque}),
	)

	suite.NotNil(updateID)
	suite.NoError(err)
	suite.Nil(suite.job.workflows[updateID.GetValue()])
	suite.Equal(newEntityVersion, versionutil.GetJobEntityVersion(
		oldConfigVersion,
		desiredStateVersion,
		oldWorkflowVersion,
	))
}

// TestJobCreateWorkflowWithDifferntOpaqueDateSuccess tests create a workflow
// with same config but different opaque data succeeds
func (suite *jobTestSuite) TestJobCreateWorkflowWithDifferentOpaqueDateSuccess() {
	opaque := "test"
	updatedOpaque := "test2"
	updateID := &peloton.UpdateID{Value: uuid.NewRandom().String()}

	workflowType := models.WorkflowType_UPDATE
	updateConfig := &pbupdate.UpdateConfig{
		BatchSize: 10,
	}
	suite.job.runtime.UpdateID = updateID
	oldConfigVersion := suite.job.runtime.GetConfigurationVersion()
	oldWorkflowVersion := suite.job.runtime.GetWorkflowVersion()
	desiredStateVersion := suite.job.runtime.GetDesiredStateVersion()
	entityVersion := versionutil.GetJobEntityVersion(
		oldConfigVersion,
		desiredStateVersion,
		oldWorkflowVersion,
	)
	prevConfig := &pbjob.JobConfig{
		DefaultConfig: &pbtask.TaskConfig{},
		ChangeLog:     &peloton.ChangeLog{Version: 1},
	}
	jobConfig := prevConfig
	configAddOn := &models.ConfigAddOn{}
	updateModel := &models.UpdateModel{
		UpdateID:     updateID,
		State:        pbupdate.State_ROLLING_FORWARD,
		Type:         workflowType,
		UpdateConfig: updateConfig,
		OpaqueData:   &peloton.OpaqueData{Data: opaque},
	}

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), updateID).
		Return(updateModel, nil)

	suite.jobRuntimeOps.EXPECT().
		Upsert(gomock.Any(), suite.jobID, gomock.Any()).
		Return(nil)

	suite.updateStore.EXPECT().
		ModifyUpdate(gomock.Any(), gomock.Any()).
		Return(nil)

	updateID, newEntityVersion, err := suite.job.CreateWorkflow(
		context.Background(),
		workflowType,
		updateConfig,
		entityVersion,
		WithConfig(
			jobConfig,
			prevConfig,
			configAddOn,
			nil,
		),
		WithOpaqueData(&peloton.OpaqueData{Data: updatedOpaque}),
	)

	suite.NotNil(updateID)
	suite.NoError(err)
	suite.Nil(suite.job.workflows[updateID.GetValue()])
	suite.Equal(newEntityVersion, versionutil.GetJobEntityVersion(
		oldConfigVersion,
		desiredStateVersion,
		oldWorkflowVersion+1,
	))
}

// TestJobCreateWorkflowSuccess tests the success case of
// creating workflow
func (suite *jobTestSuite) TestJobCreateWorkflowSuccess() {
	var instancesAdded []uint32
	var instnacesRemoved []uint32
	instancesUpdated := []uint32{0, 1, 2}
	opaque := "test"

	workflowType := models.WorkflowType_START
	updateConfig := &pbupdate.UpdateConfig{
		BatchSize: 10,
	}
	oldConfigVersion := suite.job.runtime.GetConfigurationVersion()
	oldWorkflowVersion := suite.job.runtime.GetWorkflowVersion()
	desiredStateVersion := suite.job.runtime.GetDesiredStateVersion()
	entityVersion := versionutil.GetJobEntityVersion(
		oldConfigVersion,
		desiredStateVersion,
		oldWorkflowVersion,
	)
	prevConfig := &pbjob.JobConfig{
		DefaultConfig: &pbtask.TaskConfig{},
		ChangeLog:     &peloton.ChangeLog{Version: 1},
	}
	jobConfig := prevConfig
	configAddOn := &models.ConfigAddOn{}
	jobSpec := &stateless.JobSpec{
		DefaultSpec: &pbpod.PodSpec{},
		Revision:    &v1alphapeloton.Revision{Version: 1},
	}

	for _, i := range instancesUpdated {
		suite.updateStore.EXPECT().
			AddWorkflowEvent(
				gomock.Any(),
				gomock.Any(),
				i,
				workflowType,
				pbupdate.State_INITIALIZED).Return(nil)
	}

	gomock.InOrder(
		suite.jobStore.EXPECT().
			GetMaxJobConfigVersion(gomock.Any(), suite.jobID.GetValue()).
			Return(prevConfig.GetChangeLog().GetVersion(), nil),

		suite.jobConfigOps.EXPECT().
			Create(
				gomock.Any(),
				suite.jobID,
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				gomock.Any()).
			Do(func(_ context.Context,
				_ *peloton.JobID,
				config *pbjob.JobConfig,
				_ *models.ConfigAddOn,
				spec *stateless.JobSpec,
				_ uint64,
			) {
				suite.Equal(
					config.GetChangeLog().GetVersion(),
					prevConfig.GetChangeLog().GetVersion()+1)
				suite.Equal(
					config.GetChangeLog().GetVersion(),
					spec.GetRevision().GetVersion())
			}).
			Return(nil),
		suite.jobIndexOps.EXPECT().
			Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
			Return(nil),

		suite.jobUpdateEventsOps.EXPECT().
			Create(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				pbupdate.State_INITIALIZED).
			Return(nil),

		suite.updateStore.EXPECT().
			CreateUpdate(gomock.Any(), gomock.Any()).
			Do(func(_ context.Context, updateInfo *models.UpdateModel) {
				suite.Equal(updateInfo.GetJobConfigVersion(),
					prevConfig.GetChangeLog().GetVersion()+1)
				suite.Equal(updateInfo.GetPrevJobConfigVersion(), prevConfig.GetChangeLog().GetVersion())
				suite.Equal(updateInfo.GetState(), pbupdate.State_INITIALIZED)
				suite.Equal(updateInfo.GetJobID(), suite.jobID)
				suite.Equal(updateInfo.GetInstancesAdded(), instancesAdded)
				suite.Equal(updateInfo.GetInstancesUpdated(), instancesUpdated)
				suite.Equal(updateInfo.GetInstancesRemoved(), instnacesRemoved)
				suite.Equal(updateInfo.GetType(), workflowType)
				suite.Equal(updateInfo.GetUpdateConfig(), updateConfig)
				suite.Equal(updateInfo.GetOpaqueData().GetData(), opaque)
			}).
			Return(nil),

		suite.jobRuntimeOps.EXPECT().
			Upsert(gomock.Any(), suite.jobID, gomock.Any()).
			Return(nil),
		suite.jobIndexOps.EXPECT().
			Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
			Return(nil),
	)

	updateID, newEntityVersion, err := suite.job.CreateWorkflow(
		context.Background(),
		workflowType,
		updateConfig,
		entityVersion,
		WithConfig(
			jobConfig,
			prevConfig,
			configAddOn,
			jobSpec,
		),
		WithInstanceToProcess(
			instancesAdded,
			instancesUpdated,
			instnacesRemoved,
		),
		WithOpaqueData(&peloton.OpaqueData{Data: opaque}),
	)

	suite.NotNil(updateID)
	suite.NoError(err)
	suite.NotNil(suite.job.workflows[updateID.GetValue()])
	suite.Equal(newEntityVersion, versionutil.GetJobEntityVersion(
		oldConfigVersion+1,
		desiredStateVersion,
		oldWorkflowVersion+1,
	))
}

// TestJobCreateWorkflowWithStartTasksForStoppedJobSuccess tests the success case of
// creating workflow with StartTasks flag on for a stopped job
func (suite *jobTestSuite) TestJobCreateWorkflowWithStartTasksForStoppedJobSuccess() {
	var instancesAdded []uint32
	var instnacesRemoved []uint32
	instancesUpdated := []uint32{0, 1, 2}
	opaque := "test"

	workflowType := models.WorkflowType_UPDATE
	updateConfig := &pbupdate.UpdateConfig{
		BatchSize:  10,
		StartTasks: true,
	}
	oldConfigVersion := suite.job.runtime.GetConfigurationVersion()
	oldWorkflowVersion := suite.job.runtime.GetWorkflowVersion()
	desiredStateVersion := suite.job.runtime.GetDesiredStateVersion()
	entityVersion := versionutil.GetJobEntityVersion(
		oldConfigVersion,
		desiredStateVersion,
		oldWorkflowVersion,
	)
	prevConfig := &pbjob.JobConfig{
		DefaultConfig: &pbtask.TaskConfig{},
		ChangeLog:     &peloton.ChangeLog{Version: 1},
	}
	jobConfig := prevConfig
	configAddOn := &models.ConfigAddOn{}

	for _, i := range instancesUpdated {
		suite.updateStore.EXPECT().
			AddWorkflowEvent(
				gomock.Any(),
				gomock.Any(),
				i,
				workflowType,
				pbupdate.State_INITIALIZED).Return(nil)
	}

	gomock.InOrder(
		suite.jobStore.EXPECT().
			GetMaxJobConfigVersion(gomock.Any(), suite.jobID.GetValue()).
			Return(prevConfig.GetChangeLog().GetVersion(), nil),

		suite.jobConfigOps.EXPECT().
			Create(
				gomock.Any(),
				suite.jobID,
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				gomock.Any()).
			Do(func(_ context.Context,
				_ *peloton.JobID,
				config *pbjob.JobConfig,
				_ *models.ConfigAddOn,
				_ *stateless.JobSpec,
				_ uint64,
			) {
				suite.Equal(
					config.GetChangeLog().GetVersion(),
					prevConfig.GetChangeLog().GetVersion()+1)
			}).
			Return(nil),
		suite.jobIndexOps.EXPECT().
			Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
			Return(nil),
		suite.jobUpdateEventsOps.EXPECT().
			Create(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				pbupdate.State_INITIALIZED).
			Return(nil),

		suite.updateStore.EXPECT().
			CreateUpdate(gomock.Any(), gomock.Any()).
			Do(func(_ context.Context, updateInfo *models.UpdateModel) {
				suite.Equal(updateInfo.GetJobConfigVersion(),
					prevConfig.GetChangeLog().GetVersion()+1)
				suite.Equal(updateInfo.GetPrevJobConfigVersion(), prevConfig.GetChangeLog().GetVersion())
				suite.Equal(updateInfo.GetState(), pbupdate.State_INITIALIZED)
				suite.Equal(updateInfo.GetJobID(), suite.jobID)
				suite.Equal(updateInfo.GetInstancesAdded(), instancesAdded)
				suite.Equal(updateInfo.GetInstancesUpdated(), instancesUpdated)
				suite.Equal(updateInfo.GetInstancesRemoved(), instnacesRemoved)
				suite.Equal(updateInfo.GetType(), workflowType)
				suite.Equal(updateInfo.GetUpdateConfig(), updateConfig)
				suite.Equal(updateInfo.GetOpaqueData().GetData(), opaque)
			}).
			Return(nil),

		suite.jobRuntimeOps.EXPECT().
			Upsert(gomock.Any(), suite.jobID, gomock.Any()).
			Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
				suite.Equal(runtime.GetGoalState(), pbjob.JobState_RUNNING)
			}).
			Return(nil),
		suite.jobIndexOps.EXPECT().
			Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
			Return(nil),
	)

	updateID, newEntityVersion, err := suite.job.CreateWorkflow(
		context.Background(),
		workflowType,
		updateConfig,
		entityVersion,
		WithConfig(
			jobConfig,
			prevConfig,
			configAddOn,
			nil,
		),
		WithInstanceToProcess(
			instancesAdded,
			instancesUpdated,
			instnacesRemoved,
		),
		WithOpaqueData(&peloton.OpaqueData{Data: opaque}),
	)

	suite.NotNil(updateID)
	suite.NoError(err)
	suite.NotNil(suite.job.workflows[updateID.GetValue()])
	suite.Equal(newEntityVersion, versionutil.GetJobEntityVersion(
		oldConfigVersion+1,
		desiredStateVersion,
		oldWorkflowVersion+1,
	))
}

// TestJobCreateWorkflowUpdateConfigFailure tests the failure case of
// creating workflow due to update config failure
func (suite *jobTestSuite) TestJobCreateWorkflowUpdateConfigFailure() {
	var instancesAdded []uint32
	var instnacesRemoved []uint32
	instancesUpdated := []uint32{0, 1, 2}

	workflowType := models.WorkflowType_START
	updateConfig := &pbupdate.UpdateConfig{
		BatchSize: 10,
	}
	entityVersion := versionutil.GetJobEntityVersion(
		suite.job.runtime.GetConfigurationVersion(),
		suite.job.runtime.GetDesiredStateVersion(),
		suite.job.runtime.GetWorkflowVersion(),
	)
	prevConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{Version: 1},
	}
	jobConfig := prevConfig
	configAddOn := &models.ConfigAddOn{}

	gomock.InOrder(
		suite.jobStore.EXPECT().
			GetMaxJobConfigVersion(gomock.Any(), suite.jobID.GetValue()).
			Return(prevConfig.GetChangeLog().GetVersion(), nil),

		suite.jobConfigOps.EXPECT().
			Create(
				gomock.Any(),
				suite.jobID,
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				gomock.Any()).
			Do(func(_ context.Context,
				_ *peloton.JobID,
				config *pbjob.JobConfig,
				_ *models.ConfigAddOn,
				_ *stateless.JobSpec,
				_ uint64,
			) {
				suite.Equal(
					config.GetChangeLog().GetVersion(),
					prevConfig.GetChangeLog().GetVersion()+1)
			}).
			Return(yarpcerrors.InternalErrorf("test error")),
	)

	updateID, newEntityVersion, err := suite.job.CreateWorkflow(
		context.Background(),
		workflowType,
		updateConfig,
		entityVersion,
		WithConfig(
			jobConfig,
			prevConfig,
			configAddOn,
			nil,
		),
		WithInstanceToProcess(
			instancesAdded,
			instancesUpdated,
			instnacesRemoved,
		),
	)

	suite.Nil(updateID)
	suite.Error(err)
	suite.Nil(newEntityVersion)
	suite.Nil(suite.job.workflows[updateID.GetValue()])
}

// TestJobCreateWorkflowWorkflowCreationFailure tests the failure case of
// creating workflow due to failure of creating update workflow in db
func (suite *jobTestSuite) TestJobCreateWorkflowWorkflowCreationFailure() {
	var instancesAdded []uint32
	var instnacesRemoved []uint32
	instancesUpdated := []uint32{0, 1, 2}

	workflowType := models.WorkflowType_START
	updateConfig := &pbupdate.UpdateConfig{
		BatchSize: 10,
	}
	entityVersion := versionutil.GetJobEntityVersion(
		suite.job.runtime.GetConfigurationVersion(),
		suite.job.runtime.GetDesiredStateVersion(),
		suite.job.runtime.GetWorkflowVersion(),
	)
	prevConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{Version: 1},
	}
	jobConfig := prevConfig
	configAddOn := &models.ConfigAddOn{}

	for _, i := range instancesUpdated {
		suite.updateStore.EXPECT().
			AddWorkflowEvent(
				gomock.Any(),
				gomock.Any(),
				uint32(i),
				models.WorkflowType_START,
				pbupdate.State_INITIALIZED).
			Return(nil)
	}

	gomock.InOrder(
		suite.jobStore.EXPECT().
			GetMaxJobConfigVersion(gomock.Any(), suite.jobID.GetValue()).
			Return(prevConfig.GetChangeLog().GetVersion(), nil),

		suite.jobConfigOps.EXPECT().
			Create(
				gomock.Any(),
				suite.jobID,
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				gomock.Any()).
			Do(func(_ context.Context,
				_ *peloton.JobID,
				config *pbjob.JobConfig,
				_ *models.ConfigAddOn,
				_ *stateless.JobSpec,
				_ uint64,
			) {
				suite.Equal(
					config.GetChangeLog().GetVersion(),
					prevConfig.GetChangeLog().GetVersion()+1)
			}).
			Return(nil),
		suite.jobIndexOps.EXPECT().
			Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
			Return(nil),
		suite.jobUpdateEventsOps.EXPECT().
			Create(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				pbupdate.State_INITIALIZED).
			Return(nil),
		suite.updateStore.EXPECT().
			CreateUpdate(gomock.Any(), gomock.Any()).
			Do(func(_ context.Context, updateInfo *models.UpdateModel) {
				suite.Equal(updateInfo.GetJobConfigVersion(),
					prevConfig.GetChangeLog().GetVersion()+1)
				suite.Equal(updateInfo.GetPrevJobConfigVersion(), prevConfig.GetChangeLog().GetVersion())
				suite.Equal(updateInfo.GetState(), pbupdate.State_INITIALIZED)
				suite.Equal(updateInfo.GetJobID(), suite.jobID)
				suite.Equal(updateInfo.GetInstancesAdded(), instancesAdded)
				suite.Equal(updateInfo.GetInstancesUpdated(), instancesUpdated)
				suite.Equal(updateInfo.GetInstancesRemoved(), instnacesRemoved)
				suite.Equal(updateInfo.GetType(), workflowType)
				suite.Equal(updateInfo.GetUpdateConfig(), updateConfig)
			}).
			Return(yarpcerrors.InternalErrorf("test error")),
	)

	updateID, newEntityVersion, err := suite.job.CreateWorkflow(
		context.Background(),
		workflowType,
		updateConfig,
		entityVersion,
		WithConfig(
			jobConfig,
			prevConfig,
			configAddOn,
			nil,
		),
		WithInstanceToProcess(
			instancesAdded,
			instancesUpdated,
			instnacesRemoved,
		),
	)

	suite.Nil(updateID)
	suite.Error(err)
	suite.Nil(newEntityVersion)
	suite.Nil(suite.job.workflows[updateID.GetValue()])
}

// TestJobCreateWorkflowUpdateRuntimeFailure tests the failure case of
// creating workflow due to update runtime failure
func (suite *jobTestSuite) TestJobCreateWorkflowUpdateRuntimeFailure() {
	var instancesAdded []uint32
	var instnacesRemoved []uint32
	instancesUpdated := []uint32{0, 1, 2}

	workflowType := models.WorkflowType_START
	updateConfig := &pbupdate.UpdateConfig{
		BatchSize: 10,
	}
	entityVersion := versionutil.GetJobEntityVersion(
		suite.job.runtime.GetConfigurationVersion(),
		suite.job.runtime.GetDesiredStateVersion(),
		suite.job.runtime.GetWorkflowVersion(),
	)
	prevConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{Version: 1},
	}
	jobConfig := prevConfig
	configAddOn := &models.ConfigAddOn{}

	for _, i := range instancesUpdated {
		suite.updateStore.EXPECT().
			AddWorkflowEvent(
				gomock.Any(),
				gomock.Any(),
				i,
				workflowType,
				pbupdate.State_INITIALIZED).Return(nil)
	}

	gomock.InOrder(
		suite.jobStore.EXPECT().
			GetMaxJobConfigVersion(gomock.Any(), suite.jobID.GetValue()).
			Return(prevConfig.GetChangeLog().GetVersion(), nil),

		suite.jobConfigOps.EXPECT().
			Create(
				gomock.Any(),
				suite.jobID,
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				gomock.Any()).
			Do(func(_ context.Context,
				_ *peloton.JobID,
				config *pbjob.JobConfig,
				_ *models.ConfigAddOn,
				_ *stateless.JobSpec,
				_ uint64,
			) {
				suite.Equal(
					config.GetChangeLog().GetVersion(),
					prevConfig.GetChangeLog().GetVersion()+1)
			}).
			Return(nil),
		suite.jobIndexOps.EXPECT().
			Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
			Return(nil),
		suite.jobUpdateEventsOps.EXPECT().
			Create(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				pbupdate.State_INITIALIZED).
			Return(nil),
		suite.updateStore.EXPECT().
			CreateUpdate(gomock.Any(), gomock.Any()).
			Do(func(_ context.Context, updateInfo *models.UpdateModel) {
				suite.Equal(updateInfo.GetJobConfigVersion(),
					prevConfig.GetChangeLog().GetVersion()+1)
				suite.Equal(updateInfo.GetPrevJobConfigVersion(), prevConfig.GetChangeLog().GetVersion())
				suite.Equal(updateInfo.GetState(), pbupdate.State_INITIALIZED)
				suite.Equal(updateInfo.GetJobID(), suite.jobID)
				suite.Equal(updateInfo.GetInstancesAdded(), instancesAdded)
				suite.Equal(updateInfo.GetInstancesUpdated(), instancesUpdated)
				suite.Equal(updateInfo.GetInstancesRemoved(), instnacesRemoved)
				suite.Equal(updateInfo.GetType(), workflowType)
				suite.Equal(updateInfo.GetUpdateConfig(), updateConfig)
			}).
			Return(nil),

		suite.jobRuntimeOps.EXPECT().
			Upsert(gomock.Any(), suite.jobID, gomock.Any()).
			Return(yarpcerrors.InternalErrorf("test error")),
	)

	updateID, newEntityVersion, err := suite.job.CreateWorkflow(
		context.Background(),
		workflowType,
		updateConfig,
		entityVersion,
		WithConfig(
			jobConfig,
			prevConfig,
			configAddOn,
			nil,
		),
		WithInstanceToProcess(
			instancesAdded,
			instancesUpdated,
			instnacesRemoved,
		),
	)

	// still return the update id, since the update may actually persisted in
	// job runtime.
	// do not add update in job.workflows because if the update is not persisted
	// in job runtime, there is no way to clean it up.
	suite.NotNil(updateID)
	suite.Error(err)
	suite.Nil(newEntityVersion)
	suite.Nil(suite.job.workflows[updateID.GetValue()])
}

// TestJobCreateWorkflowOnDeletedJobError tests the failure case of
// creating workflow for a deleted job
func (suite *jobTestSuite) TestJobCreateWorkflowOnDeletedJobError() {
	var instancesAdded []uint32
	var instnacesRemoved []uint32
	instancesUpdated := []uint32{0, 1, 2}

	suite.job.runtime.GoalState = pbjob.JobState_DELETED
	workflowType := models.WorkflowType_UPDATE
	updateConfig := &pbupdate.UpdateConfig{
		BatchSize: 10,
	}
	entityVersion := versionutil.GetJobEntityVersion(
		suite.job.runtime.GetConfigurationVersion(),
		suite.job.runtime.GetDesiredStateVersion(),
		suite.job.runtime.GetWorkflowVersion(),
	)
	prevConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{Version: 1},
	}
	jobConfig := prevConfig
	configAddOn := &models.ConfigAddOn{}

	for _, i := range instancesUpdated {
		suite.updateStore.EXPECT().
			AddWorkflowEvent(
				gomock.Any(),
				gomock.Any(),
				i,
				workflowType,
				pbupdate.State_INITIALIZED).Return(nil)
	}

	gomock.InOrder(
		suite.jobStore.EXPECT().
			GetMaxJobConfigVersion(gomock.Any(), suite.jobID.GetValue()).
			Return(prevConfig.GetChangeLog().GetVersion(), nil),

		suite.jobConfigOps.EXPECT().
			Create(
				gomock.Any(),
				suite.jobID,
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				gomock.Any()).
			Do(func(_ context.Context,
				_ *peloton.JobID,
				config *pbjob.JobConfig,
				_ *models.ConfigAddOn,
				_ *stateless.JobSpec,
				_ uint64,
			) {
				suite.Equal(
					config.GetChangeLog().GetVersion(),
					prevConfig.GetChangeLog().GetVersion()+1)
			}).
			Return(nil),
		suite.jobIndexOps.EXPECT().
			Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
			Return(nil),
		suite.jobUpdateEventsOps.EXPECT().
			Create(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				pbupdate.State_INITIALIZED).
			Return(nil),
		suite.updateStore.EXPECT().
			CreateUpdate(gomock.Any(), gomock.Any()).
			Do(func(_ context.Context, updateInfo *models.UpdateModel) {
				suite.Equal(updateInfo.GetJobConfigVersion(),
					prevConfig.GetChangeLog().GetVersion()+1)
				suite.Equal(updateInfo.GetPrevJobConfigVersion(), prevConfig.GetChangeLog().GetVersion())
				suite.Equal(updateInfo.GetState(), pbupdate.State_INITIALIZED)
				suite.Equal(updateInfo.GetJobID(), suite.jobID)
				suite.Equal(updateInfo.GetInstancesAdded(), instancesAdded)
				suite.Equal(updateInfo.GetInstancesUpdated(), instancesUpdated)
				suite.Equal(updateInfo.GetInstancesRemoved(), instnacesRemoved)
				suite.Equal(updateInfo.GetType(), workflowType)
				suite.Equal(updateInfo.GetUpdateConfig(), updateConfig)
			}).
			Return(nil),
	)

	updateID, newEntityVersion, err := suite.job.CreateWorkflow(
		context.Background(),
		workflowType,
		updateConfig,
		entityVersion,
		WithConfig(
			jobConfig,
			prevConfig,
			configAddOn,
			nil,
		),
		WithInstanceToProcess(
			instancesAdded,
			instancesUpdated,
			instnacesRemoved,
		),
	)

	// still return the update id, since the update may actually persisted in
	// job runtime.
	// do not add update in job.workflows because if the update is not persisted
	// in job runtime, there is no way to clean it up.
	suite.NotNil(updateID)
	suite.Error(err)
	suite.Nil(newEntityVersion)
	suite.Nil(suite.job.workflows[updateID.GetValue()])
}

// TestJobCreateWorkflowOnTerminatingJobError tests the failure case of
// creating workflow for a job actively being terminated
func (suite *jobTestSuite) TestJobCreateWorkflowOnTerminatingJobError() {
	suite.job.runtime.State = pbjob.JobState_KILLING
	suite.job.runtime.GoalState = pbjob.JobState_KILLED

	updateConfig := &pbupdate.UpdateConfig{
		BatchSize:  10,
		StartTasks: true,
	}
	entityVersion := versionutil.GetJobEntityVersion(
		suite.job.runtime.GetConfigurationVersion(),
		suite.job.runtime.GetDesiredStateVersion(),
		suite.job.runtime.GetWorkflowVersion(),
	)

	updateID, newEntityVersion, err := suite.job.CreateWorkflow(
		context.Background(),
		models.WorkflowType_UPDATE,
		updateConfig,
		entityVersion,
	)

	suite.Nil(updateID)
	suite.Error(err)
	suite.Nil(newEntityVersion)
	suite.Nil(suite.job.workflows[updateID.GetValue()])
}

// TestResumeWorkflowSuccess tests the success case
// of resuming a workflow
func (suite *jobTestSuite) TestResumeWorkflowSuccess() {
	oldConfigVersion := suite.job.runtime.GetConfigurationVersion()
	oldWorkflowVersion := suite.job.runtime.GetWorkflowVersion()
	opaque := "test"
	desiredStateVersion := suite.job.runtime.GetDesiredStateVersion()
	entityVersion := versionutil.GetJobEntityVersion(
		oldConfigVersion,
		desiredStateVersion,
		oldWorkflowVersion,
	)

	updateID := &peloton.UpdateID{Value: testUpdateID}
	suite.job.runtime.UpdateID = updateID
	suite.job.workflows[updateID.GetValue()] = &update{
		id:         updateID,
		jobFactory: suite.job.jobFactory,
		prevState:  pbupdate.State_ROLLING_FORWARD,
		state:      pbupdate.State_PAUSED,
	}

	gomock.InOrder(
		suite.jobRuntimeOps.EXPECT().
			Upsert(gomock.Any(), suite.job.ID(), gomock.Any()).
			Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
				suite.Equal(runtime.GetConfigurationVersion(), oldConfigVersion)
				suite.Equal(runtime.GetWorkflowVersion(), oldWorkflowVersion+1)
			}).Return(nil),
		suite.jobIndexOps.EXPECT().
			Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
			Return(nil),
		suite.jobUpdateEventsOps.EXPECT().
			Create(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				pbupdate.State_ROLLING_FORWARD).
			Return(nil),
		suite.updateStore.EXPECT().
			WriteUpdateProgress(gomock.Any(), gomock.Any()).
			Do(func(ctx context.Context, updateInfo *models.UpdateModel) {
				suite.Equal(updateInfo.GetState(), pbupdate.State_ROLLING_FORWARD)
				suite.Equal(updateInfo.GetOpaqueData().GetData(), opaque)
			}).
			Return(nil),
	)

	updateIDResult, newEntityVersion, err := suite.job.ResumeWorkflow(
		context.Background(),
		entityVersion,
		WithOpaqueData(&peloton.OpaqueData{Data: opaque}),
	)

	suite.NoError(err)
	suite.Equal(
		versionutil.GetJobEntityVersion(oldConfigVersion, desiredStateVersion, oldWorkflowVersion+1),
		newEntityVersion,
	)
	suite.Equal(updateIDResult, updateID)
}

// TestResumeWorkflowNilEntityVersionFailure tests the failure case
// of resuming a workflow due to nil entity version provided
func (suite *jobTestSuite) TestResumeWorkflowNilEntityVersionFailure() {
	updateID := &peloton.UpdateID{Value: testUpdateID}
	suite.job.runtime.UpdateID = updateID
	suite.job.workflows[updateID.GetValue()] = &update{
		id:         updateID,
		jobFactory: suite.job.jobFactory,
		prevState:  pbupdate.State_ROLLING_FORWARD,
		state:      pbupdate.State_PAUSED,
	}

	updateID, newEntityVersion, err := suite.job.ResumeWorkflow(context.Background(), nil)
	suite.Error(err)
	suite.Nil(newEntityVersion)
	suite.Nil(updateID)
}

// TestResumeWorkflowWrongEntityVersionFailure tests the failure case
// of resuming a workflow due to wrong entity version provided
func (suite *jobTestSuite) TestResumeWorkflowWrongEntityVersionFailure() {
	updateID := &peloton.UpdateID{Value: testUpdateID}
	suite.job.runtime.UpdateID = updateID
	suite.job.workflows[updateID.GetValue()] = &update{
		id:         updateID,
		jobFactory: suite.job.jobFactory,
		state:      pbupdate.State_ROLLING_FORWARD,
	}

	updateID, newEntityVersion, err := suite.job.ResumeWorkflow(
		context.Background(),
		versionutil.GetJobEntityVersion(
			suite.job.runtime.GetConfigurationVersion()+1,
			suite.job.runtime.GetDesiredStateVersion(),
			suite.job.runtime.GetWorkflowVersion(),
		),
	)
	suite.Error(err)
	suite.Nil(newEntityVersion)
	suite.Nil(updateID)
}

// TestResumeWorkflowNotExistInCacheSuccess tests the success case
// of resuming a workflow when the workflow is not in cache
func (suite *jobTestSuite) TestResumeWorkflowNotExistInCacheSuccess() {
	oldConfigVersion := suite.job.runtime.GetConfigurationVersion()
	oldWorkflowVersion := suite.job.runtime.GetWorkflowVersion()
	desiredStateVersion := suite.job.runtime.GetDesiredStateVersion()
	entityVersion := versionutil.GetJobEntityVersion(oldConfigVersion, desiredStateVersion, oldWorkflowVersion)

	updateID := &peloton.UpdateID{Value: testUpdateID}
	suite.job.runtime.UpdateID = updateID

	gomock.InOrder(
		suite.jobRuntimeOps.EXPECT().
			Upsert(gomock.Any(), suite.job.ID(), gomock.Any()).
			Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
				suite.Equal(runtime.GetConfigurationVersion(), oldConfigVersion)
				suite.Equal(runtime.GetWorkflowVersion(), oldWorkflowVersion+1)
			}).Return(nil),
		suite.jobIndexOps.EXPECT().
			Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
			Return(nil),
		suite.updateStore.EXPECT().
			GetUpdate(gomock.Any(), updateID).
			Return(&models.UpdateModel{
				PrevState: pbupdate.State_ROLLING_FORWARD,
				State:     pbupdate.State_PAUSED,
			}, nil),
		suite.jobUpdateEventsOps.EXPECT().
			Create(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				pbupdate.State_ROLLING_FORWARD).
			Return(nil),
		suite.updateStore.EXPECT().
			WriteUpdateProgress(gomock.Any(), gomock.Any()).
			Do(func(ctx context.Context, updateInfo *models.UpdateModel) {
				suite.Equal(updateInfo.GetState(), pbupdate.State_ROLLING_FORWARD)
			}).
			Return(nil),
	)

	updateIDResult, newEntityVersion, err := suite.job.ResumeWorkflow(context.Background(), entityVersion)
	suite.NoError(err)
	suite.Equal(
		versionutil.GetJobEntityVersion(oldConfigVersion, desiredStateVersion, oldWorkflowVersion+1),
		newEntityVersion,
	)
	suite.Equal(updateIDResult, updateID)
}

// TestResumeWorkflowNoUpdate the case of update pause
// when there is no update
func (suite *jobTestSuite) TestResumeWorkflowNoUpdate() {
	entityVersion := versionutil.GetJobEntityVersion(
		suite.job.runtime.GetConfigurationVersion(),
		suite.job.runtime.GetDesiredStateVersion(),
		suite.job.runtime.GetWorkflowVersion(),
	)

	updateID, newEntityVersion, err := suite.job.ResumeWorkflow(context.Background(), entityVersion)
	suite.Error(err)
	suite.Nil(newEntityVersion)
	suite.Nil(updateID)
}

// TestPauseWorkflowSuccess tests the success case
// of pausing a workflow
func (suite *jobTestSuite) TestPauseWorkflowSuccess() {
	oldConfigVersion := suite.job.runtime.GetConfigurationVersion()
	oldWorkflowVersion := suite.job.runtime.GetWorkflowVersion()
	opaque := "test"
	desiredStateVersion := suite.job.runtime.GetDesiredStateVersion()
	entityVersion := versionutil.GetJobEntityVersion(oldConfigVersion, desiredStateVersion, oldWorkflowVersion)

	updateID := &peloton.UpdateID{Value: testUpdateID}
	suite.job.runtime.UpdateID = updateID
	suite.job.workflows[updateID.GetValue()] = &update{
		id:         updateID,
		jobFactory: suite.job.jobFactory,
		state:      pbupdate.State_ROLLING_FORWARD,
	}

	gomock.InOrder(
		suite.jobRuntimeOps.EXPECT().
			Upsert(gomock.Any(), suite.job.ID(), gomock.Any()).
			Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
				suite.Equal(runtime.GetConfigurationVersion(), oldConfigVersion)
				suite.Equal(runtime.GetWorkflowVersion(), oldWorkflowVersion+1)
			}).Return(nil),
		suite.jobIndexOps.EXPECT().
			Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
			Return(nil),
		suite.jobUpdateEventsOps.EXPECT().
			Create(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				pbupdate.State_PAUSED).
			Return(nil),
		suite.updateStore.EXPECT().
			WriteUpdateProgress(gomock.Any(), gomock.Any()).
			Do(func(ctx context.Context, updateInfo *models.UpdateModel) {
				suite.Equal(updateInfo.GetState(), pbupdate.State_PAUSED)
				suite.Equal(updateInfo.GetOpaqueData().GetData(), opaque)
			}).
			Return(nil),
	)

	updateIDResult, newEntityVersion, err := suite.job.PauseWorkflow(
		context.Background(),
		entityVersion,
		WithOpaqueData(&peloton.OpaqueData{Data: opaque}),
	)
	suite.NoError(err)
	suite.Equal(
		versionutil.GetJobEntityVersion(oldConfigVersion, desiredStateVersion, oldWorkflowVersion+1),
		newEntityVersion,
	)
	suite.Equal(updateIDResult, updateID)
}

// TestPauseWorkflowNilEntityVersionFailure tests the failure case
// of pausing a workflow due to nil entity version provided
func (suite *jobTestSuite) TestPauseWorkflowNilEntityVersionFailure() {
	updateID := &peloton.UpdateID{Value: testUpdateID}
	suite.job.runtime.UpdateID = updateID
	suite.job.workflows[updateID.GetValue()] = &update{
		id:         updateID,
		jobFactory: suite.job.jobFactory,
		state:      pbupdate.State_ROLLING_FORWARD,
	}

	updateID, newEntityVersion, err := suite.job.PauseWorkflow(context.Background(), nil)
	suite.Error(err)
	suite.Nil(newEntityVersion)
	suite.Nil(updateID)
}

// TestPauseWorkflowWrongEntityVersionFailure tests the failure case
// of pausing a workflow due to wrong entity version provided
func (suite *jobTestSuite) TestPauseWorkflowWrongEntityVersionFailure() {
	updateID := &peloton.UpdateID{Value: testUpdateID}
	suite.job.runtime.UpdateID = updateID
	suite.job.workflows[updateID.GetValue()] = &update{
		id:         updateID,
		jobFactory: suite.job.jobFactory,
		state:      pbupdate.State_ROLLING_FORWARD,
	}

	updateID, newEntityVersion, err := suite.job.PauseWorkflow(
		context.Background(),
		versionutil.GetJobEntityVersion(
			suite.job.runtime.GetConfigurationVersion()+1,
			suite.job.runtime.GetDesiredStateVersion(),
			suite.job.runtime.GetWorkflowVersion(),
		),
	)
	suite.Error(err)
	suite.Nil(newEntityVersion)
	suite.Nil(updateID)
}

// TestPauseWorkflowNotExistInCacheSuccess tests the success case
// of pausing a workflow when the workflow is not in cache
func (suite *jobTestSuite) TestPauseWorkflowNotExistInCacheSuccess() {
	oldConfigVersion := suite.job.runtime.GetConfigurationVersion()
	oldWorkflowVersion := suite.job.runtime.GetWorkflowVersion()
	desiredStateVersion := suite.job.runtime.GetDesiredStateVersion()
	entityVersion := versionutil.GetJobEntityVersion(oldConfigVersion, desiredStateVersion, oldWorkflowVersion)

	updateID := &peloton.UpdateID{Value: testUpdateID}
	suite.job.runtime.UpdateID = updateID

	gomock.InOrder(
		suite.jobRuntimeOps.EXPECT().
			Upsert(gomock.Any(), suite.job.ID(), gomock.Any()).
			Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
				suite.Equal(runtime.GetConfigurationVersion(), oldConfigVersion)
				suite.Equal(runtime.GetWorkflowVersion(), oldWorkflowVersion+1)
			}).Return(nil),
		suite.jobIndexOps.EXPECT().
			Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
			Return(nil),
		suite.updateStore.EXPECT().
			GetUpdate(gomock.Any(), updateID).
			Return(&models.UpdateModel{
				State: pbupdate.State_ROLLING_FORWARD,
			}, nil),
		suite.jobUpdateEventsOps.EXPECT().
			Create(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				pbupdate.State_PAUSED).
			Return(nil),
		suite.updateStore.EXPECT().
			WriteUpdateProgress(gomock.Any(), gomock.Any()).
			Do(func(ctx context.Context, updateInfo *models.UpdateModel) {
				suite.Equal(updateInfo.GetState(), pbupdate.State_PAUSED)
			}).
			Return(nil),
	)

	updateIDResult, newEntityVersion, err := suite.job.PauseWorkflow(context.Background(), entityVersion)
	suite.NoError(err)
	suite.Equal(
		versionutil.GetJobEntityVersion(oldConfigVersion, desiredStateVersion, oldWorkflowVersion+1),
		newEntityVersion,
	)
	suite.Equal(updateIDResult, updateID)
}

// TestPauseWorkflowNoUpdate tests the case of update pause
// when there is no update
func (suite *jobTestSuite) TestPauseWorkflowNoUpdate() {
	entityVersion := versionutil.GetJobEntityVersion(
		suite.job.runtime.GetConfigurationVersion(),
		suite.job.runtime.GetDesiredStateVersion(),
		suite.job.runtime.GetWorkflowVersion(),
	)

	updateID, newEntityVersion, err := suite.job.PauseWorkflow(context.Background(), entityVersion)
	suite.Error(err)
	suite.Nil(newEntityVersion)
	suite.Nil(updateID)
}

// TestAbortWorkflowSuccess tests the success case
// of aborting a workflow
func (suite *jobTestSuite) TestAbortWorkflowSuccess() {
	oldConfigVersion := suite.job.runtime.GetConfigurationVersion()
	oldWorkflowVersion := suite.job.runtime.GetWorkflowVersion()
	opaque := "test"
	desiredStateVersion := suite.job.runtime.GetDesiredStateVersion()
	entityVersion := versionutil.GetJobEntityVersion(oldConfigVersion, desiredStateVersion, oldWorkflowVersion)

	updateID := &peloton.UpdateID{Value: testUpdateID}
	suite.job.runtime.UpdateID = updateID
	suite.job.workflows[updateID.GetValue()] = &update{
		id:         updateID,
		jobFactory: suite.job.jobFactory,
		state:      pbupdate.State_ROLLING_FORWARD,
	}

	gomock.InOrder(
		suite.jobRuntimeOps.EXPECT().
			Upsert(gomock.Any(), suite.job.ID(), gomock.Any()).
			Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
				suite.Equal(runtime.GetConfigurationVersion(), oldConfigVersion)
				suite.Equal(runtime.GetWorkflowVersion(), oldWorkflowVersion+1)
			}).Return(nil),
		suite.jobIndexOps.EXPECT().
			Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
			Return(nil),
		suite.jobUpdateEventsOps.EXPECT().
			Create(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				pbupdate.State_ABORTED).
			Return(nil),
		suite.updateStore.EXPECT().
			WriteUpdateProgress(gomock.Any(), gomock.Any()).
			Do(func(ctx context.Context, updateInfo *models.UpdateModel) {
				suite.Equal(updateInfo.GetState(), pbupdate.State_ABORTED)
				suite.Equal(updateInfo.GetOpaqueData().GetData(), opaque)
				suite.NotEmpty(updateInfo.GetCompletionTime())
			}).
			Return(nil),
	)

	updateIDResult, newEntityVersion, err := suite.job.AbortWorkflow(
		context.Background(),
		entityVersion,
		WithOpaqueData(&peloton.OpaqueData{Data: opaque}),
	)
	suite.NoError(err)
	suite.Equal(
		versionutil.GetJobEntityVersion(oldConfigVersion, desiredStateVersion, oldWorkflowVersion+1),
		newEntityVersion,
	)
	suite.Equal(updateID, updateIDResult)
}

// TestAbortWorkflowWrongEntityVersionFailure tests the failure case
// of aborting a workflow due to wrong entity version provided
func (suite *jobTestSuite) TestAbortWorkflowWrongEntityVersionFailure() {
	updateID := &peloton.UpdateID{Value: testUpdateID}
	suite.job.runtime.UpdateID = updateID
	suite.job.workflows[updateID.GetValue()] = &update{
		id:         updateID,
		jobFactory: suite.job.jobFactory,
		state:      pbupdate.State_ROLLING_FORWARD,
	}

	updateID, newEntityVersion, err := suite.job.AbortWorkflow(
		context.Background(),
		versionutil.GetJobEntityVersion(
			suite.job.runtime.GetConfigurationVersion()+1,
			suite.job.runtime.GetDesiredStateVersion(),
			suite.job.runtime.GetWorkflowVersion(),
		),
	)
	suite.Error(err)
	suite.Nil(newEntityVersion)
	suite.Nil(updateID)
}

// TestAbortWorkflowNilEntityVersionFailure tests the failure case
// of aborting a workflow due to nil entity version provided
func (suite *jobTestSuite) TestAbortWorkflowNilEntityVersionFailure() {
	updateID := &peloton.UpdateID{Value: testUpdateID}
	suite.job.runtime.UpdateID = updateID
	suite.job.workflows[updateID.GetValue()] = &update{
		id:         updateID,
		jobFactory: suite.job.jobFactory,
		prevState:  pbupdate.State_ROLLING_FORWARD,
		state:      pbupdate.State_PAUSED,
	}

	updateID, newEntityVersion, err := suite.job.AbortWorkflow(context.Background(), nil)
	suite.Error(err)
	suite.Nil(newEntityVersion)
	suite.Nil(updateID)
}

// TestAbortWorkflowNotExistInCacheSuccess tests the success case
// of aborting a workflow when the workflow is not in cache
func (suite *jobTestSuite) TestAbortWorkflowNotExistInCacheSuccess() {
	oldConfigVersion := suite.job.runtime.GetConfigurationVersion()
	oldWorkflowVersion := suite.job.runtime.GetWorkflowVersion()
	desiredStateVersion := suite.job.runtime.GetDesiredStateVersion()
	entityVersion := versionutil.GetJobEntityVersion(
		oldConfigVersion,
		desiredStateVersion,
		oldWorkflowVersion)

	updateID := &peloton.UpdateID{Value: testUpdateID}
	suite.job.runtime.UpdateID = updateID

	gomock.InOrder(
		suite.jobRuntimeOps.EXPECT().
			Upsert(gomock.Any(), suite.job.ID(), gomock.Any()).
			Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
				suite.Equal(runtime.GetConfigurationVersion(), oldConfigVersion)
				suite.Equal(runtime.GetWorkflowVersion(), oldWorkflowVersion+1)
			}).Return(nil),
		suite.jobIndexOps.EXPECT().
			Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
			Return(nil),

		suite.updateStore.EXPECT().
			GetUpdate(gomock.Any(), updateID).
			Return(&models.UpdateModel{
				State: pbupdate.State_ROLLING_FORWARD,
			}, nil),

		suite.jobUpdateEventsOps.EXPECT().
			Create(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				pbupdate.State_ABORTED).
			Return(nil),

		suite.updateStore.EXPECT().
			WriteUpdateProgress(gomock.Any(), gomock.Any()).
			Do(func(ctx context.Context, updateInfo *models.UpdateModel) {
				suite.Equal(updateInfo.GetState(), pbupdate.State_ABORTED)
				suite.NotEmpty(updateInfo.GetCompletionTime())
			}).
			Return(nil),
	)

	updateIDResult, newEntityVersion, err := suite.job.AbortWorkflow(context.Background(), entityVersion)
	suite.NoError(err)
	suite.Equal(
		versionutil.GetJobEntityVersion(
			oldConfigVersion,
			desiredStateVersion,
			oldWorkflowVersion+1),
		newEntityVersion,
	)
	suite.Equal(updateIDResult, updateID)
}

// TestAbortWorkflowNoUpdate tests the case of update pause
// when there is no update
func (suite *jobTestSuite) TestAbortWorkflowNoUpdate() {
	entityVersion := versionutil.GetJobEntityVersion(
		suite.job.runtime.GetConfigurationVersion(),
		suite.job.runtime.GetDesiredStateVersion(),
		suite.job.runtime.GetWorkflowVersion(),
	)

	updateID, newEntityVersion, err := suite.job.AbortWorkflow(context.Background(), entityVersion)
	suite.Nil(newEntityVersion)
	suite.Error(err)
	suite.Nil(updateID)
}

// TestAddExistingWorkflow tests adding an already exist workflow
func (suite *jobTestSuite) TestAddExistingWorkflow() {
	cachedUpdate := &update{
		id:         &peloton.UpdateID{Value: testUpdateID},
		jobFactory: suite.job.jobFactory,
		state:      pbupdate.State_ROLLING_FORWARD,
	}
	suite.job.workflows[testUpdateID] = cachedUpdate

	suite.Equal(cachedUpdate,
		suite.job.AddWorkflow(&peloton.UpdateID{Value: testUpdateID}))

}

// TestAddExistingWorkflow tests adding a workflow
// that does not exist
func (suite *jobTestSuite) TestAddNonExistingWorkflow() {
	cachedUpdate := suite.job.AddWorkflow(&peloton.UpdateID{Value: testUpdateID})
	suite.Equal(cachedUpdate.GetState().State, pbupdate.State_INVALID)
	suite.NotNil(suite.job.workflows[testUpdateID])
}

// TestGetExistingWorkflow tests getting an already exist workflow
func (suite *jobTestSuite) TestGetExistingWorkflow() {
	cachedUpdate := &update{
		id:         &peloton.UpdateID{Value: testUpdateID},
		jobFactory: suite.job.jobFactory,
		state:      pbupdate.State_ROLLING_FORWARD,
	}
	suite.job.workflows[testUpdateID] = cachedUpdate

	suite.Equal(cachedUpdate,
		suite.job.GetWorkflow(&peloton.UpdateID{Value: testUpdateID}))

}

// TestGetNonExistingWorkflow tests getting a workflow
// that does not exist
func (suite *jobTestSuite) TestGetNonExistingWorkflow() {
	suite.Nil(suite.job.GetWorkflow(&peloton.UpdateID{Value: testUpdateID}))
	suite.Nil(suite.job.workflows[testUpdateID])
}

// TestDeleteWorkflow tests clearing workflow
func (suite *jobTestSuite) TestClearWorkflow() {
	cachedUpdate := &update{
		id:         &peloton.UpdateID{Value: testUpdateID},
		jobFactory: suite.job.jobFactory,
		state:      pbupdate.State_ROLLING_FORWARD,
	}
	suite.job.workflows[testUpdateID] = cachedUpdate

	suite.job.ClearWorkflow(&peloton.UpdateID{Value: testUpdateID})
	suite.Nil(suite.job.workflows[testUpdateID])
}

// TestRollbackWorkflowSuccess tests the success case of
// rollback a workflow
func (suite *jobTestSuite) TestRollbackWorkflowSuccess() {
	jobPrevVersion := uint64(1)
	jobPrevConfig := &pbjob.JobConfig{
		DefaultConfig: &pbtask.TaskConfig{
			Revocable: true,
		},
		ChangeLog: &peloton.ChangeLog{
			Version: jobPrevVersion,
		},
		InstanceCount: 5,
	}

	jobVersion := uint64(2)
	jobConfig := &pbjob.JobConfig{
		DefaultConfig: &pbtask.TaskConfig{
			Revocable: false,
		},
		ChangeLog: &peloton.ChangeLog{
			Version: jobVersion,
		},
		InstanceCount: 10,
	}

	cachedWorkflow := &update{}
	cachedWorkflow.state = pbupdate.State_ROLLING_FORWARD
	cachedWorkflow.jobID = suite.jobID
	cachedWorkflow.jobVersion = jobVersion
	cachedWorkflow.jobPrevVersion = jobPrevVersion
	cachedWorkflow.jobFactory = suite.job.jobFactory
	cachedWorkflow.workflowType = models.WorkflowType_UPDATE

	suite.job.runtime.UpdateID = &peloton.UpdateID{Value: testUpdateID}
	suite.job.workflows[testUpdateID] = cachedWorkflow

	taskRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < jobConfig.GetInstanceCount(); i++ {
		runtime := &pbtask.RuntimeInfo{
			State:                pbtask.TaskState_RUNNING,
			ConfigVersion:        jobPrevVersion,
			DesiredConfigVersion: jobPrevVersion,
		}
		taskRuntimes[i] = runtime
	}

	suite.jobConfigOps.EXPECT().GetResult(
		gomock.Any(), suite.jobID, jobPrevVersion).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig: jobPrevConfig,
		}, nil)

	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID.GetValue()).
		Return(jobVersion, nil)

	suite.jobConfigOps.EXPECT().
		Create(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Do(func(_ context.Context,
			_ *peloton.JobID,
			config *pbjob.JobConfig,
			_ *models.ConfigAddOn,
			_ *stateless.JobSpec,
			_ uint64,
		) {
			suite.Equal(config.ChangeLog.Version, jobVersion+1)
		}).
		Return(nil)
	suite.jobIndexOps.EXPECT().
		Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	suite.taskConfigV2Ops.EXPECT().
		Create(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			jobVersion+1,
		).Return(nil)

	suite.jobConfigOps.EXPECT().Get(gomock.Any(), suite.jobID, jobVersion).
		Return(jobConfig, nil, nil)

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, nil).
		Return(taskRuntimes, nil)

	for i := uint32(0); i < jobPrevConfig.GetInstanceCount(); i++ {
		suite.taskConfigV2Ops.EXPECT().
			GetTaskConfig(gomock.Any(), suite.jobID, i, jobPrevVersion).
			Return(&pbtask.TaskConfig{}, nil, nil)
	}

	for i := uint32(0); i < jobConfig.GetInstanceCount(); i++ {
		suite.updateStore.EXPECT().
			AddWorkflowEvent(
				gomock.Any(),
				gomock.Any(),
				i,
				models.WorkflowType_UPDATE,
				pbupdate.State_ROLLING_BACKWARD).
			Return(nil)
	}

	suite.jobUpdateEventsOps.EXPECT().
		Create(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			pbupdate.State_ROLLING_BACKWARD).
		Return(nil)

	suite.updateStore.EXPECT().
		ModifyUpdate(gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, updateInfo *models.UpdateModel) {
			suite.Equal(updateInfo.GetState(), pbupdate.State_ROLLING_BACKWARD)
			suite.Equal(updateInfo.GetJobConfigVersion(), jobVersion+1)
			suite.Equal(updateInfo.GetPrevJobConfigVersion(), jobVersion)
			suite.Equal(updateInfo.GetInstancesDone(), uint32(0))
			suite.Equal(updateInfo.GetInstancesFailed(), uint32(0))
			suite.Empty(updateInfo.GetInstancesCurrent())
		}).Return(nil)

	suite.jobRuntimeOps.EXPECT().
		Upsert(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.ConfigurationVersion, jobVersion+1)
		}).
		Return(nil)
	suite.jobIndexOps.EXPECT().
		Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	suite.NoError(suite.job.RollbackWorkflow(context.Background()))

	suite.Empty(cachedWorkflow.GetInstancesFailed())
	suite.Empty(cachedWorkflow.GetInstancesDone())
	suite.Empty(cachedWorkflow.GetInstancesCurrent())
	suite.Equal(cachedWorkflow.GetState().State, pbupdate.State_ROLLING_BACKWARD)
	suite.Equal(len(cachedWorkflow.GetInstancesRemoved()), 5)
	suite.Equal(len(cachedWorkflow.GetInstancesUpdated()), 5)
}

// TestRollbackWorkflowRecoverFailure tests the failure case of
// rollback a workflow due to recover failure
func (suite *jobTestSuite) TestRollbackWorkflowRecoverFailure() {
	cachedWorkflow := &update{}
	cachedWorkflow.state = pbupdate.State_INVALID
	cachedWorkflow.workflowType = models.WorkflowType_UPDATE
	cachedWorkflow.jobFactory = suite.job.jobFactory

	suite.job.runtime.UpdateID = &peloton.UpdateID{Value: testUpdateID}
	suite.job.workflows[testUpdateID] = cachedWorkflow

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.job.RollbackWorkflow(context.Background()))
}

// TestRollbackWorkflowGetPrevConfigFailure tests the failure case of
// rollback a workflow due to unable get prev job config
func (suite *jobTestSuite) TestRollbackWorkflowGetPrevConfigFailure() {
	jobPrevVersion := uint64(1)

	jobVersion := uint64(2)
	jobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: jobVersion,
		},
		InstanceCount: 10,
	}

	cachedWorkflow := &update{}
	cachedWorkflow.state = pbupdate.State_ROLLING_FORWARD
	cachedWorkflow.jobID = suite.jobID
	cachedWorkflow.jobVersion = jobVersion
	cachedWorkflow.jobPrevVersion = jobPrevVersion
	cachedWorkflow.jobFactory = suite.job.jobFactory
	cachedWorkflow.workflowType = models.WorkflowType_UPDATE

	suite.job.runtime.UpdateID = &peloton.UpdateID{Value: testUpdateID}
	suite.job.workflows[testUpdateID] = cachedWorkflow

	taskRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < jobConfig.GetInstanceCount(); i++ {
		runtime := &pbtask.RuntimeInfo{
			State:                pbtask.TaskState_RUNNING,
			ConfigVersion:        jobPrevVersion,
			DesiredConfigVersion: jobPrevVersion,
		}
		taskRuntimes[i] = runtime
	}

	suite.jobConfigOps.EXPECT().GetResult(
		gomock.Any(), suite.jobID, jobPrevVersion).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.job.RollbackWorkflow(context.Background()))
}

// TestRollbackWorkflowGetTargetConfigFailure tests the failure case of
// rollback a workflow due to unable get target job config
func (suite *jobTestSuite) TestRollbackWorkflowGetTargetConfigFailure() {
	jobPrevVersion := uint64(1)
	jobPrevConfig := &pbjob.JobConfig{
		DefaultConfig: &pbtask.TaskConfig{},
		ChangeLog: &peloton.ChangeLog{
			Version: jobPrevVersion,
		},
		InstanceCount: 5,
	}

	jobVersion := uint64(2)
	jobConfig := &pbjob.JobConfig{
		DefaultConfig: &pbtask.TaskConfig{},
		ChangeLog: &peloton.ChangeLog{
			Version: jobVersion,
		},
		InstanceCount: 10,
	}

	cachedWorkflow := &update{}
	cachedWorkflow.state = pbupdate.State_ROLLING_FORWARD
	cachedWorkflow.jobID = suite.jobID
	cachedWorkflow.jobVersion = jobVersion
	cachedWorkflow.jobPrevVersion = jobPrevVersion
	cachedWorkflow.jobFactory = suite.job.jobFactory
	cachedWorkflow.workflowType = models.WorkflowType_UPDATE

	suite.job.runtime.UpdateID = &peloton.UpdateID{Value: testUpdateID}
	suite.job.workflows[testUpdateID] = cachedWorkflow

	taskRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < jobConfig.GetInstanceCount(); i++ {
		runtime := &pbtask.RuntimeInfo{
			State:                pbtask.TaskState_RUNNING,
			ConfigVersion:        jobPrevVersion,
			DesiredConfigVersion: jobPrevVersion,
		}
		taskRuntimes[i] = runtime
	}

	suite.jobConfigOps.EXPECT().GetResult(
		gomock.Any(), suite.jobID, jobPrevVersion).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig: jobPrevConfig,
		}, nil)

	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID.GetValue()).
		Return(jobVersion, nil)

	suite.jobConfigOps.EXPECT().
		Create(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Do(func(_ context.Context,
			_ *peloton.JobID,
			config *pbjob.JobConfig,
			_ *models.ConfigAddOn,
			_ *stateless.JobSpec,
			_ uint64,
		) {
			suite.Equal(config.ChangeLog.Version, jobVersion+1)
		}).
		Return(nil)
	suite.jobIndexOps.EXPECT().
		Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	suite.taskConfigV2Ops.EXPECT().
		Create(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			jobVersion+1,
		).Return(nil)

	suite.jobConfigOps.EXPECT().Get(gomock.Any(), suite.jobID, jobVersion).
		Return(nil, nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.job.RollbackWorkflow(context.Background()))
}

// TestRollbackWorkflowCopyConfigFailure tests the failure case of
// rollback a workflow due to config copy
func (suite *jobTestSuite) TestRollbackWorkflowCopyConfigFailure() {
	jobPrevVersion := uint64(1)
	jobPrevConfig := &pbjob.JobConfig{
		DefaultConfig: &pbtask.TaskConfig{},
		ChangeLog: &peloton.ChangeLog{
			Version: jobPrevVersion,
		},
		InstanceCount: 5,
	}

	jobVersion := uint64(2)
	jobConfig := &pbjob.JobConfig{
		DefaultConfig: &pbtask.TaskConfig{},
		ChangeLog: &peloton.ChangeLog{
			Version: jobVersion,
		},
		InstanceCount: 10,
	}

	cachedWorkflow := &update{}
	cachedWorkflow.state = pbupdate.State_ROLLING_FORWARD
	cachedWorkflow.jobID = suite.jobID
	cachedWorkflow.jobVersion = jobVersion
	cachedWorkflow.jobPrevVersion = jobPrevVersion
	cachedWorkflow.jobFactory = suite.job.jobFactory
	cachedWorkflow.workflowType = models.WorkflowType_UPDATE

	suite.job.runtime.UpdateID = &peloton.UpdateID{Value: testUpdateID}
	suite.job.workflows[testUpdateID] = cachedWorkflow

	taskRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < jobConfig.GetInstanceCount(); i++ {
		runtime := &pbtask.RuntimeInfo{
			State:                pbtask.TaskState_RUNNING,
			ConfigVersion:        jobPrevVersion,
			DesiredConfigVersion: jobPrevVersion,
		}
		taskRuntimes[i] = runtime
	}

	suite.jobConfigOps.EXPECT().GetResult(
		gomock.Any(), suite.jobID, jobPrevVersion).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig: jobPrevConfig,
		}, nil)

	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID.GetValue()).
		Return(jobVersion, nil)

	suite.jobConfigOps.EXPECT().
		Create(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Do(func(_ context.Context,
			_ *peloton.JobID,
			config *pbjob.JobConfig,
			_ *models.ConfigAddOn,
			_ *stateless.JobSpec,
			_ uint64,
		) {
			suite.Equal(config.ChangeLog.Version, jobVersion+1)
		}).
		Return(nil)
	suite.jobIndexOps.EXPECT().
		Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	suite.taskConfigV2Ops.EXPECT().
		Create(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			jobVersion+1,
		).Return(yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.job.RollbackWorkflow(context.Background()))
}

// TestRollbackWorkflowNoWorkflow tests the case of
// rollback a workflow whe the job does not have one
func (suite *jobTestSuite) TestRollbackWorkflowNoWorkflow() {
	jobPrevVersion := uint64(1)

	jobVersion := uint64(2)

	cachedWorkflow := &update{}
	cachedWorkflow.state = pbupdate.State_ROLLING_FORWARD
	cachedWorkflow.jobID = suite.jobID
	cachedWorkflow.jobVersion = jobVersion
	cachedWorkflow.jobPrevVersion = jobPrevVersion
	cachedWorkflow.jobFactory = suite.job.jobFactory
	cachedWorkflow.workflowType = models.WorkflowType_UPDATE

	suite.job.runtime.UpdateID = nil

	suite.Error(suite.job.RollbackWorkflow(context.Background()))
}

// TestRollbackWorkflowSuccessAfterModifyUpdateFails tests workflow rollback can
// be retried successfully after it fails due to updateStore.ModifyUpdate
func (suite *jobTestSuite) TestRollbackWorkflowSuccessAfterModifyUpdateFails() {
	jobPrevVersion := uint64(1)
	jobPrevConfig := &pbjob.JobConfig{
		DefaultConfig: &pbtask.TaskConfig{
			Revocable: true,
		},
		ChangeLog: &peloton.ChangeLog{
			Version: jobPrevVersion,
		},
		InstanceCount: 5,
	}

	jobVersion := uint64(2)
	jobConfig := &pbjob.JobConfig{
		DefaultConfig: &pbtask.TaskConfig{
			Revocable: false,
		},
		ChangeLog: &peloton.ChangeLog{
			Version: jobVersion,
		},
		InstanceCount: 10,
	}

	cachedWorkflow := &update{}
	cachedWorkflow.state = pbupdate.State_ROLLING_FORWARD
	cachedWorkflow.jobID = suite.jobID
	cachedWorkflow.jobVersion = jobVersion
	cachedWorkflow.jobPrevVersion = jobPrevVersion
	cachedWorkflow.jobFactory = suite.job.jobFactory
	cachedWorkflow.workflowType = models.WorkflowType_UPDATE

	suite.job.runtime.UpdateID = &peloton.UpdateID{Value: testUpdateID}
	suite.job.workflows[testUpdateID] = cachedWorkflow

	taskRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < jobConfig.GetInstanceCount(); i++ {
		runtime := &pbtask.RuntimeInfo{
			State:                pbtask.TaskState_RUNNING,
			ConfigVersion:        jobPrevVersion,
			DesiredConfigVersion: jobPrevVersion,
		}
		taskRuntimes[i] = runtime
	}

	suite.jobConfigOps.EXPECT().GetResult(
		gomock.Any(), suite.jobID, jobPrevVersion).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig: proto.Clone(jobPrevConfig).(*pbjob.JobConfig),
		}, nil)

	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID.GetValue()).
		Return(jobVersion, nil)

	suite.jobConfigOps.EXPECT().
		Create(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Do(func(_ context.Context,
			_ *peloton.JobID,
			config *pbjob.JobConfig,
			_ *models.ConfigAddOn,
			_ *stateless.JobSpec,
			_ uint64,
		) {
			suite.Equal(config.ChangeLog.Version, jobVersion+1)
		}).
		Return(nil)
	suite.jobIndexOps.EXPECT().
		Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	suite.taskConfigV2Ops.EXPECT().
		Create(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			jobVersion+1,
		).Return(nil)

	suite.jobConfigOps.EXPECT().Get(gomock.Any(), suite.jobID, jobVersion).
		Return(proto.Clone(jobConfig).(*pbjob.JobConfig), nil, nil)

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, nil).
		Return(taskRuntimes, nil)

	for i := uint32(0); i < jobPrevConfig.GetInstanceCount(); i++ {
		suite.taskConfigV2Ops.EXPECT().
			GetTaskConfig(gomock.Any(), suite.jobID, i, jobPrevVersion).
			Return(&pbtask.TaskConfig{}, nil, nil)
	}

	for i := uint32(0); i < jobConfig.GetInstanceCount(); i++ {
		suite.updateStore.EXPECT().
			AddWorkflowEvent(
				gomock.Any(),
				gomock.Any(),
				uint32(i),
				models.WorkflowType_UPDATE,
				pbupdate.State_ROLLING_BACKWARD).
			Return(nil)
	}

	suite.jobUpdateEventsOps.EXPECT().
		Create(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			pbupdate.State_ROLLING_BACKWARD).
		Return(nil)

	suite.updateStore.EXPECT().
		ModifyUpdate(gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, updateInfo *models.UpdateModel) {
			suite.Equal(updateInfo.GetState(), pbupdate.State_ROLLING_BACKWARD)
			suite.Equal(updateInfo.GetJobConfigVersion(), jobVersion+1)
			suite.Equal(updateInfo.GetPrevJobConfigVersion(), jobVersion)
			suite.Equal(updateInfo.GetInstancesDone(), uint32(0))
			suite.Equal(updateInfo.GetInstancesFailed(), uint32(0))
			suite.Empty(updateInfo.GetInstancesCurrent())
		}).Return(yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.job.RollbackWorkflow(context.Background()))

	newMaxJobVersion := jobVersion + 1

	for i := uint32(0); i < jobConfig.GetInstanceCount(); i++ {
		suite.updateStore.EXPECT().
			AddWorkflowEvent(
				gomock.Any(),
				gomock.Any(),
				uint32(i),
				models.WorkflowType_UPDATE,
				pbupdate.State_ROLLING_BACKWARD).
			Return(nil)
	}

	gomock.InOrder(
		suite.updateStore.EXPECT().
			GetUpdate(gomock.Any(), gomock.Any()).
			Return(
				&models.UpdateModel{
					State:                pbupdate.State_ROLLING_FORWARD,
					JobConfigVersion:     jobVersion,
					PrevJobConfigVersion: jobPrevVersion,
					Type:                 models.WorkflowType_UPDATE,
				}, nil),

		suite.jobConfigOps.EXPECT().GetResult(
			gomock.Any(), suite.jobID, jobPrevVersion).
			Return(&ormobjects.JobConfigOpsResult{
				JobConfig: proto.Clone(jobPrevConfig).(*pbjob.JobConfig),
			}, nil),

		suite.jobConfigOps.EXPECT().Get(
			gomock.Any(), suite.jobID, jobPrevVersion).
			Return(proto.Clone(jobPrevConfig).(*pbjob.JobConfig), nil, nil),

		suite.jobStore.EXPECT().
			GetMaxJobConfigVersion(gomock.Any(), suite.jobID.GetValue()).
			Return(newMaxJobVersion, nil),

		suite.jobConfigOps.EXPECT().
			Create(
				gomock.Any(),
				suite.jobID,
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				gomock.Any()).
			Do(func(_ context.Context,
				_ *peloton.JobID,
				config *pbjob.JobConfig,
				_ *models.ConfigAddOn,
				_ *stateless.JobSpec,
				_ uint64,
			) {
				suite.Equal(config.ChangeLog.Version, newMaxJobVersion+1)
			}).
			Return(nil),
		suite.jobIndexOps.EXPECT().
			Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
			Return(nil),

		suite.taskConfigV2Ops.EXPECT().
			Create(
				gomock.Any(),
				suite.jobID,
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
			).Return(nil).AnyTimes(),

		suite.jobConfigOps.EXPECT().Get(
			gomock.Any(), suite.jobID, jobVersion).
			Return(proto.Clone(jobConfig).(*pbjob.JobConfig), nil, nil),

		suite.taskStore.EXPECT().
			GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, nil).
			Return(taskRuntimes, nil),

		suite.jobUpdateEventsOps.EXPECT().
			Create(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				pbupdate.State_ROLLING_BACKWARD).
			Return(nil),

		suite.updateStore.EXPECT().
			ModifyUpdate(gomock.Any(), gomock.Any()).
			Do(func(_ context.Context, updateInfo *models.UpdateModel) {
				suite.Equal(updateInfo.GetState(), pbupdate.State_ROLLING_BACKWARD)
				suite.Equal(updateInfo.GetJobConfigVersion(), newMaxJobVersion+1)
				suite.Equal(updateInfo.GetPrevJobConfigVersion(), jobVersion)
				suite.Equal(updateInfo.GetInstancesDone(), uint32(0))
				suite.Equal(updateInfo.GetInstancesFailed(), uint32(0))
				suite.Empty(updateInfo.GetInstancesCurrent())
			}).Return(nil),

		suite.jobRuntimeOps.EXPECT().
			Upsert(gomock.Any(), suite.jobID, gomock.Any()).
			Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
				suite.Equal(runtime.ConfigurationVersion, newMaxJobVersion+1)
			}).
			Return(nil),
		suite.jobIndexOps.EXPECT().
			Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
			Return(nil),
	)

	suite.NoError(suite.job.RollbackWorkflow(context.Background()))

}

// TestRollbackWorkflowSuccessAfterJobRuntimeUpdateDBWriteFails tests workflow rollback can
// be retried successfully after it fails due to job runtime update failure
func (suite *jobTestSuite) TestRollbackWorkflowSuccessAfterJobRuntimeUpdateDBWriteFails() {
	jobPrevVersion := uint64(1)
	jobPrevConfig := &pbjob.JobConfig{
		DefaultConfig: &pbtask.TaskConfig{},
		ChangeLog: &peloton.ChangeLog{
			Version: jobPrevVersion,
		},
		InstanceCount: 5,
	}

	jobVersion := uint64(2)
	jobConfig := &pbjob.JobConfig{
		DefaultConfig: &pbtask.TaskConfig{},
		ChangeLog: &peloton.ChangeLog{
			Version: jobVersion,
		},
		InstanceCount: 10,
	}

	cachedWorkflow := &update{}
	cachedWorkflow.state = pbupdate.State_ROLLING_FORWARD
	cachedWorkflow.jobID = suite.jobID
	cachedWorkflow.jobVersion = jobVersion
	cachedWorkflow.jobPrevVersion = jobPrevVersion
	cachedWorkflow.jobFactory = suite.job.jobFactory
	cachedWorkflow.workflowType = models.WorkflowType_UPDATE

	suite.job.runtime.UpdateID = &peloton.UpdateID{Value: testUpdateID}
	suite.job.workflows[testUpdateID] = cachedWorkflow

	taskRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < jobConfig.GetInstanceCount(); i++ {
		runtime := &pbtask.RuntimeInfo{
			State:                pbtask.TaskState_RUNNING,
			ConfigVersion:        jobPrevVersion,
			DesiredConfigVersion: jobPrevVersion,
		}
		taskRuntimes[i] = runtime
	}

	suite.jobConfigOps.EXPECT().GetResult(
		gomock.Any(), suite.jobID, jobPrevVersion).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig: jobPrevConfig,
		}, nil)

	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID.GetValue()).
		Return(jobVersion, nil)

	suite.jobConfigOps.EXPECT().
		Create(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Do(func(_ context.Context,
			_ *peloton.JobID,
			config *pbjob.JobConfig,
			_ *models.ConfigAddOn,
			_ *stateless.JobSpec,
			_ uint64,
		) {
			suite.Equal(config.ChangeLog.Version, jobVersion+1)
		}).
		Return(nil)
	suite.jobIndexOps.EXPECT().
		Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	suite.taskConfigV2Ops.EXPECT().
		Create(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			jobVersion+1,
		).Return(nil)

	suite.jobConfigOps.EXPECT().Get(
		gomock.Any(), suite.jobID, jobVersion).
		Return(jobConfig, nil, nil)

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, nil).
		Return(taskRuntimes, nil)

	for i := uint32(0); i < jobPrevConfig.GetInstanceCount(); i++ {
		suite.taskConfigV2Ops.EXPECT().
			GetTaskConfig(gomock.Any(), suite.jobID, i, jobPrevVersion).
			Return(&pbtask.TaskConfig{}, nil, nil)
	}

	for i := uint32(0); i < jobConfig.GetInstanceCount(); i++ {
		suite.updateStore.EXPECT().
			AddWorkflowEvent(
				gomock.Any(),
				gomock.Any(),
				uint32(i),
				models.WorkflowType_UPDATE,
				pbupdate.State_ROLLING_BACKWARD).
			Return(nil).AnyTimes()
	}

	suite.jobUpdateEventsOps.EXPECT().
		Create(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			pbupdate.State_ROLLING_BACKWARD).
		Return(nil)

	suite.updateStore.EXPECT().
		ModifyUpdate(gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, updateInfo *models.UpdateModel) {
			suite.Equal(updateInfo.GetState(), pbupdate.State_ROLLING_BACKWARD)
			suite.Equal(updateInfo.GetJobConfigVersion(), jobVersion+1)
			suite.Equal(updateInfo.GetPrevJobConfigVersion(), jobVersion)
			suite.Equal(updateInfo.GetInstancesDone(), uint32(0))
			suite.Equal(updateInfo.GetInstancesFailed(), uint32(0))
			suite.Empty(updateInfo.GetInstancesCurrent())
		}).Return(nil)

	suite.jobRuntimeOps.EXPECT().
		Upsert(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.ConfigurationVersion, jobVersion+1)
		}).
		Return(yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.job.RollbackWorkflow(context.Background()))

	newMaxJobVersion := jobVersion + 1

	suite.jobRuntimeOps.EXPECT().
		Get(context.Background(), suite.jobID).
		Return(&pbjob.RuntimeInfo{
			ConfigurationVersion: jobVersion,
			UpdateID:             &peloton.UpdateID{Value: testUpdateID},
		}, nil)

	suite.jobRuntimeOps.EXPECT().
		Upsert(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.ConfigurationVersion, newMaxJobVersion)
		}).
		Return(nil)
	suite.jobIndexOps.EXPECT().
		Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	suite.NoError(suite.job.RollbackWorkflow(context.Background()))
}

// TestRollbackWorkflowSuccessAfterJobRuntimeDBWriteSucceedsWithError tests workflow rollback can
// be retried successfully after it fails due to job runtime update failure, but the runtime
// is actually persisted in db
func (suite *jobTestSuite) TestRollbackWorkflowSuccessAfterJobRuntimeDBWriteSucceedsWithError() {
	jobPrevVersion := uint64(1)
	jobPrevConfig := &pbjob.JobConfig{
		DefaultConfig: &pbtask.TaskConfig{},
		ChangeLog: &peloton.ChangeLog{
			Version: jobPrevVersion,
		},
		InstanceCount: 5,
	}

	jobVersion := uint64(2)
	jobConfig := &pbjob.JobConfig{
		DefaultConfig: &pbtask.TaskConfig{},
		ChangeLog: &peloton.ChangeLog{
			Version: jobVersion,
		},
		InstanceCount: 10,
	}

	cachedWorkflow := &update{}
	cachedWorkflow.state = pbupdate.State_ROLLING_FORWARD
	cachedWorkflow.jobID = suite.jobID
	cachedWorkflow.jobVersion = jobVersion
	cachedWorkflow.jobPrevVersion = jobPrevVersion
	cachedWorkflow.jobFactory = suite.job.jobFactory
	cachedWorkflow.workflowType = models.WorkflowType_UPDATE

	suite.job.runtime.UpdateID = &peloton.UpdateID{Value: testUpdateID}
	suite.job.workflows[testUpdateID] = cachedWorkflow

	taskRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < jobConfig.GetInstanceCount(); i++ {
		runtime := &pbtask.RuntimeInfo{
			State:                pbtask.TaskState_RUNNING,
			ConfigVersion:        jobPrevVersion,
			DesiredConfigVersion: jobPrevVersion,
		}
		taskRuntimes[i] = runtime
	}

	suite.jobConfigOps.EXPECT().GetResult(
		gomock.Any(), suite.jobID, jobPrevVersion).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig: jobPrevConfig,
		}, nil)

	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID.GetValue()).
		Return(jobVersion, nil)

	suite.jobConfigOps.EXPECT().
		Create(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Do(func(_ context.Context,
			_ *peloton.JobID,
			config *pbjob.JobConfig,
			_ *models.ConfigAddOn,
			_ *stateless.JobSpec,
			_ uint64,
		) {
			suite.Equal(config.ChangeLog.Version, jobVersion+1)
		}).
		Return(nil)
	suite.jobIndexOps.EXPECT().
		Update(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	suite.taskConfigV2Ops.EXPECT().
		Create(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			jobVersion+1,
		).Return(nil)

	suite.jobConfigOps.EXPECT().Get(
		gomock.Any(), suite.jobID, jobVersion).
		Return(jobConfig, nil, nil)

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, nil).
		Return(taskRuntimes, nil)

	for i := uint32(0); i < jobPrevConfig.GetInstanceCount(); i++ {
		suite.taskConfigV2Ops.EXPECT().
			GetTaskConfig(gomock.Any(), suite.jobID, i, jobPrevVersion).
			Return(&pbtask.TaskConfig{}, nil, nil)
	}

	for i := uint32(0); i < jobConfig.GetInstanceCount(); i++ {
		suite.updateStore.EXPECT().
			AddWorkflowEvent(
				gomock.Any(),
				gomock.Any(),
				uint32(i),
				models.WorkflowType_UPDATE,
				pbupdate.State_ROLLING_BACKWARD).
			Return(nil).AnyTimes()
	}

	suite.jobUpdateEventsOps.EXPECT().
		Create(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			pbupdate.State_ROLLING_BACKWARD).
		Return(nil)

	suite.updateStore.EXPECT().
		ModifyUpdate(gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, updateInfo *models.UpdateModel) {
			suite.Equal(updateInfo.GetState(), pbupdate.State_ROLLING_BACKWARD)
			suite.Equal(updateInfo.GetJobConfigVersion(), jobVersion+1)
			suite.Equal(updateInfo.GetPrevJobConfigVersion(), jobVersion)
			suite.Equal(updateInfo.GetInstancesDone(), uint32(0))
			suite.Equal(updateInfo.GetInstancesFailed(), uint32(0))
			suite.Empty(updateInfo.GetInstancesCurrent())
		}).Return(nil)

	suite.jobRuntimeOps.EXPECT().
		Upsert(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.ConfigurationVersion, jobVersion+1)
		}).
		Return(yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.job.RollbackWorkflow(context.Background()))

	newMaxJobVersion := jobVersion + 1

	suite.jobRuntimeOps.EXPECT().
		Get(context.Background(), suite.jobID).
		Return(&pbjob.RuntimeInfo{
			ConfigurationVersion: newMaxJobVersion,
			UpdateID:             &peloton.UpdateID{Value: testUpdateID},
		}, nil)

	suite.NoError(suite.job.RollbackWorkflow(context.Background()))
}

// TestJobCreateTaskConfigsSuccess tests success case of creating task configurations
func (suite *jobTestSuite) TestJobCreateTaskConfigsSuccess() {
	instanceCount := uint32(10)
	jobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 1,
		},
		OwningTeam:    "uber",
		LdapGroups:    []string{"money", "team6", "otto"},
		InstanceCount: instanceCount,
		DefaultConfig: &pbtask.TaskConfig{
			Name: "instance",
			Resource: &pbtask.ResourceConfig{
				CpuLimit:    0.8,
				MemLimitMb:  800,
				DiskLimitMb: 1500,
			},
		},
		InstanceConfig: map[uint32]*pbtask.TaskConfig{
			2: {
				Name: "instance2",
				Resource: &pbtask.ResourceConfig{
					CpuLimit:    1,
					MemLimitMb:  500,
					DiskLimitMb: 1000,
				},
			},

			4: {
				Name: "instance4",
				Resource: &pbtask.ResourceConfig{
					CpuLimit:    1,
					MemLimitMb:  600,
					DiskLimitMb: 1200,
				},
			},
		},
	}

	suite.taskConfigV2Ops.EXPECT().
		Create(
			gomock.Any(),
			suite.jobID,
			int64(-1),
			jobConfig.GetDefaultConfig(),
			gomock.Any(),
			nil,
			jobConfig.GetChangeLog().GetVersion()).
		Return(nil)

	for i, taskConfig := range jobConfig.GetInstanceConfig() {
		suite.taskConfigV2Ops.EXPECT().
			Create(
				gomock.Any(),
				suite.jobID,
				int64(i),
				taskConfig,
				gomock.Any(),
				nil,
				jobConfig.GetChangeLog().GetVersion()).
			Return(nil)
	}

	suite.NoError(
		suite.job.CreateTaskConfigs(
			context.Background(),
			suite.jobID,
			jobConfig,
			&models.ConfigAddOn{},
			nil,
		),
	)
}

// TestJobCreateTaskConfigsWithSpec tests success case of creating
// task configs along with with pod spec
func (suite *jobTestSuite) TestJobCreateTaskConfigsWithSpec() {
	instanceCount := uint32(10)
	jobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 1,
		},
		DefaultConfig: &pbtask.TaskConfig{
			Name: "instance",
			Resource: &pbtask.ResourceConfig{
				CpuLimit:    0.8,
				MemLimitMb:  800,
				DiskLimitMb: 1500,
			},
		},
		InstanceCount: instanceCount,
		InstanceConfig: map[uint32]*pbtask.TaskConfig{
			2: {
				Name: "instance2",
				Resource: &pbtask.ResourceConfig{
					CpuLimit:    1,
					MemLimitMb:  500,
					DiskLimitMb: 1000,
				},
			},
			4: {
				Name: "instance4",
				Resource: &pbtask.ResourceConfig{
					CpuLimit:    1,
					MemLimitMb:  600,
					DiskLimitMb: 1200,
				},
			},
		},
	}

	podSpec := &pbpod.PodSpec{
		PodName:    &v1alphapeloton.PodName{Value: "test-pod"},
		Containers: []*pbpod.ContainerSpec{{}},
	}
	jobSpec := &stateless.JobSpec{
		DefaultSpec: podSpec,
		InstanceSpec: map[uint32]*pbpod.PodSpec{
			2: podSpec,
			4: podSpec,
		},
		Revision: &v1alphapeloton.Revision{Version: 1},
	}

	suite.taskConfigV2Ops.EXPECT().
		Create(
			gomock.Any(),
			suite.jobID,
			int64(-1),
			jobConfig.GetDefaultConfig(),
			gomock.Any(),
			jobSpec.GetDefaultSpec(),
			jobConfig.GetChangeLog().GetVersion()).
		Return(nil)

	for i, taskConfig := range jobConfig.GetInstanceConfig() {
		suite.taskConfigV2Ops.EXPECT().
			Create(
				gomock.Any(),
				suite.jobID,
				int64(i),
				taskConfig,
				gomock.Any(),
				podSpec,
				jobConfig.GetChangeLog().GetVersion()).
			Return(nil)
	}

	suite.NoError(
		suite.job.CreateTaskConfigs(
			context.Background(),
			suite.jobID,
			jobConfig,
			&models.ConfigAddOn{},
			jobSpec,
		),
	)
}

// TestJobCreateTaskConfigsNoDefaultConfigSuccess tests success case of
// creating task configurations without task config
func (suite *jobTestSuite) TestJobCreateTaskConfigsNoDefaultConfigSuccess() {
	instanceCount := uint32(10)
	jobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 1,
		},
		OwningTeam:    "uber",
		LdapGroups:    []string{"money", "team6", "otto"},
		InstanceCount: instanceCount,
		InstanceConfig: map[uint32]*pbtask.TaskConfig{
			2: {
				Name: "instance2",
				Resource: &pbtask.ResourceConfig{
					CpuLimit:    1,
					MemLimitMb:  500,
					DiskLimitMb: 1000,
				},
			},

			4: {
				Name: "instance4",
				Resource: &pbtask.ResourceConfig{
					CpuLimit:    1,
					MemLimitMb:  600,
					DiskLimitMb: 1200,
				},
			},
		},
	}

	for i, taskConfig := range jobConfig.GetInstanceConfig() {
		suite.taskConfigV2Ops.EXPECT().
			Create(
				gomock.Any(),
				suite.jobID,
				int64(i),
				taskConfig,
				gomock.Any(),
				nil,
				jobConfig.GetChangeLog().GetVersion()).
			Return(nil)
	}

	suite.NoError(
		suite.job.CreateTaskConfigs(
			context.Background(),
			suite.jobID,
			jobConfig,
			&models.ConfigAddOn{},
			nil,
		),
	)
}

// TestJobCreateTaskConfigsFailureToCreateDefaultConfig tests failure
// case of JobCreateTaskConfigs due to error while writing default config
func (suite *jobTestSuite) TestJobCreateTaskConfigsFailureToCreateDefaultConfig() {
	instanceCount := uint32(10)
	jobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 1,
		},
		OwningTeam:    "uber",
		LdapGroups:    []string{"money", "team6", "otto"},
		InstanceCount: instanceCount,
		DefaultConfig: &pbtask.TaskConfig{
			Name: "instance",
			Resource: &pbtask.ResourceConfig{
				CpuLimit:    0.8,
				MemLimitMb:  800,
				DiskLimitMb: 1500,
			},
		},
	}

	suite.taskConfigV2Ops.EXPECT().
		Create(
			gomock.Any(),
			suite.jobID,
			int64(-1),
			jobConfig.GetDefaultConfig(),
			gomock.Any(),
			nil,
			jobConfig.GetChangeLog().GetVersion()).
		Return(yarpcerrors.InternalErrorf("test error"))

	suite.Error(
		suite.job.CreateTaskConfigs(
			context.Background(),
			suite.jobID,
			jobConfig,
			&models.ConfigAddOn{},
			nil,
		),
	)
}

func (suite *jobTestSuite) TestGetCurrentAndGoalState() {
	currentState := suite.job.CurrentState()
	suite.Equal(currentState.StateVersion, suite.job.runtime.GetStateVersion())
	suite.Equal(currentState.State, suite.job.runtime.GetState())

	goalState := suite.job.GoalState()
	suite.Equal(goalState.StateVersion, suite.job.runtime.GetDesiredStateVersion())
	suite.Equal(goalState.State, suite.job.runtime.GetGoalState())
}

func (suite *jobTestSuite) TestGetStateCount() {
	taskInfos := make(map[uint32]*pbtask.TaskInfo)
	taskInfos[0] = &pbtask.TaskInfo{
		Runtime: &pbtask.RuntimeInfo{
			State:     pbtask.TaskState_PENDING,
			GoalState: pbtask.TaskState_SUCCEEDED,
			Revision:  &peloton.ChangeLog{Version: 1},
			Message:   "default",
		},
	}
	taskInfos[1] = &pbtask.TaskInfo{
		Runtime: &pbtask.RuntimeInfo{
			State:     pbtask.TaskState_PENDING,
			GoalState: pbtask.TaskState_SUCCEEDED,
			Revision:  &peloton.ChangeLog{Version: 1},
			Message:   "default",
		},
	}

	taskInfos[2] = &pbtask.TaskInfo{
		Runtime: &pbtask.RuntimeInfo{
			State:     pbtask.TaskState_KILLED,
			GoalState: pbtask.TaskState_DELETED,
			Revision:  &peloton.ChangeLog{Version: 1},
			Message:   common.TaskThrottleMessage,
		},
	}

	suite.job.ReplaceTasks(taskInfos, true)
	suite.job.config = &cachedConfig{
		jobType: pbjob.JobType_SERVICE,
	}

	testUpdate := newUpdate(&peloton.UpdateID{Value: testUpdateID}, nil)
	testUpdate.state = pbupdate.State_ROLLING_FORWARD
	suite.job.workflows[testUpdateID] = testUpdate

	taskCount, throttledTasks, _ := suite.job.GetTaskStateCount()
	updateCount := suite.job.GetWorkflowStateCount()
	suite.Equal(
		taskCount[TaskStateSummary{
			CurrentState: pbtask.TaskState_PENDING,
			GoalState:    pbtask.TaskState_SUCCEEDED,
			HealthState:  pbtask.HealthState_INVALID,
		}], 2)
	suite.Equal(
		taskCount[TaskStateSummary{
			CurrentState: pbtask.TaskState_KILLED,
			GoalState:    pbtask.TaskState_DELETED,
			HealthState:  pbtask.HealthState_INVALID,
		}], 1)
	suite.Equal(throttledTasks, 1)
	suite.Equal(updateCount[pbupdate.State_ROLLING_FORWARD], 1)
}

// TestJobRollingCreateSuccess tests job
// rolling create in cache and db
func (suite *jobTestSuite) TestJobRollingCreateSuccess() {
	jobConfig := &pbjob.JobConfig{
		InstanceCount: 10,
		Type:          pbjob.JobType_SERVICE,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
	}

	createdBy := "test"
	configAddOn := &models.ConfigAddOn{
		SystemLabels: []*peloton.Label{
			{
				Key: fmt.Sprintf(
					common.SystemLabelKeyTemplate,
					common.SystemLabelPrefix,
					common.SystemLabelJobOwner),
				Value: createdBy,
			},
			{
				Key: fmt.Sprintf(
					common.SystemLabelKeyTemplate,
					common.SystemLabelPrefix,
					common.SystemLabelJobType),
				Value: jobConfig.Type.String(),
			},
		},
	}

	suite.activeJobsOps.EXPECT().
		Create(gomock.Any(), suite.jobID).Return(nil)

	suite.jobRuntimeOps.EXPECT().
		Upsert(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, initialRuntime *pbjob.RuntimeInfo) {
			suite.Equal(initialRuntime.State, pbjob.JobState_UNINITIALIZED)
			suite.Equal(initialRuntime.GoalState, pbjob.JobState_RUNNING)
			suite.Equal(initialRuntime.Revision.Version, uint64(1))
			suite.Equal(initialRuntime.ConfigurationVersion, uint64(1))
			suite.Equal(initialRuntime.TaskStats[pbjob.JobState_INITIALIZED.String()], jobConfig.InstanceCount)
			suite.Equal(len(initialRuntime.TaskStats), 1)
		}).
		Return(nil)

	suite.jobNameToIDOps.EXPECT().
		Create(gomock.Any(), gomock.Any(), suite.jobID).
		Return(nil)

	suite.jobConfigOps.EXPECT().
		Create(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Do(func(_ context.Context,
			_ *peloton.JobID,
			config *pbjob.JobConfig,
			addOn *models.ConfigAddOn,
			_ *stateless.JobSpec,
			version uint64) {
			suite.Equal(version, uint64(jobmgrcommon.DummyConfigVersion))
			suite.Equal(config.ChangeLog.Version, uint64(jobmgrcommon.DummyConfigVersion))
			suite.Zero(config.InstanceCount)
			suite.Equal(config.Type, jobConfig.Type)
			suite.Len(addOn.SystemLabels, len(configAddOn.SystemLabels))
			for i := 0; i < len(configAddOn.SystemLabels); i++ {
				suite.Equal(configAddOn.SystemLabels[i].GetKey(), addOn.SystemLabels[i].GetKey())
				suite.Equal(configAddOn.SystemLabels[i].GetValue(), addOn.SystemLabels[i].GetValue())
			}
			suite.Nil(config.DefaultConfig)
			suite.Nil(config.InstanceConfig)
		}).
		Return(nil)

	suite.jobConfigOps.EXPECT().
		Create(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Do(func(_ context.Context,
			_ *peloton.JobID,
			config *pbjob.JobConfig,
			addOn *models.ConfigAddOn,
			_ *stateless.JobSpec,
			version uint64) {
			suite.Equal(version, uint64(1))
			suite.Equal(config.ChangeLog.Version, uint64(1))
			suite.Equal(config.InstanceCount, jobConfig.InstanceCount)
			suite.Equal(config.Type, jobConfig.Type)
			suite.Len(addOn.SystemLabels, len(configAddOn.SystemLabels))
			for i := 0; i < len(configAddOn.SystemLabels); i++ {
				suite.Equal(configAddOn.SystemLabels[i].GetKey(), addOn.SystemLabels[i].GetKey())
				suite.Equal(configAddOn.SystemLabels[i].GetValue(), addOn.SystemLabels[i].GetValue())
			}
		}).
		Return(nil)

	suite.updateStore.EXPECT().
		AddWorkflowEvent(
			gomock.Any(), gomock.Any(),
			gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).
		Times(int(jobConfig.InstanceCount))

	suite.jobUpdateEventsOps.EXPECT().
		Create(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Return(nil)

	suite.updateStore.EXPECT().
		CreateUpdate(gomock.Any(), gomock.Any()).
		Return(nil)

	suite.jobRuntimeOps.EXPECT().
		Upsert(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.GetState(), pbjob.JobState_PENDING)
		}).Return(nil)
	suite.jobIndexOps.EXPECT().
		Create(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	err := suite.job.RollingCreate(
		context.Background(),
		jobConfig,
		configAddOn,
		nil,
		nil,
		nil,
	)
	suite.NoError(err)
	config, err := suite.job.GetConfig(context.Background())
	suite.NoError(err)
	suite.Equal(config.GetInstanceCount(), uint32(10))
	suite.Equal(config.GetChangeLog().Version, uint64(1))
	suite.Equal(config.GetType(), pbjob.JobType_SERVICE)
	runtime, err := suite.job.GetRuntime(context.Background())
	suite.NoError(err)
	suite.Equal(runtime.GetRevision().GetVersion(), uint64(1))
	suite.Equal(runtime.GetConfigurationVersion(), uint64(1))
	suite.Equal(runtime.GetState(), pbjob.JobState_PENDING)
	suite.Equal(runtime.GetGoalState(), pbjob.JobState_RUNNING)
	suite.checkListeners()
}

// TestJobRollingCreateNilConfigFailure tests job
// rolling create in cache and db fails due to nil
// config
func (suite *jobTestSuite) TestJobRollingCreateNilConfigFailure() {
	createdBy := "test"
	configAddOn := &models.ConfigAddOn{
		SystemLabels: []*peloton.Label{
			{
				Key: fmt.Sprintf(
					common.SystemLabelKeyTemplate,
					common.SystemLabelPrefix,
					common.SystemLabelJobOwner),
				Value: createdBy,
			},
		},
	}

	err := suite.job.RollingCreate(
		context.Background(),
		nil,
		configAddOn,
		nil,
		nil,
		nil,
	)
	suite.Error(err)
}

// TestJobRollingCreateAddActiveJobFailure tests job
// rolling create in cache and db fails due to call
// to Create fails
func (suite *jobTestSuite) TestJobRollingCreateAddActiveJobFailure() {
	jobConfig := &pbjob.JobConfig{
		InstanceCount: 10,
		Type:          pbjob.JobType_SERVICE,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
	}

	createdBy := "test"
	configAddOn := &models.ConfigAddOn{
		SystemLabels: []*peloton.Label{
			{
				Key: fmt.Sprintf(
					common.SystemLabelKeyTemplate,
					common.SystemLabelPrefix,
					common.SystemLabelJobOwner),
				Value: createdBy,
			},
			{
				Key: fmt.Sprintf(
					common.SystemLabelKeyTemplate,
					common.SystemLabelPrefix,
					common.SystemLabelJobType),
				Value: jobConfig.Type.String(),
			},
		},
	}

	suite.activeJobsOps.EXPECT().
		Create(gomock.Any(), suite.jobID).
		Return(yarpcerrors.InternalErrorf("test error"))

	err := suite.job.RollingCreate(
		context.Background(),
		jobConfig,
		configAddOn,
		nil,
		nil,
		nil,
	)
	suite.Error(err)
}

// TestJobRollingCreateJobRuntimeFailure
// tests job rolling create in cache and db fails due to
// job_runtime create fails
func (suite *jobTestSuite) TestJobRollingCreateJobRuntimeFailure() {
	jobConfig := &pbjob.JobConfig{
		InstanceCount: 10,
		Type:          pbjob.JobType_SERVICE,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
	}

	createdBy := "test"
	configAddOn := &models.ConfigAddOn{
		SystemLabels: []*peloton.Label{
			{
				Key: fmt.Sprintf(
					common.SystemLabelKeyTemplate,
					common.SystemLabelPrefix,
					common.SystemLabelJobOwner),
				Value: createdBy,
			},
			{
				Key: fmt.Sprintf(
					common.SystemLabelKeyTemplate,
					common.SystemLabelPrefix,
					common.SystemLabelJobType),
				Value: jobConfig.Type.String(),
			},
		},
	}

	suite.activeJobsOps.EXPECT().
		Create(gomock.Any(), suite.jobID).Return(nil)

	suite.jobRuntimeOps.EXPECT().
		Upsert(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, initialRuntime *pbjob.RuntimeInfo) {
			suite.Equal(initialRuntime.State, pbjob.JobState_UNINITIALIZED)
			suite.Equal(initialRuntime.GoalState, pbjob.JobState_RUNNING)
			suite.Equal(initialRuntime.Revision.Version, uint64(1))
			suite.Equal(initialRuntime.ConfigurationVersion, uint64(1))
			suite.Equal(initialRuntime.TaskStats[pbjob.JobState_INITIALIZED.String()], jobConfig.InstanceCount)
			suite.Equal(len(initialRuntime.TaskStats), 1)
		}).
		Return(yarpcerrors.InternalErrorf("test error"))

	err := suite.job.RollingCreate(
		context.Background(),
		jobConfig,
		configAddOn,
		nil,
		nil,
		nil,
	)
	suite.Error(err)
}

// TestGetTaskSpreadCounts tests JobSpreadCounts returned by GetTaskStateCounts
func (suite *jobTestSuite) TestGetTaskSpreadCounts() {
	testcases := []struct {
		isSpread                                bool
		numTasksTotal, numTasksPlaced, numHosts uint32
		message                                 string
	}{
		{
			message:  "Not spread",
			isSpread: false,
		},
		{
			isSpread:      true,
			numTasksTotal: 10,
			message:       "Spread, no runtime",
		},
		{
			message:        "Spread, with runtime",
			isSpread:       true,
			numTasksTotal:  10,
			numTasksPlaced: 5,
			numHosts:       4,
		},
	}
	for _, tc := range testcases {
		if tc.isSpread == true {
			suite.job.config.placementStrategy =
				pbjob.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD_JOB
		}
		var tasks []*task
		for i := uint32(0); i < tc.numTasksTotal; i++ {
			tasks = append(tasks, suite.job.addTaskToJobMap(i))
		}
		if tc.numHosts > 0 {
			taskRuntimes := initializeRuntimes(
				tc.numTasksTotal,
				pbtask.TaskState_LAUNCHED)
			for i, r := range taskRuntimes {
				tasks[i].runtime = r
				if i < tc.numTasksPlaced {
					r.Host = "host-" + fmt.Sprintf("%d", i%tc.numHosts)
				}
			}
		}
		_, _, spread := suite.job.GetTaskStateCount()
		suite.Equal(int(tc.numTasksPlaced), spread.taskCount)
		suite.Equal(int(tc.numHosts), spread.hostCount)
	}
}

// TestGetCachedConfig tests getting the config for
// the job when the job is present in the cache
func (suite *jobTestSuite) TestGetCachedConfig() {
	cachedConfig := suite.job.GetCachedConfig()
	suite.Equal(
		suite.job.config.GetSLA().GetMaximumUnavailableInstances(),
		cachedConfig.GetSLA().GetMaximumUnavailableInstances(),
	)
	suite.Equal(
		suite.job.config.GetSLA().GetRevocable(),
		cachedConfig.GetSLA().GetRevocable(),
	)
	suite.Equal(
		suite.job.config.GetSLA().GetPreemptible(),
		cachedConfig.GetSLA().GetPreemptible(),
	)
	suite.Equal(
		suite.job.config.GetSLA().GetMaximumRunningInstances(),
		cachedConfig.GetSLA().GetMaximumRunningInstances(),
	)
	suite.Equal(
		suite.job.config.GetSLA().GetMaxRunningTime(),
		cachedConfig.GetSLA().GetMaxRunningTime(),
	)
	suite.Equal(
		suite.job.config.GetSLA().GetPriority(),
		cachedConfig.GetSLA().GetPriority(),
	)
	suite.Equal(suite.job.config.GetRespoolID(), cachedConfig.GetRespoolID())
	suite.Equal(suite.job.config.GetInstanceCount(), cachedConfig.GetInstanceCount())
	suite.Equal(suite.job.config.GetType(), cachedConfig.GetType())
	suite.Equal(suite.job.config.GetName(), cachedConfig.GetName())
	suite.Equal(suite.job.config.GetLabels(), cachedConfig.GetLabels())
	suite.Equal(suite.job.config.GetChangeLog(), cachedConfig.GetChangeLog())
}

// TestGetCachedConfigNilConfig tests getting cached
// job config when the job is not in cache
func (suite *jobTestSuite) TestGetCachedConfigNilConfig() {
	suite.job.config = nil
	suite.Nil(suite.job.GetCachedConfig())
}
