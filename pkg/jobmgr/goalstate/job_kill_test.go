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

	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/private/models"

	goalstatemocks "github.com/uber/peloton/pkg/common/goalstate/mocks"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	storemocks "github.com/uber/peloton/pkg/storage/mocks"

	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type JobKillTestSuite struct {
	suite.Suite

	ctrl                *gomock.Controller
	jobStore            *storemocks.MockJobStore
	jobGoalStateEngine  *goalstatemocks.MockEngine
	taskGoalStateEngine *goalstatemocks.MockEngine
	jobFactory          *cachedmocks.MockJobFactory
	cachedJob           *cachedmocks.MockJob
	cachedConfig        *cachedmocks.MockJobConfigCache
	goalStateDriver     *driver
	jobID               *peloton.JobID
	jobEnt              *jobEntity
}

func TestJobKill(t *testing.T) {
	suite.Run(t, new(JobKillTestSuite))
}

func (suite *JobKillTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.jobStore = storemocks.NewMockJobStore(suite.ctrl)
	suite.jobGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.taskGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.cachedConfig = cachedmocks.NewMockJobConfigCache(suite.ctrl)

	suite.goalStateDriver = &driver{
		jobEngine:  suite.jobGoalStateEngine,
		taskEngine: suite.taskGoalStateEngine,
		jobStore:   suite.jobStore,
		jobFactory: suite.jobFactory,
		mtx:        NewMetrics(tally.NoopScope),
		cfg:        &Config{},
	}
	suite.goalStateDriver.cfg.normalize()
	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.jobEnt = &jobEntity{
		id:     suite.jobID,
		driver: suite.goalStateDriver,
	}
}

func (suite *JobKillTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestJobKill tests killing a fully created job
func (suite JobKillTestSuite) TestJobKill() {
	instanceCount := uint32(5)
	stateVersion := uint64(1)
	desiredStateVersion := uint64(2)

	cachedTasks := make(map[uint32]cached.Task)
	mockTasks := make(map[uint32]*cachedmocks.MockTask)
	for i := uint32(0); i < instanceCount; i++ {
		cachedTask := cachedmocks.NewMockTask(suite.ctrl)
		mockTasks[i] = cachedTask
		cachedTasks[i] = cachedTask
	}

	runtimes := make(map[uint32]*pbtask.RuntimeInfo)
	runtimes[0] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_RUNNING,
		GoalState: pbtask.TaskState_SUCCEEDED,
	}
	runtimes[1] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_RUNNING,
		GoalState: pbtask.TaskState_SUCCEEDED,
	}
	runtimes[2] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_INITIALIZED,
		GoalState: pbtask.TaskState_SUCCEEDED,
	}
	runtimes[3] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_INITIALIZED,
		GoalState: pbtask.TaskState_SUCCEEDED,
	}
	runtimes[4] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_FAILED,
		GoalState: pbtask.TaskState_RUNNING,
	}

	termStatus := &pbtask.TerminationStatus{
		Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_ON_REQUEST,
	}
	runtimeDiffs := make(map[uint32]jobmgrcommon.RuntimeDiff)
	runtimeDiffs[0] = map[string]interface{}{
		jobmgrcommon.GoalStateField:         pbtask.TaskState_KILLED,
		jobmgrcommon.MessageField:           "Task stop API request",
		jobmgrcommon.ReasonField:            "",
		jobmgrcommon.TerminationStatusField: termStatus,
		jobmgrcommon.DesiredHostField:       "",
	}
	runtimeDiffs[1] = map[string]interface{}{
		jobmgrcommon.GoalStateField:         pbtask.TaskState_KILLED,
		jobmgrcommon.MessageField:           "Task stop API request",
		jobmgrcommon.ReasonField:            "",
		jobmgrcommon.TerminationStatusField: termStatus,
		jobmgrcommon.DesiredHostField:       "",
	}
	runtimeDiffs[2] = map[string]interface{}{
		jobmgrcommon.GoalStateField:         pbtask.TaskState_KILLED,
		jobmgrcommon.MessageField:           "Task stop API request",
		jobmgrcommon.ReasonField:            "",
		jobmgrcommon.TerminationStatusField: termStatus,
		jobmgrcommon.DesiredHostField:       "",
	}
	runtimeDiffs[3] = map[string]interface{}{
		jobmgrcommon.GoalStateField:         pbtask.TaskState_KILLED,
		jobmgrcommon.MessageField:           "Task stop API request",
		jobmgrcommon.ReasonField:            "",
		jobmgrcommon.TerminationStatusField: termStatus,
		jobmgrcommon.DesiredHostField:       "",
	}
	runtimeDiffs[4] = map[string]interface{}{
		jobmgrcommon.GoalStateField:         pbtask.TaskState_KILLED,
		jobmgrcommon.MessageField:           "Task stop API request",
		jobmgrcommon.ReasonField:            "",
		jobmgrcommon.TerminationStatusField: termStatus,
		jobmgrcommon.DesiredHostField:       "",
	}

	jobRuntime := &pbjob.RuntimeInfo{
		State:               pbjob.JobState_RUNNING,
		GoalState:           pbjob.JobState_SUCCEEDED,
		StateVersion:        stateVersion,
		DesiredStateVersion: desiredStateVersion,
	}

	suite.cachedJob.EXPECT().ID().Return(suite.jobID).AnyTimes()

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
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
			nil,
			cached.UpdateCacheAndDB).
		Do(func(_ context.Context,
			jobInfo *pbjob.JobInfo,
			_ *models.ConfigAddOn,
			_ *stateless.JobSpec,
			_ cached.UpdateRequest) {
			suite.Equal(jobInfo.Runtime.State, pbjob.JobState_KILLING)
			suite.Equal(jobInfo.Runtime.StateVersion, desiredStateVersion)
		}).
		Return(nil)

	for i := uint32(0); i < instanceCount; i++ {
		mockTasks[i].EXPECT().
			GetRuntime(gomock.Any()).
			Return(runtimes[i], nil)
	}

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), runtimeDiffs, false).
		Return(nil, nil, nil)

	suite.cachedJob.EXPECT().
		GetJobType().
		Return(pbjob.JobType_BATCH)

	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return().
		Times(int(instanceCount - 1)) // one of the instance is not in terminal state

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := JobKill(context.Background(), suite.jobEnt)
	suite.NoError(err)
}

// TestJobKillNoJob tests when job is not exist
func (suite JobKillTestSuite) TestJobKillNoJob() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(nil)
	err := JobKill(context.Background(), suite.jobEnt)
	suite.NoError(err)
}

// TestJobKillNoRumtimes tests when task doesn't has runtime
func (suite JobKillTestSuite) TestJobKillNoRumtimes() {
	cachedTasks := make(map[uint32]cached.Task)
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
	cachedTasks[0] = cachedTask

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()
	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			GoalState:           pbjob.JobState_KILLED,
			StateVersion:        1,
			DesiredStateVersion: 2,
		}, nil)
	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()
	suite.cachedJob.EXPECT().
		GetAllTasks().
		Return(cachedTasks)
	cachedTask.EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, errors.New(""))
	err := JobKill(context.Background(), suite.jobEnt)
	suite.Error(err)
}

// TestJobKillPatchFailed tests failure case when patching the runtime
func (suite JobKillTestSuite) TestJobKillPatchFailed() {
	cachedTasks := make(map[uint32]cached.Task)
	cachedTask0 := cachedmocks.NewMockTask(suite.ctrl)
	cachedTasks[0] = cachedTask0
	cachedTask1 := cachedmocks.NewMockTask(suite.ctrl)
	cachedTasks[1] = cachedTask1
	runtimes := make(map[uint32]*pbtask.RuntimeInfo)
	runtimes[0] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_SUCCEEDED,
		GoalState: pbtask.TaskState_SUCCEEDED,
	}
	runtimes[1] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_RUNNING,
		GoalState: pbtask.TaskState_SUCCEEDED,
	}

	suite.cachedJob.EXPECT().ID().Return(suite.jobID).AnyTimes()

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).AnyTimes()
	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			GoalState:           pbjob.JobState_KILLED,
			StateVersion:        1,
			DesiredStateVersion: 2,
		}, nil)
	suite.cachedJob.EXPECT().
		GetAllTasks().
		Return(cachedTasks)
	cachedTask0.EXPECT().
		GetRuntime(gomock.Any()).
		Return(runtimes[0], nil)
	cachedTask1.EXPECT().
		GetRuntime(gomock.Any()).
		Return(runtimes[1], nil)
	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, nil, errors.New(""))

	err := JobKill(context.Background(), suite.jobEnt)
	suite.Error(err)
}

// TestJobKillPatchTasksRetryInstances tests case when patching
// the runtime needs to be retried for a few tasks
func (suite JobKillTestSuite) TestJobKillPatchTasksRetryInstances() {
	instanceCount := uint32(5)
	stateVersion := uint64(1)
	desiredStateVersion := uint64(2)

	cachedTasks := make(map[uint32]cached.Task)
	mockTasks := make(map[uint32]*cachedmocks.MockTask)
	for i := uint32(0); i < instanceCount; i++ {
		cachedTask := cachedmocks.NewMockTask(suite.ctrl)
		mockTasks[i] = cachedTask
		cachedTasks[i] = cachedTask
	}

	runtimes := make(map[uint32]*pbtask.RuntimeInfo)
	runtimes[0] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_RUNNING,
		GoalState: pbtask.TaskState_SUCCEEDED,
	}
	runtimes[1] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_RUNNING,
		GoalState: pbtask.TaskState_SUCCEEDED,
	}
	runtimes[2] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_INITIALIZED,
		GoalState: pbtask.TaskState_SUCCEEDED,
	}
	runtimes[3] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_INITIALIZED,
		GoalState: pbtask.TaskState_SUCCEEDED,
	}
	runtimes[4] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_FAILED,
		GoalState: pbtask.TaskState_RUNNING,
	}

	termStatus := &pbtask.TerminationStatus{
		Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_ON_REQUEST,
	}
	runtimeDiffs := make(map[uint32]jobmgrcommon.RuntimeDiff)
	runtimeDiffs[0] = map[string]interface{}{
		jobmgrcommon.GoalStateField:         pbtask.TaskState_KILLED,
		jobmgrcommon.MessageField:           "Task stop API request",
		jobmgrcommon.ReasonField:            "",
		jobmgrcommon.TerminationStatusField: termStatus,
		jobmgrcommon.DesiredHostField:       "",
	}
	runtimeDiffs[1] = map[string]interface{}{
		jobmgrcommon.GoalStateField:         pbtask.TaskState_KILLED,
		jobmgrcommon.MessageField:           "Task stop API request",
		jobmgrcommon.ReasonField:            "",
		jobmgrcommon.TerminationStatusField: termStatus,
		jobmgrcommon.DesiredHostField:       "",
	}
	runtimeDiffs[2] = map[string]interface{}{
		jobmgrcommon.GoalStateField:         pbtask.TaskState_KILLED,
		jobmgrcommon.MessageField:           "Task stop API request",
		jobmgrcommon.ReasonField:            "",
		jobmgrcommon.TerminationStatusField: termStatus,
		jobmgrcommon.DesiredHostField:       "",
	}
	runtimeDiffs[3] = map[string]interface{}{
		jobmgrcommon.GoalStateField:         pbtask.TaskState_KILLED,
		jobmgrcommon.MessageField:           "Task stop API request",
		jobmgrcommon.ReasonField:            "",
		jobmgrcommon.TerminationStatusField: termStatus,
		jobmgrcommon.DesiredHostField:       "",
	}
	runtimeDiffs[4] = map[string]interface{}{
		jobmgrcommon.GoalStateField:         pbtask.TaskState_KILLED,
		jobmgrcommon.MessageField:           "Task stop API request",
		jobmgrcommon.ReasonField:            "",
		jobmgrcommon.TerminationStatusField: termStatus,
		jobmgrcommon.DesiredHostField:       "",
	}

	jobRuntime := &pbjob.RuntimeInfo{
		State:               pbjob.JobState_RUNNING,
		GoalState:           pbjob.JobState_SUCCEEDED,
		StateVersion:        stateVersion,
		DesiredStateVersion: desiredStateVersion,
	}

	suite.cachedJob.EXPECT().ID().Return(suite.jobID).AnyTimes()

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedJob.EXPECT().
		GetAllTasks().
		Return(cachedTasks).
		AnyTimes()

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	for i := uint32(0); i < instanceCount; i++ {
		mockTasks[i].EXPECT().
			GetRuntime(gomock.Any()).
			Return(runtimes[i], nil)
	}

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), runtimeDiffs, false).
		Return(nil, []uint32{0}, nil)

	suite.cachedJob.EXPECT().
		GetJobType().
		Return(pbjob.JobType_BATCH)

	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return().
		Times(int(instanceCount - 1)) // one of the instance is not in terminal state

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := JobKill(context.Background(), suite.jobEnt)
	suite.NoError(err)
}

// TestJobKillNoRumtimes tests when job doesn't has runtime
func (suite JobKillTestSuite) TestJobKillNoJobRuntime() {
	cachedTasks := make(map[uint32]cached.Task)
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
	cachedTasks[0] = cachedTask
	runtimes := make(map[uint32]*pbtask.RuntimeInfo)
	runtimes[0] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_SUCCEEDED,
		GoalState: pbtask.TaskState_KILLED,
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).AnyTimes()

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, errors.New(""))

	err := JobKill(context.Background(), suite.jobEnt)
	suite.Error(err)
}

// TestJobKillUpdateRuntimeFails tests killing a fully created job
// fails due to job runtime update failure
func (suite JobKillTestSuite) TestJobKillUpdateRuntimeFails() {
	instanceCount := uint32(5)
	stateVersion := uint64(1)
	desiredStateVersion := uint64(2)

	cachedTasks := make(map[uint32]cached.Task)
	mockTasks := make(map[uint32]*cachedmocks.MockTask)
	for i := uint32(0); i < instanceCount; i++ {
		cachedTask := cachedmocks.NewMockTask(suite.ctrl)
		mockTasks[i] = cachedTask
		cachedTasks[i] = cachedTask
	}

	runtimes := make(map[uint32]*pbtask.RuntimeInfo)
	runtimes[0] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_RUNNING,
		GoalState: pbtask.TaskState_SUCCEEDED,
	}
	runtimes[1] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_RUNNING,
		GoalState: pbtask.TaskState_SUCCEEDED,
	}
	runtimes[2] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_INITIALIZED,
		GoalState: pbtask.TaskState_SUCCEEDED,
	}
	runtimes[3] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_INITIALIZED,
		GoalState: pbtask.TaskState_SUCCEEDED,
	}
	runtimes[4] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_FAILED,
		GoalState: pbtask.TaskState_RUNNING,
	}

	termStatus := &pbtask.TerminationStatus{
		Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_ON_REQUEST,
	}
	runtimeDiffs := make(map[uint32]jobmgrcommon.RuntimeDiff)
	runtimeDiffs[0] = map[string]interface{}{
		jobmgrcommon.GoalStateField:         pbtask.TaskState_KILLED,
		jobmgrcommon.MessageField:           "Task stop API request",
		jobmgrcommon.ReasonField:            "",
		jobmgrcommon.TerminationStatusField: termStatus,
		jobmgrcommon.DesiredHostField:       "",
	}
	runtimeDiffs[1] = map[string]interface{}{
		jobmgrcommon.GoalStateField:         pbtask.TaskState_KILLED,
		jobmgrcommon.MessageField:           "Task stop API request",
		jobmgrcommon.ReasonField:            "",
		jobmgrcommon.TerminationStatusField: termStatus,
		jobmgrcommon.DesiredHostField:       "",
	}
	runtimeDiffs[2] = map[string]interface{}{
		jobmgrcommon.GoalStateField:         pbtask.TaskState_KILLED,
		jobmgrcommon.MessageField:           "Task stop API request",
		jobmgrcommon.ReasonField:            "",
		jobmgrcommon.TerminationStatusField: termStatus,
		jobmgrcommon.DesiredHostField:       "",
	}
	runtimeDiffs[3] = map[string]interface{}{
		jobmgrcommon.GoalStateField:         pbtask.TaskState_KILLED,
		jobmgrcommon.MessageField:           "Task stop API request",
		jobmgrcommon.ReasonField:            "",
		jobmgrcommon.TerminationStatusField: termStatus,
		jobmgrcommon.DesiredHostField:       "",
	}
	runtimeDiffs[4] = map[string]interface{}{
		jobmgrcommon.GoalStateField:         pbtask.TaskState_KILLED,
		jobmgrcommon.MessageField:           "Task stop API request",
		jobmgrcommon.ReasonField:            "",
		jobmgrcommon.TerminationStatusField: termStatus,
		jobmgrcommon.DesiredHostField:       "",
	}

	jobRuntime := &pbjob.RuntimeInfo{
		State:               pbjob.JobState_RUNNING,
		GoalState:           pbjob.JobState_SUCCEEDED,
		StateVersion:        stateVersion,
		DesiredStateVersion: desiredStateVersion,
	}

	suite.cachedJob.EXPECT().ID().Return(suite.jobID).AnyTimes()

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedJob.EXPECT().
		GetAllTasks().
		Return(cachedTasks).
		AnyTimes()

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	for i := uint32(0); i < instanceCount; i++ {
		mockTasks[i].EXPECT().
			GetRuntime(gomock.Any()).
			Return(runtimes[i], nil)
	}

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), runtimeDiffs, false).
		Return(nil, nil, nil)

	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return().
		Times(int(instanceCount - 1)) // one of the instance is not in terminal state

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
			suite.Equal(jobInfo.Runtime.State, pbjob.JobState_KILLING)
			suite.Equal(jobInfo.Runtime.StateVersion, desiredStateVersion)
		}).
		Return(errors.New("test error"))

	err := JobKill(context.Background(), suite.jobEnt)
	suite.Error(err)
}

// Tests to delete a partially create stateless job if current state is pending
func (suite JobKillTestSuite) TestJobKillPartiallyCreated_StatelessJob() {
	cachedTasks := make(map[uint32]cached.Task)
	mockTasks := make(map[uint32]*cachedmocks.MockTask)
	for i := uint32(0); i < 1; i++ {
		cachedTask := cachedmocks.NewMockTask(suite.ctrl)
		mockTasks[i] = cachedTask
		cachedTasks[i] = cachedTask
	}

	runtimes := make(map[uint32]*pbtask.RuntimeInfo)
	runtimes[0] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_KILLED,
		GoalState: pbtask.TaskState_KILLED,
	}

	jobRuntime := &pbjob.RuntimeInfo{
		State:     pbjob.JobState_PENDING,
		GoalState: pbjob.JobState_DELETED,
	}

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return().
		AnyTimes()

	suite.cachedJob.EXPECT().
		GetJobType().
		Return(pbjob.JobType_SERVICE).
		AnyTimes()

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
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
			nil,
			cached.UpdateCacheAndDB).
		Do(func(_ context.Context,
			jobInfo *pbjob.JobInfo,
			_ *models.ConfigAddOn,
			_ *stateless.JobSpec,
			_ cached.UpdateRequest) {
			suite.Equal(jobInfo.Runtime.State, pbjob.JobState_KILLED)
		}).
		Return(nil)

	for i := uint32(0); i < 1; i++ {
		mockTasks[i].EXPECT().
			GetRuntime(gomock.Any()).
			Return(runtimes[i], nil).
			AnyTimes()
	}

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, nil, nil)

	err := JobKill(context.Background(), suite.jobEnt)
	suite.NoError(err)
}

// TestJobKillPartiallyCreatedJob tests killing partially created jobs
func (suite JobKillTestSuite) TestJobKillPartiallyCreatedJob() {
	cachedTasks := make(map[uint32]cached.Task)
	mockTasks := make(map[uint32]*cachedmocks.MockTask)
	for i := uint32(2); i < 4; i++ {
		cachedTask := cachedmocks.NewMockTask(suite.ctrl)
		mockTasks[i] = cachedTask
		cachedTasks[i] = cachedTask
	}

	runtimes := make(map[uint32]*pbtask.RuntimeInfo)
	runtimes[2] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_SUCCEEDED,
		GoalState: pbtask.TaskState_KILLED,
	}
	runtimes[3] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_KILLED,
		GoalState: pbtask.TaskState_KILLED,
	}
	jobRuntime := &pbjob.RuntimeInfo{
		State:     pbjob.JobState_INITIALIZED,
		GoalState: pbjob.JobState_KILLED,
	}

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return().
		AnyTimes()

	suite.cachedJob.EXPECT().
		GetJobType().
		Return(pbjob.JobType_BATCH).
		AnyTimes()

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
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
			nil,
			cached.UpdateCacheAndDB).
		Do(func(_ context.Context,
			jobInfo *pbjob.JobInfo,
			_ *models.ConfigAddOn,
			_ *stateless.JobSpec,
			_ cached.UpdateRequest) {
			suite.Equal(jobInfo.Runtime.State, pbjob.JobState_KILLED)
		}).
		Return(nil)

	for i := uint32(2); i < 4; i++ {
		mockTasks[i].EXPECT().
			GetRuntime(gomock.Any()).
			Return(runtimes[i], nil).Times(2)
	}

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, nil, nil)

	err := JobKill(context.Background(), suite.jobEnt)
	suite.NoError(err)

	runtimes[2] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_RUNNING,
		GoalState: pbtask.TaskState_KILLED,
	}
	jobRuntime.State = pbjob.JobState_INITIALIZED

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).AnyTimes()

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

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
			suite.Equal(jobInfo.Runtime.State, pbjob.JobState_KILLING)
		}).
		Return(nil)

	for i := uint32(2); i < 4; i++ {
		mockTasks[i].EXPECT().
			GetRuntime(gomock.Any()).
			Return(runtimes[i], nil).AnyTimes()
	}

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, nil, nil)

	err = JobKill(context.Background(), suite.jobEnt)
	suite.NoError(err)
}

// TestJobKillPartiallyCreatedJob_AllTerminated tests killing partially created jobs,
// where all the tasks are in terminal state.
func (suite JobKillTestSuite) TestJobKillPartiallyCreatedJob_AllTerminated() {
	cachedTasks := make(map[uint32]cached.Task)
	mockTasks := make(map[uint32]*cachedmocks.MockTask)
	var instanceCount uint32 = 5
	for i := uint32(2); i < instanceCount; i++ {
		cachedTask := cachedmocks.NewMockTask(suite.ctrl)
		mockTasks[i] = cachedTask
		cachedTasks[i] = cachedTask
	}

	runtimes := make(map[uint32]*pbtask.RuntimeInfo)
	runtimes[2] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_SUCCEEDED,
		GoalState: pbtask.TaskState_SUCCEEDED,
	}
	runtimes[3] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_FAILED,
		GoalState: pbtask.TaskState_RUNNING,
	}
	runtimes[4] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_KILLED,
		GoalState: pbtask.TaskState_KILLED,
	}
	jobRuntime := &pbjob.RuntimeInfo{
		State:     pbjob.JobState_INITIALIZED,
		GoalState: pbjob.JobState_KILLED,
	}

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return().
		AnyTimes()

	suite.cachedJob.EXPECT().
		GetJobType().
		Return(pbjob.JobType_BATCH).
		AnyTimes()

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).AnyTimes()

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
			nil,
			cached.UpdateCacheAndDB).
		Do(func(_ context.Context,
			jobInfo *pbjob.JobInfo,
			_ *models.ConfigAddOn,
			_ *stateless.JobSpec,
			_ cached.UpdateRequest) {
			suite.NotEmpty(jobInfo.GetRuntime().GetCompletionTime())
			suite.Equal(jobInfo.Runtime.State, pbjob.JobState_KILLED)
		}).
		Return(nil)

	for i := uint32(2); i < instanceCount; i++ {
		mockTasks[i].EXPECT().
			GetRuntime(gomock.Any()).
			Return(runtimes[i], nil).Times(2)
	}

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, nil, nil)

	err := JobKill(context.Background(), suite.jobEnt)
	suite.NoError(err)
}
