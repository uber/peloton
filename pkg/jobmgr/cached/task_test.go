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
	"sort"
	"testing"
	"time"

	mesosv1 "github.com/uber/peloton/.gen/mesos/v1"
	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"

	storemocks "github.com/uber/peloton/pkg/storage/mocks"

	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

func initializeTaskRuntime(state pbtask.TaskState, version uint64) *pbtask.RuntimeInfo {
	runtime := &pbtask.RuntimeInfo{
		State: state,
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   version,
		},
	}
	return runtime
}

func initializeLabel(key string, value string) *peloton.Label {
	return &peloton.Label{
		Key:   key,
		Value: value,
	}
}

type TaskTestSuite struct {
	suite.Suite

	ctrl       *gomock.Controller
	jobID      *peloton.JobID
	instanceID uint32
	taskStore  *storemocks.MockTaskStore
	listeners  []*FakeTaskListener

	testTaskScope tally.TestScope
}

func (suite *TaskTestSuite) SetupTest() {
	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}

	suite.instanceID = uint32(1)

	suite.ctrl = gomock.NewController(suite.T())
	suite.taskStore = storemocks.NewMockTaskStore(suite.ctrl)
	suite.listeners = append(suite.listeners,
		new(FakeTaskListener),
		new(FakeTaskListener))

	suite.testTaskScope = tally.NewTestScope("", nil)
}

func (suite *TaskTestSuite) TearDownTest() {
	suite.listeners = nil
	suite.ctrl.Finish()
}

func TestTask(t *testing.T) {
	suite.Run(t, new(TaskTestSuite))
}

// initializeTask initializes a test task to be used in the unit test
func (suite *TaskTestSuite) initializeTask(
	taskStore *storemocks.MockTaskStore,
	jobID *peloton.JobID, instanceID uint32,
	runtime *pbtask.RuntimeInfo) *task {
	tt := &task{
		id:      instanceID,
		jobID:   jobID,
		runtime: runtime,
		jobFactory: &jobFactory{
			mtx:         NewMetrics(tally.NoopScope),
			taskMetrics: NewTaskMetrics(suite.testTaskScope),
			taskStore:   taskStore,
			running:     true,
			jobs:        map[string]*job{},
		},
		jobType: pbjob.JobType_BATCH,
	}
	for _, l := range suite.listeners {
		tt.jobFactory.listeners = append(tt.jobFactory.listeners, l)
	}
	config := &cachedConfig{
		jobType:   pbjob.JobType_BATCH,
		changeLog: &peloton.ChangeLog{Version: 1},
	}
	job := &job{
		id:     jobID,
		config: config,
		runtime: &pbjob.RuntimeInfo{
			ConfigurationVersion: config.changeLog.Version},
	}
	tt.jobFactory.jobs[jobID.GetValue()] = job
	return tt
}

// checkListeners verifies that listeners received the correct data
func (suite *TaskTestSuite) checkListeners(tt *task, jobType pbjob.JobType) {
	suite.NotZero(len(suite.listeners))
	for i, l := range suite.listeners {
		msg := fmt.Sprintf("Listener %d", i)
		suite.Equal(suite.jobID, l.jobID, msg)
		suite.Equal(suite.instanceID, l.instanceID, msg)
		suite.Equal(jobType, l.jobType, msg)
		suite.Equal(tt.runtime, l.taskRuntime, msg)
	}
}

// checkListenersNotCalled verifies that listeners did not get invoked
func (suite *TaskTestSuite) checkListenersNotCalled() {
	suite.NotZero(len(suite.listeners))
	for i, l := range suite.listeners {
		msg := fmt.Sprintf("Listener %d", i)
		suite.Nil(l.jobID, msg)
		suite.Nil(l.taskRuntime, msg)
	}
}

// TestTaskCreateRuntime tests creating task runtime without any DB errors
func (suite *TaskTestSuite) TestCreateRuntime() {
	tt := suite.initializeTask(suite.taskStore, suite.jobID,
		suite.instanceID, nil)
	version := uint64(3)
	runtime := initializeTaskRuntime(pbtask.TaskState_INITIALIZED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	runtime.ConfigVersion = version

	suite.taskStore.EXPECT().
		CreateTaskRuntime(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
			runtime,
			gomock.Any(),
			tt.jobType).
		Return(nil)

	suite.taskStore.EXPECT().
		GetTaskConfig(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
			version).
		Return(nil, nil, nil)

	err := tt.CreateTask(context.Background(), runtime, "team10")
	suite.Nil(err)
	suite.False(tt.initializedAt.IsZero())
	suite.checkListeners(tt, tt.jobType)
}

// TestTaskPatchTask tests updating the task runtime without any DB errors
func (suite *TaskTestSuite) TestPatchTask() {
	var labels []*peloton.Label

	version := uint64(3)
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	runtime.ConfigVersion = version - 1
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		runtime)
	labels = append(labels, initializeLabel("key", "value"))
	taskConfig := &pbtask.TaskConfig{
		Labels: labels,
	}

	diff := jobmgrcommon.RuntimeDiff{
		jobmgrcommon.StateField:         pbtask.TaskState_RUNNING,
		jobmgrcommon.ConfigVersionField: version,
	}

	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
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
			suite.Equal(tt.jobType, jobType)
		}).
		Return(nil)

	suite.taskStore.EXPECT().
		GetTaskConfig(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
			version).
		Return(taskConfig, nil, nil)

	err := tt.PatchTask(context.Background(), diff)
	suite.Nil(err)
	suite.checkListeners(tt, tt.jobType)
}

// TestTaskPatchTask tests updating the task runtime without any DB errors
func (suite *TaskTestSuite) TestPatchTask_WithInitializedState() {
	var labels []*peloton.Label

	labels = append(labels, initializeLabel("key", "value"))
	runtime := initializeTaskRuntime(pbtask.TaskState_INITIALIZED, 2)
	currentMesosTaskID := "acf6e6d4-51be-4b60-8900-683f11252848" + "-1-1"
	runtime.MesosTaskId = &mesosv1.TaskID{
		Value: &currentMesosTaskID,
	}
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		runtime)
	tt.config = &taskConfigCache{
		labels: labels,
	}

	mesosTaskID := "acf6e6d4-51be-4b60-8900-683f11252848" + "-1-2"
	diff := jobmgrcommon.RuntimeDiff{
		jobmgrcommon.StateField: pbtask.TaskState_INITIALIZED,
		jobmgrcommon.MesosTaskIDField: &mesosv1.TaskID{
			Value: &mesosTaskID,
		},
	}

	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
			gomock.Any(),
			gomock.Any()).
		Do(func(
			ctx context.Context,
			jobID *peloton.JobID,
			instanceID uint32,
			runtime *pbtask.RuntimeInfo,
			jobType pbjob.JobType) {
			suite.Equal(runtime.GetState(), pbtask.TaskState_INITIALIZED)
			suite.Equal(uint64(runtime.Revision.Version), uint64(3))
			suite.Equal(runtime.GetGoalState(), pbtask.TaskState_SUCCEEDED)
		}).
		Return(nil)

	oldTime := time.Now()
	err := tt.PatchTask(context.Background(), diff)
	suite.Nil(err)
	suite.False(tt.initializedAt.IsZero())
	suite.NotEqual(oldTime, tt.initializedAt)
	suite.checkListeners(tt, pbjob.JobType_BATCH)
}

// TestPatchTask_KillInitializedTask tests updating the case of
// trying to kill initialized task
func (suite *TaskTestSuite) TestPatchTask_KillInitializedTask() {
	var labels []*peloton.Label

	labels = append(labels, initializeLabel("key", "value"))
	runtime := initializeTaskRuntime(pbtask.TaskState_INITIALIZED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		runtime)
	tt.config = &taskConfigCache{
		labels: labels,
	}

	diff := jobmgrcommon.RuntimeDiff{
		jobmgrcommon.GoalStateField: pbtask.TaskState_KILLED,
	}

	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
			gomock.Any(),
			gomock.Any()).
		Do(func(
			ctx context.Context,
			jobID *peloton.JobID,
			instanceID uint32,
			runtime *pbtask.RuntimeInfo,
			jobType pbjob.JobType) {
			suite.Equal(runtime.GetState(), pbtask.TaskState_INITIALIZED)
			suite.Equal(runtime.Revision.Version, uint64(3))
			suite.Equal(runtime.GetGoalState(), pbtask.TaskState_KILLED)
		}).
		Return(nil)

	err := tt.PatchTask(context.Background(), diff)
	suite.Nil(err)
	suite.checkListeners(tt, pbjob.JobType_BATCH)
}

// TestTaskPatchTask_NoRuntimeInCache tests updating task runtime when
// the runtime in cache is nil
func (suite *TaskTestSuite) TestPatchTask_NoRuntimeInCache() {
	var labels []*peloton.Label

	labels = append(labels, initializeLabel("key", "value"))
	version := uint64(3)
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	runtime.ConfigVersion = version
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		nil)
	taskConfig := &pbtask.TaskConfig{
		Labels: labels,
	}

	diff := jobmgrcommon.RuntimeDiff{
		jobmgrcommon.StateField: pbtask.TaskState_RUNNING,
	}

	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, suite.instanceID).Return(runtime, nil)

	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
			gomock.Any(),
			gomock.Any()).
		Do(func(ctx context.Context,
			jobID *peloton.JobID,
			instanceID uint32,
			runtime *pbtask.RuntimeInfo,
			jobType pbjob.JobType) {
			suite.Equal(runtime.GetState(), pbtask.TaskState_RUNNING)
			suite.Equal(runtime.Revision.Version, uint64(3))
			suite.Equal(runtime.GetGoalState(), pbtask.TaskState_SUCCEEDED)
		}).
		Return(nil)

	suite.taskStore.EXPECT().
		GetTaskConfig(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
			version).
		Return(taskConfig, nil, nil)

	err := tt.PatchTask(context.Background(), diff)
	suite.Nil(err)
	suite.checkListeners(tt, pbjob.JobType_BATCH)
}

// TestPatchTask_DBError tests updating the task runtime with DB errors
func (suite *TaskTestSuite) TestPatchTask_DBError() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		runtime)

	diff := jobmgrcommon.RuntimeDiff{
		jobmgrcommon.StateField: pbtask.TaskState_RUNNING,
	}

	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
			gomock.Any(),
			gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err := tt.PatchTask(context.Background(), diff)
	suite.NotNil(err)
	suite.checkListenersNotCalled()
}

// TestPatchTaskGetConfigDBError tests updating the task runtime
// with a DB error when getting the task config
func (suite *TaskTestSuite) TestPatchTaskGetConfigDBError() {
	version := uint64(3)
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	runtime.ConfigVersion = version - 1
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		runtime)

	diff := jobmgrcommon.RuntimeDiff{
		jobmgrcommon.StateField:         pbtask.TaskState_RUNNING,
		jobmgrcommon.ConfigVersionField: version,
	}

	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
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
			suite.Equal(tt.jobType, jobType)
		}).
		Return(nil)

	suite.taskStore.EXPECT().
		GetTaskConfig(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
			version).
		Return(nil, nil, fmt.Errorf("fake db error"))

	err := tt.PatchTask(context.Background(), diff)
	suite.NotNil(err)
	suite.checkListenersNotCalled()
}

func (suite *TaskTestSuite) TestCompareAndSetNilRuntime() {
	var labels []*peloton.Label

	labels = append(labels, initializeLabel("key", "value"))
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	tt := suite.initializeTask(suite.taskStore, suite.jobID,
		suite.instanceID, runtime)
	tt.config = &taskConfigCache{
		labels: labels,
	}

	_, err := tt.CompareAndSetTask(
		context.Background(),
		nil,
		pbjob.JobType_BATCH,
	)
	suite.True(yarpcerrors.IsInvalidArgument(err))
}

// TestCompareAndSetTask tests changing the task runtime
// using compare and set
func (suite *TaskTestSuite) TestCompareAndSetTask() {
	var labels []*peloton.Label

	labels = append(labels, initializeLabel("key", "value"))
	version := uint64(3)
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	runtime.ConfigVersion = version - 1
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		runtime)

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
			suite.instanceID,
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

	suite.taskStore.EXPECT().
		GetTaskConfig(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
			version).
		Return(taskConfig, nil, nil)

	_, err := tt.CompareAndSetTask(
		context.Background(),
		newRuntime,
		pbjob.JobType_BATCH,
	)
	suite.Nil(err)
	suite.checkListeners(tt, pbjob.JobType_BATCH)
}

// TestCompareAndSetTaskFailValidation tests changing the task runtime
// using compare and set but the new runtime fails validation
func (suite *TaskTestSuite) TestCompareAndSetTaskFailValidation() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		runtime)

	newRuntime := initializeTaskRuntime(pbtask.TaskState_PENDING, 2)
	newRuntime.GoalState = pbtask.TaskState_SUCCEEDED

	_, err := tt.CompareAndSetTask(
		context.Background(),
		newRuntime,
		pbjob.JobType_BATCH,
	)
	suite.Nil(err)
	suite.checkListenersNotCalled()
}

// TestCompareAndSetTaskLoadRuntime tests changing the task runtime
// using compare and set and runtime needs to be reloaded from DB
func (suite *TaskTestSuite) TestCompareAndSetTaskLoadRuntime() {
	var labels []*peloton.Label

	labels = append(labels, initializeLabel("key", "value"))
	version := uint64(3)
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		runtime)
	tt.runtime = nil
	tt.config = &taskConfigCache{
		configVersion: version - 1,
		labels:        labels,
	}

	newRuntime := initializeTaskRuntime(pbtask.TaskState_RUNNING, 2)
	newRuntime.GoalState = pbtask.TaskState_SUCCEEDED
	newRuntime.ConfigVersion = version

	taskConfig := &pbtask.TaskConfig{
		Labels: labels,
	}

	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, suite.instanceID).
		Return(runtime, nil)

	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
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

	suite.taskStore.EXPECT().
		GetTaskConfig(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
			version).
		Return(taskConfig, nil, nil)

	_, err := tt.CompareAndSetTask(
		context.Background(),
		newRuntime,
		pbjob.JobType_BATCH,
	)
	suite.Nil(err)
	suite.checkListeners(tt, pbjob.JobType_BATCH)
}

// TestCompareAndSetTaskLoadRuntimeDBError tests changing the task runtime
// using compare and set and runtime reload from DB yields error
func (suite *TaskTestSuite) TestCompareAndSetTaskLoadRuntimeDBError() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		runtime)
	tt.runtime = nil

	newRuntime := initializeTaskRuntime(pbtask.TaskState_RUNNING, 2)
	newRuntime.GoalState = pbtask.TaskState_SUCCEEDED

	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, suite.instanceID).
		Return(runtime, fmt.Errorf("fake db error"))

	_, err := tt.CompareAndSetTask(
		context.Background(),
		newRuntime,
		pbjob.JobType_BATCH,
	)
	suite.Error(err)
	suite.checkListenersNotCalled()
}

// TestCompareAndSetTaskVersionError tests changing the task runtime
// using compare and set but with a version error
func (suite *TaskTestSuite) TestCompareAndSetTaskVersionError() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		runtime)

	newRuntime := initializeTaskRuntime(pbtask.TaskState_RUNNING, 3)
	newRuntime.GoalState = pbtask.TaskState_SUCCEEDED

	_, err := tt.CompareAndSetTask(
		context.Background(),
		newRuntime,
		pbjob.JobType_BATCH,
	)
	suite.NotNil(err)
	suite.Equal(err, jobmgrcommon.UnexpectedVersionError)
	suite.checkListenersNotCalled()
}

// TestCompareAndSetTaskDBError tests changing the task runtime
// using compare and set but getting a DB error in the write
func (suite *TaskTestSuite) TestCompareAndSetTaskDBError() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		runtime)

	newRuntime := initializeTaskRuntime(pbtask.TaskState_RUNNING, 2)
	newRuntime.GoalState = pbtask.TaskState_SUCCEEDED

	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
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
		Return(fmt.Errorf("fake DB Error"))

	_, err := tt.CompareAndSetTask(
		context.Background(),
		newRuntime,
		pbjob.JobType_BATCH,
	)
	suite.Error(err)
	suite.checkListenersNotCalled()
}

// TestCompareAndSetTaskConfigDBError tests changing the task runtime
// using compare and set but getting an error while fetching the task config
func (suite *TaskTestSuite) TestCompareAndSetTaskConfigDBError() {
	version := uint64(3)
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	runtime.ConfigVersion = version - 1
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		runtime)

	newRuntime := initializeTaskRuntime(pbtask.TaskState_RUNNING, 2)
	newRuntime.GoalState = pbtask.TaskState_SUCCEEDED
	newRuntime.ConfigVersion = version

	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
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

	suite.taskStore.EXPECT().
		GetTaskConfig(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
			version).
		Return(nil, nil, fmt.Errorf("fake DB Error"))

	_, err := tt.CompareAndSetTask(
		context.Background(),
		newRuntime,
		pbjob.JobType_BATCH,
	)
	suite.Error(err)
	suite.checkListenersNotCalled()
}

// TestReplaceTask tests replacing runtime in the cache only
func (suite *TaskTestSuite) TestReplaceTask() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		runtime)

	newRuntime := initializeTaskRuntime(pbtask.TaskState_RUNNING, 3)

	err := tt.ReplaceTask(newRuntime, nil, false)
	suite.Nil(err)
	suite.Equal(tt.runtime.GetState(), pbtask.TaskState_RUNNING)
	suite.checkListenersNotCalled()
}

// TestReplaceTask_NoExistingCache tests replacing cache when
// there is no existing cache
func (suite *TaskTestSuite) TestReplaceTask_NoExistingCache() {
	var labels []*peloton.Label

	labels = append(labels, initializeLabel("key", "value"))
	taskConfig := &pbtask.TaskConfig{
		Labels: labels,
	}

	tt := &task{
		id:    suite.instanceID,
		jobID: suite.jobID,
	}

	suite.Equal(suite.instanceID, tt.ID())
	suite.Equal(suite.jobID.Value, tt.jobID.Value)

	// Test fetching state and goal state of task
	runtime := pbtask.RuntimeInfo{
		State:     pbtask.TaskState_RUNNING,
		GoalState: pbtask.TaskState_SUCCEEDED,
		Revision: &peloton.ChangeLog{
			Version: 1,
		},
	}
	tt.ReplaceTask(&runtime, taskConfig, false)

	curState := tt.CurrentState()
	curGoalState := tt.GoalState()
	suite.Equal(runtime.State, curState.State)
	suite.Equal(runtime.GoalState, curGoalState.State)
	suite.Equal(labels[0].GetKey(), tt.config.labels[0].GetKey())
	suite.Equal(labels[0].GetValue(), tt.config.labels[0].GetValue())
	suite.checkListenersNotCalled()
}

// TestReplaceTask_StaleRuntime tests replacing with stale runtime
func (suite *TaskTestSuite) TestReplaceTask_StaleRuntime() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		runtime)

	newRuntime := initializeTaskRuntime(pbtask.TaskState_RUNNING, 1)

	err := tt.ReplaceTask(newRuntime, nil, false)
	suite.Nil(err)
	suite.Equal(tt.runtime.GetState(), pbtask.TaskState_LAUNCHED)
	suite.checkListenersNotCalled()
}

func (suite *TaskTestSuite) TestGetCacheRuntime() {
	tt := []struct {
		runtime       *pbtask.RuntimeInfo
		expectedState pbtask.TaskState
	}{
		{
			runtime:       &pbtask.RuntimeInfo{State: pbtask.TaskState_RUNNING},
			expectedState: pbtask.TaskState_RUNNING,
		},
		{
			runtime:       nil,
			expectedState: pbtask.TaskState_UNKNOWN,
		},
	}

	for _, t := range tt {
		task := suite.initializeTask(
			suite.taskStore, suite.jobID, suite.instanceID, t.runtime)
		suite.Equal(t.expectedState, task.GetCacheRuntime().GetState())
	}
}

func (suite *TaskTestSuite) TestValidateState() {
	mesosIDWithRunID1 := "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-1"
	mesosIDWithRunID2 := "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-2"

	tt := []struct {
		curRuntime     *pbtask.RuntimeInfo
		newRuntime     *pbtask.RuntimeInfo
		expectedResult bool
		message        string
	}{
		{
			curRuntime:     &pbtask.RuntimeInfo{},
			newRuntime:     nil,
			expectedResult: false,
			message:        "nil new runtime should fail validation",
		},
		{
			curRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_RUNNING},
			newRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_RUNNING},
			expectedResult: true,
			message:        "state has no change, validate should succeed",
		},
		{
			curRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_SUCCEEDED},
			newRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_RUNNING},
			expectedResult: false,
			message:        "current state is in terminal, no state change allowed",
		},
		{
			curRuntime: &pbtask.RuntimeInfo{
				MesosTaskId: &mesosv1.TaskID{Value: &mesosIDWithRunID2},
			},
			newRuntime: &pbtask.RuntimeInfo{
				MesosTaskId: &mesosv1.TaskID{Value: &mesosIDWithRunID1},
			},
			expectedResult: false,
			message:        "runID in mesos id cannot decrease",
		},
		{
			curRuntime: &pbtask.RuntimeInfo{
				DesiredMesosTaskId: &mesosv1.TaskID{Value: &mesosIDWithRunID2},
			},
			newRuntime: &pbtask.RuntimeInfo{
				DesiredMesosTaskId: &mesosv1.TaskID{Value: &mesosIDWithRunID1},
			},
			expectedResult: false,
			message:        "runID in desired mesos id cannot decrease",
		},
		{
			curRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_PENDING},
			newRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_PLACING},
			expectedResult: true,
			message:        "state can change between resmgr state change",
		},
		{
			curRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_INITIALIZED},
			newRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_PENDING},
			expectedResult: true,
			message:        "state can change from INITIALIZED to resmgr state ",
		},
		{
			curRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_PENDING},
			newRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_KILLING},
			expectedResult: true,
			message:        "any state can change to KILLING state",
		},
		{
			curRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_RUNNING},
			newRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_PENDING},
			expectedResult: false,
			message:        "invalid sate transition from RUNNING to PENDING",
		},
		{
			curRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_KILLING},
			newRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_RUNNING},
			expectedResult: false,
			message:        "KILLING can only transit to terminal state",
		},
		{
			curRuntime: &pbtask.RuntimeInfo{
				GoalState:            pbtask.TaskState_DELETED,
				DesiredConfigVersion: 3,
			},
			newRuntime: &pbtask.RuntimeInfo{
				GoalState:            pbtask.TaskState_RUNNING,
				DesiredConfigVersion: 3,
			},
			expectedResult: false,
			message:        "DELETED goal state cannot be overwritten with config change",
		},
		{
			curRuntime: &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_RUNNING,
				GoalState:            pbtask.TaskState_DELETED,
				DesiredConfigVersion: 3,
			},
			newRuntime: &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_RUNNING,
				GoalState:            pbtask.TaskState_RUNNING,
				DesiredConfigVersion: 4,
			},
			expectedResult: true,
			message:        "DELETED goal state can be overwritten with config change",
		},
	}

	for i, t := range tt {
		task := suite.initializeTask(
			suite.taskStore, suite.jobID, suite.instanceID, t.curRuntime)
		suite.Equal(task.validateState(t.newRuntime), t.expectedResult,
			"test %d fails. message: %s", i, t.message)
	}
}

// TestGetResourceManagerProcessingStates tests
// whether GetResourceManagerProcessingStates returns right states
func TestGetResourceManagerProcessingStates(t *testing.T) {
	expect := []string{
		pbtask.TaskState_LAUNCHING.String(),
		pbtask.TaskState_PENDING.String(),
		pbtask.TaskState_READY.String(),
		pbtask.TaskState_PLACING.String(),
		pbtask.TaskState_PLACED.String(),
		pbtask.TaskState_PREEMPTING.String(),
	}
	states := GetResourceManagerProcessingStates()
	sort.Strings(expect)
	sort.Strings(states)

	assert.Equal(t, expect, states)
}

// TestDeleteTask tests that listeners receive an event when a task is deleted
func (suite *TaskTestSuite) TestDeleteTask() {
	runtime := initializeTaskRuntime(pbtask.TaskState_DELETED, 2)
	runtime.GoalState = pbtask.TaskState_DELETED
	tt := suite.initializeTask(suite.taskStore, suite.jobID,
		suite.instanceID, runtime)

	tt.DeleteTask()
	suite.checkListeners(tt, tt.jobType)
}

// TestDeleteTaskNoRuntime tests that listeners receive no event
// when a task with no runtime is deleted
func (suite *TaskTestSuite) TestDeleteTaskNoRuntime() {
	tt := suite.initializeTask(suite.taskStore, suite.jobID,
		suite.instanceID, nil)
	tt.runtime = nil

	tt.DeleteTask()
	suite.checkListenersNotCalled()
}

// TestDeleteTaskNoRuntime tests that listeners receive no event
// when a task with no runtime is deleted
func (suite *TaskTestSuite) TestDeleteRunningTask() {
	runtime := initializeTaskRuntime(pbtask.TaskState_RUNNING, 2)
	tt := suite.initializeTask(suite.taskStore, suite.jobID,
		suite.instanceID, runtime)

	tt.DeleteTask()
	suite.checkListenersNotCalled()
}

// TestGetLabels tests getting labels from the cache
func (suite *TaskTestSuite) TestGetLabels() {
	var labels []*peloton.Label

	labels = append(labels, initializeLabel("key", "value"))
	version := uint64(3)
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.ConfigVersion = version
	tt := suite.initializeTask(suite.taskStore, suite.jobID,
		suite.instanceID, runtime)

	taskConfig := &pbtask.TaskConfig{
		Labels: labels,
	}

	suite.taskStore.EXPECT().
		GetTaskConfig(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
			version).
		Return(taskConfig, nil, nil)

	newLabels, err := tt.GetLabels(context.Background())
	suite.Nil(err)
	suite.Equal(len(newLabels), len(labels))
	for count, label := range labels {
		suite.Equal(newLabels[count].GetKey(), label.GetKey())
		suite.Equal(newLabels[count].GetValue(), label.GetValue())
	}
}

// TestGetLabelsRuntimeDBError tests getting a DB error
// when fetching the task runtime
func (suite *TaskTestSuite) TestGetLabelsRuntimeDBError() {
	tt := suite.initializeTask(suite.taskStore, suite.jobID,
		suite.instanceID, nil)

	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, suite.instanceID).
		Return(nil, fmt.Errorf("fake db error"))

	_, err := tt.GetLabels(context.Background())
	suite.NotNil(err)
}

// TestGetLabelsConfigDBError tests getting a DB error
// when fetching the task config
func (suite *TaskTestSuite) TestGetLabelsConfigDBError() {
	version := uint64(3)
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.ConfigVersion = version
	tt := suite.initializeTask(suite.taskStore, suite.jobID,
		suite.instanceID, runtime)

	suite.taskStore.EXPECT().
		GetTaskConfig(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
			version).
		Return(nil, nil, fmt.Errorf("fake db error"))

	_, err := tt.GetLabels(context.Background())
	suite.NotNil(err)
}

// TestStateTransitionMetrics tests calculation of metrics like
// time-to-assign and time-ro-run
func (suite *TaskTestSuite) TestStateTransitionMetrics() {
	testcases := []struct {
		revocable bool
		toState   pbtask.TaskState
		metric    string
	}{
		{
			revocable: true,
			toState:   pbtask.TaskState_LAUNCHED,
			metric:    "time_to_assign_revocable+",
		},
		{
			revocable: false,
			toState:   pbtask.TaskState_LAUNCHED,
			metric:    "time_to_assign_non_revocable+",
		},
		{
			revocable: true,
			toState:   pbtask.TaskState_RUNNING,
			metric:    "time_to_run_revocable+",
		},
		{
			revocable: false,
			toState:   pbtask.TaskState_RUNNING,
			metric:    "time_to_run_non_revocable+",
		},
	}

	for _, usePatch := range []bool{true, false} {
		for _, tc := range testcases {
			runtime := initializeTaskRuntime(pbtask.TaskState_INITIALIZED, 0)
			tt := suite.initializeTask(suite.taskStore, suite.jobID,
				suite.instanceID, runtime)
			tt.initializedAt = time.Now()

			msg := fmt.Sprintf("%s (usePatch: %v)", tc.metric, usePatch)

			taskConfig := &pbtask.TaskConfig{Revocable: tc.revocable}
			suite.taskStore.EXPECT().
				GetTaskConfig(
					gomock.Any(),
					suite.jobID,
					suite.instanceID,
					gomock.Any()).
				Return(taskConfig, nil, nil)
			suite.taskStore.EXPECT().
				CreateTaskRuntime(
					gomock.Any(),
					suite.jobID,
					suite.instanceID,
					gomock.Any(),
					gomock.Any(),
					gomock.Any()).
				Return(nil)
			suite.taskStore.EXPECT().
				UpdateTaskRuntime(
					gomock.Any(),
					suite.jobID,
					suite.instanceID,
					gomock.Any(),
					gomock.Any()).
				Return(nil)

			err := tt.CreateTask(context.Background(), runtime, "")
			suite.NoError(err, msg)

			if usePatch {
				diff := jobmgrcommon.RuntimeDiff{
					jobmgrcommon.StateField: tc.toState,
				}
				err = tt.PatchTask(context.Background(), diff)
			} else {
				runtime.State = tc.toState
				_, err = tt.CompareAndSetTask(
					context.Background(),
					runtime,
					tt.jobType)
			}
			suite.NoError(err, msg)

			tmr, ok := suite.testTaskScope.Snapshot().Timers()[tc.metric]
			suite.True(ok, msg)
			suite.Equal(1, len(tmr.Values()), msg)
		}
	}
}
