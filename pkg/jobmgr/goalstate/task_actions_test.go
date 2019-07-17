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

	"github.com/uber/peloton/pkg/jobmgr/cached"

	goalstatemocks "github.com/uber/peloton/pkg/common/goalstate/mocks"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	storemocks "github.com/uber/peloton/pkg/storage/mocks"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

type TaskActionTestSuite struct {
	suite.Suite

	ctrl      *gomock.Controller
	taskStore *storemocks.MockTaskStore

	jobGoalStateEngine  *goalstatemocks.MockEngine
	taskGoalStateEngine *goalstatemocks.MockEngine
	jobFactory          *cachedmocks.MockJobFactory
	cachedJob           *cachedmocks.MockJob
	cachedTask          *cachedmocks.MockTask
	taskConfigV2Ops     *objectmocks.MockTaskConfigV2Ops
	goalStateDriver     *driver
	taskEnt             *taskEntity
	jobID               *peloton.JobID
	instanceID          uint32
}

func TestTaskAction(t *testing.T) {
	suite.Run(t, new(TaskActionTestSuite))
}

func (suite *TaskActionTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.taskStore = storemocks.NewMockTaskStore(suite.ctrl)
	suite.jobGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.taskGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.cachedTask = cachedmocks.NewMockTask(suite.ctrl)
	suite.taskConfigV2Ops = objectmocks.NewMockTaskConfigV2Ops(suite.ctrl)

	suite.goalStateDriver = &driver{
		taskStore:       suite.taskStore,
		jobEngine:       suite.jobGoalStateEngine,
		taskEngine:      suite.taskGoalStateEngine,
		jobFactory:      suite.jobFactory,
		mtx:             NewMetrics(tally.NoopScope),
		cfg:             &Config{},
		taskConfigV2Ops: suite.taskConfigV2Ops,
	}
	suite.goalStateDriver.cfg.normalize()

	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.instanceID = uint32(0)

	suite.taskEnt = &taskEntity{
		jobID:      suite.jobID,
		instanceID: suite.instanceID,
		driver:     suite.goalStateDriver,
	}
}

func (suite *TaskActionTestSuite) TestTaskReloadRuntime() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)
	suite.cachedJob.EXPECT().
		AddTask(gomock.Any(), suite.instanceID).
		Return(suite.cachedTask, nil)
	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, suite.instanceID).
		Return(&pbtask.RuntimeInfo{}, nil)
	suite.taskConfigV2Ops.EXPECT().
		GetTaskConfig(gomock.Any(), suite.jobID, suite.instanceID, gomock.Any()).
		Return(&pbtask.TaskConfig{}, nil, nil)
	suite.cachedJob.EXPECT().
		ReplaceTasks(gomock.Any(), gomock.Any())
	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()
	err := TaskReloadRuntime(context.Background(), suite.taskEnt)
	suite.NoError(err)
}

// TestTaskReloadRuntimeNotFoundError tests task runtime is not found
// when task is reloaded
func (suite *TaskActionTestSuite) TestTaskReloadRuntimeNotFoundError() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)
	suite.cachedJob.EXPECT().
		AddTask(gomock.Any(), suite.instanceID).
		Return(suite.cachedTask, nil)
	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, suite.instanceID).
		Return(nil, yarpcerrors.NotFoundErrorf("test error"))
	suite.cachedJob.EXPECT().
		RemoveTask(suite.instanceID)
	err := TaskReloadRuntime(context.Background(), suite.taskEnt)
	suite.NoError(err)
}

// TestTaskReloadRuntimeGetConfigError tests reloading the task into
// cache and getting a DB error when fetching the task config
func (suite *TaskActionTestSuite) TestTaskReloadRuntimeGetConfigError() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)
	suite.cachedJob.EXPECT().
		AddTask(gomock.Any(), suite.instanceID).
		Return(suite.cachedTask, nil)
	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, suite.instanceID).
		Return(&pbtask.RuntimeInfo{}, nil)
	suite.taskConfigV2Ops.EXPECT().
		GetTaskConfig(gomock.Any(), suite.jobID, suite.instanceID, gomock.Any()).
		Return(nil, nil, fmt.Errorf("fake db error"))
	err := TaskReloadRuntime(context.Background(), suite.taskEnt)
	suite.Error(err)
}

func (suite *TaskActionTestSuite) TestTaskStateInvalidAction() {
	newRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	newRuntimes[0] = &pbtask.RuntimeInfo{
		State: pbtask.TaskState_FAILED,
	}
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetTask(suite.instanceID).
		Return(suite.cachedTask)
	suite.cachedTask.EXPECT().
		CurrentState().
		Return(cached.TaskStateVector{
			State: pbtask.TaskState_RUNNING,
		})
	suite.cachedTask.EXPECT().
		GoalState().
		Return(cached.TaskStateVector{
			State: pbtask.TaskState_KILLING,
		})
	suite.cachedTask.EXPECT().
		ID().
		Return(suite.instanceID)
	err := TaskStateInvalid(context.Background(), suite.taskEnt)
	suite.NoError(err)
}

func (suite *TaskActionTestSuite) TestTaskDeleteAction() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)
	suite.cachedJob.EXPECT().
		RemoveTask(suite.instanceID).
		Return()
	suite.taskStore.EXPECT().
		DeleteTaskRuntime(gomock.Any(), suite.jobID, suite.instanceID).
		Return(nil)
	suite.cachedJob.EXPECT().
		GetJobType().
		Return(pbjob.JobType_SERVICE)
	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()
	suite.NoError(TaskDelete(context.Background(), suite.taskEnt))
}

func (suite *TaskActionTestSuite) TestTaskDeleteActionDBError() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)
	suite.cachedJob.EXPECT().
		RemoveTask(suite.instanceID).
		Return()
	suite.taskStore.EXPECT().
		DeleteTaskRuntime(gomock.Any(), suite.jobID, suite.instanceID).
		Return(fmt.Errorf("fake db error"))
	suite.cachedJob.EXPECT().
		GetJobType().
		Return(pbjob.JobType_SERVICE)
	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()
	suite.Error(TaskDelete(context.Background(), suite.taskEnt))
}
