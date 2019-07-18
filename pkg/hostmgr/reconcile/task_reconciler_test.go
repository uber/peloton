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

package reconcile

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/atomic"
	"github.com/uber-go/tally"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	sched "github.com/uber/peloton/.gen/mesos/v1/scheduler"
	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"

	"github.com/uber/peloton/pkg/common/util"
	mock_mpb "github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb/mocks"
	store_mocks "github.com/uber/peloton/pkg/storage/mocks"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"
)

const (
	streamID                       = "streamID"
	frameworkID                    = "frameworkID"
	testInstanceCount              = 5
	testJobID                      = "testJob0"
	testAgentID                    = "agentID"
	testBatchSize                  = 3
	explicitReconcileBatchInterval = 100 * time.Millisecond
	oneExplicitReconcileRunDelay   = 350 * time.Millisecond
)

// A mock implementation of FrameworkInfoProvider
type mockFrameworkInfoProvider struct{}

func (m *mockFrameworkInfoProvider) GetMesosStreamID(ctx context.Context) string {
	return streamID
}

func (m *mockFrameworkInfoProvider) GetFrameworkID(ctx context.Context) *mesos.FrameworkID {
	tmp := frameworkID
	return &mesos.FrameworkID{Value: &tmp}
}

type TaskReconcilerTestSuite struct {
	suite.Suite

	running             atomic.Bool
	ctrl                *gomock.Controller
	testScope           tally.TestScope
	schedulerClient     *mock_mpb.MockSchedulerClient
	reconciler          *taskReconciler
	mockTaskStore       *store_mocks.MockTaskStore
	mockActiveJobsOps   *objectmocks.MockActiveJobsOps
	testJobID           *peloton.JobID
	testJobConfig       *job.JobConfig
	allJobRuntime       map[string]*job.RuntimeInfo
	taskInfos           map[uint32]*task.TaskInfo
	taskMixedStateInfos map[uint32]*task.TaskInfo
}

func (suite *TaskReconcilerTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.testScope = tally.NewTestScope("", map[string]string{})
	suite.schedulerClient = mock_mpb.NewMockSchedulerClient(suite.ctrl)
	suite.mockActiveJobsOps = objectmocks.NewMockActiveJobsOps(suite.ctrl)
	suite.mockTaskStore = store_mocks.NewMockTaskStore(suite.ctrl)
	suite.testJobID = &peloton.JobID{
		Value: testJobID,
	}
	suite.testJobConfig = &job.JobConfig{
		Name:          suite.testJobID.Value,
		InstanceCount: testInstanceCount,
	}
	suite.allJobRuntime = make(map[string]*job.RuntimeInfo)
	suite.allJobRuntime[testJobID] = &job.RuntimeInfo{}
	suite.taskInfos = make(map[uint32]*task.TaskInfo)
	for i := uint32(0); i < testInstanceCount; i++ {
		suite.taskInfos[i] = suite.createTestTaskInfo(
			task.TaskState_RUNNING, i)
	}

	suite.taskMixedStateInfos = make(map[uint32]*task.TaskInfo)
	suite.taskMixedStateInfos[0] = suite.createTestTaskInfo(
		task.TaskState_LAUNCHED, 0)
	suite.taskMixedStateInfos[1] = suite.createTestTaskInfo(
		task.TaskState_LAUNCHED, 1)
	suite.taskMixedStateInfos[2] = suite.createTestTaskInfo(
		task.TaskState_STARTING, 2)
	suite.taskMixedStateInfos[3] = suite.createTestTaskInfo(
		task.TaskState_RUNNING, 3)
	suite.taskMixedStateInfos[4] = suite.createTestTaskInfo(
		task.TaskState_RUNNING, 4)

	suite.reconciler = &taskReconciler{
		schedulerClient:                suite.schedulerClient,
		metrics:                        NewMetrics(suite.testScope),
		frameworkInfoProvider:          &mockFrameworkInfoProvider{},
		activeJobsOps:                  suite.mockActiveJobsOps,
		taskStore:                      suite.mockTaskStore,
		explicitReconcileBatchInterval: explicitReconcileBatchInterval,
		explicitReconcileBatchSize:     testBatchSize,
	}
	suite.reconciler.isExplicitReconcileTurn.Store(true)
}

func (suite *TaskReconcilerTestSuite) TearDownTest() {
	log.Debug("tearing down")
	suite.ctrl.Finish()
}

func (suite *TaskReconcilerTestSuite) createTestTaskInfo(
	state task.TaskState,
	instanceID uint32) *task.TaskInfo {

	var taskID = fmt.Sprintf("%s-%d", suite.testJobID.Value, instanceID)
	return &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			MesosTaskId: &mesos.TaskID{Value: &taskID},
			State:       state,
			GoalState:   task.TaskState_SUCCEEDED,
			AgentID: &mesos.AgentID{
				Value: util.PtrPrintf(testAgentID),
			},
		},
		Config:     suite.testJobConfig.GetDefaultConfig(),
		InstanceId: instanceID,
		JobId:      suite.testJobID,
	}
}

func TestTaskReconcilerTestSuite(t *testing.T) {
	suite.Run(t, new(TaskReconcilerTestSuite))
}

func (suite *TaskReconcilerTestSuite) TestNewTaskReconciler() {
	reconciler := NewTaskReconciler(
		suite.schedulerClient,
		suite.testScope,
		&mockFrameworkInfoProvider{},
		suite.mockActiveJobsOps,
		suite.mockTaskStore,
		&TaskReconcilerConfig{
			ExplicitReconcileBatchIntervalSec: int(explicitReconcileBatchInterval / time.Millisecond),
			ExplicitReconcileBatchSize:        testBatchSize,
		},
	)
	suite.NotNil(reconciler)
}

func (suite *TaskReconcilerTestSuite) TestTaskReconcilationPeriodicalCalls() {
	// need to sync the goroutine created by taskReconciler.Reconcile
	// and the test goroutine to avoid data race in test
	finishCh := make(chan struct{}, 1)
	gomock.InOrder(
		suite.mockActiveJobsOps.EXPECT().
			GetAll(context.Background()).
			Return([]*peloton.JobID{suite.testJobID}, nil),
		suite.mockTaskStore.EXPECT().
			GetTasksForJobAndStates(
				context.Background(),
				suite.testJobID,
				[]task.TaskState{
					task.TaskState_LAUNCHED,
					task.TaskState_STARTING,
					task.TaskState_RUNNING,
					task.TaskState_KILLING,
				}).
			Return(suite.taskInfos, nil),
		suite.schedulerClient.EXPECT().
			Call(
				gomock.Eq(streamID),
				gomock.Any()).
			Do(func(_ string, msg proto.Message) {
				// Verify explicit reconcile tasks number is same as batch size.
				call := msg.(*sched.Call)
				suite.Equal(sched.Call_RECONCILE, call.GetType())
				suite.Equal(frameworkID, call.GetFrameworkId().GetValue())
				suite.Equal(testBatchSize, len(call.GetReconcile().GetTasks()))
				suite.Equal(call.GetReconcile().GetTasks()[0].AgentId.GetValue(), testAgentID)
			}).
			Return(nil),
		suite.schedulerClient.EXPECT().
			Call(
				gomock.Eq(streamID),
				gomock.Any()).
			Do(func(_ string, msg proto.Message) {
				// Verify explicit reconcile tasks number is less than batch size.
				call := msg.(*sched.Call)
				suite.Equal(sched.Call_RECONCILE, call.GetType())
				suite.Equal(frameworkID, call.GetFrameworkId().GetValue())
				suite.Equal(
					testInstanceCount-testBatchSize,
					len(call.GetReconcile().GetTasks()))
				suite.Equal(call.GetReconcile().GetTasks()[0].AgentId.GetValue(), testAgentID)
			}).
			Return(nil),
		suite.schedulerClient.EXPECT().
			Call(
				gomock.Eq(streamID),
				gomock.Any()).
			Do(func(_ string, msg proto.Message) {
				// Verify implicit reconcile call.
				call := msg.(*sched.Call)
				suite.Equal(sched.Call_RECONCILE, call.GetType())
				suite.Equal(frameworkID, call.GetFrameworkId().GetValue())
				suite.Equal(0, len(call.GetReconcile().GetTasks()))
				finishCh <- struct{}{}
			}).
			Return(nil),
	)

	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), true)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)
	suite.running.Store(true)
	suite.reconciler.Reconcile(&suite.running)
	time.Sleep(explicitReconcileBatchInterval)
	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), false)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), true)
	time.Sleep(oneExplicitReconcileRunDelay)
	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), false)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)
	suite.reconciler.Reconcile(&suite.running)
	time.Sleep(explicitReconcileBatchInterval)
	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), true)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)
	<-finishCh
}

func (suite *TaskReconcilerTestSuite) TestTaskReconcilationCallFailure() {
	// need to sync the goroutine created by taskReconciler.Reconcile
	// and the test goroutine to avoid data race in test
	finishCh := make(chan struct{}, 1)
	gomock.InOrder(
		suite.mockActiveJobsOps.EXPECT().
			GetAll(context.Background()).
			Return([]*peloton.JobID{suite.testJobID}, nil),
		suite.mockTaskStore.EXPECT().
			GetTasksForJobAndStates(
				context.Background(),
				suite.testJobID,
				[]task.TaskState{
					task.TaskState_LAUNCHED,
					task.TaskState_STARTING,
					task.TaskState_RUNNING,
					task.TaskState_KILLING,
				}).
			Return(suite.taskInfos, nil),
		suite.schedulerClient.EXPECT().
			Call(
				gomock.Eq(streamID),
				gomock.Any()).
			Do(func(_ string, msg proto.Message) {
				// Verify explicit reconcile tasks number is same as batch size.
				call := msg.(*sched.Call)
				suite.Equal(sched.Call_RECONCILE, call.GetType())
				suite.Equal(frameworkID, call.GetFrameworkId().GetValue())
				suite.Equal(testBatchSize, len(call.GetReconcile().GetTasks()))
				suite.Equal(call.GetReconcile().GetTasks()[0].AgentId.GetValue(), testAgentID)
			}).
			Return(fmt.Errorf("fake error")),
		suite.schedulerClient.EXPECT().
			Call(
				gomock.Eq(streamID),
				gomock.Any()).
			Do(func(_ string, msg proto.Message) {
				// Verify implicit reconcile call.
				call := msg.(*sched.Call)
				suite.Equal(sched.Call_RECONCILE, call.GetType())
				suite.Equal(frameworkID, call.GetFrameworkId().GetValue())
				suite.Equal(0, len(call.GetReconcile().GetTasks()))
				finishCh <- struct{}{}
			}).
			Return(fmt.Errorf("fake error")),
	)

	suite.running.Store(true)

	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), true)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)
	suite.reconciler.Reconcile(&suite.running)
	time.Sleep(oneExplicitReconcileRunDelay)
	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), false)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)
	suite.reconciler.Reconcile(&suite.running)
	time.Sleep(oneExplicitReconcileRunDelay)
	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), true)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)
	<-finishCh
}

func (suite *TaskReconcilerTestSuite) TestReconcilerNotStartIfAlreadyRunning() {
	// need to sync the goroutine created by taskReconciler.Reconcile
	// and the test goroutine to avoid data race in test
	finishCh := make(chan struct{}, 1)
	gomock.InOrder(
		suite.mockActiveJobsOps.EXPECT().
			GetAll(context.Background()).
			Return([]*peloton.JobID{suite.testJobID}, nil),
		suite.mockTaskStore.EXPECT().
			GetTasksForJobAndStates(
				context.Background(),
				suite.testJobID,
				[]task.TaskState{
					task.TaskState_LAUNCHED,
					task.TaskState_STARTING,
					task.TaskState_RUNNING,
					task.TaskState_KILLING,
				}).
			Return(suite.taskInfos, nil),
		suite.schedulerClient.EXPECT().
			Call(
				gomock.Eq(streamID),
				gomock.Any()).
			Do(func(_ string, msg proto.Message) {
				// Verify explicit reconcile tasks number is same as batch size.
				call := msg.(*sched.Call)
				suite.Equal(sched.Call_RECONCILE, call.GetType())
				suite.Equal(frameworkID, call.GetFrameworkId().GetValue())
				suite.Equal(testBatchSize, len(call.GetReconcile().GetTasks()))
				finishCh <- struct{}{}
			}).
			Return(nil),
	)

	suite.running.Store(true)

	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), true)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)

	suite.reconciler.Reconcile(&suite.running)
	time.Sleep(explicitReconcileBatchInterval / 2)

	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), false)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), true)
	// Another call to explicit reconcile should be a no-op
	suite.reconciler.reconcileExplicitly(context.Background(), &suite.running)

	suite.running.Store(false)
	time.Sleep(oneExplicitReconcileRunDelay)
	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), false)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)
	<-finishCh
}

func (suite *TaskReconcilerTestSuite) TestTaskReconcilationWithStatingStates() {
	// need to sync the goroutine created by taskReconciler.Reconcile
	// and the test goroutine to avoid data race in test
	finishCh := make(chan struct{}, 1)
	gomock.InOrder(
		suite.mockActiveJobsOps.EXPECT().
			GetAll(context.Background()).
			Return([]*peloton.JobID{suite.testJobID}, nil),
		suite.mockTaskStore.EXPECT().
			GetTasksForJobAndStates(
				context.Background(),
				suite.testJobID,
				[]task.TaskState{
					task.TaskState_LAUNCHED,
					task.TaskState_STARTING,
					task.TaskState_RUNNING,
					task.TaskState_KILLING,
				}).
			Return(suite.taskMixedStateInfos, nil),
		suite.schedulerClient.EXPECT().
			Call(
				gomock.Eq(streamID),
				gomock.Any()).
			Do(func(_ string, msg proto.Message) {
				// Verify explicit reconcile tasks number is same as batch size.
				call := msg.(*sched.Call)
				suite.Equal(sched.Call_RECONCILE, call.GetType())
				suite.Equal(frameworkID, call.GetFrameworkId().GetValue())
				suite.Equal(3, len(call.GetReconcile().GetTasks()))
				suite.Equal(call.GetReconcile().GetTasks()[0].AgentId.GetValue(), testAgentID)
			}).
			Return(nil),
		suite.schedulerClient.EXPECT().
			Call(
				gomock.Eq(streamID),
				gomock.Any()).
			Do(func(_ string, msg proto.Message) {
				// Verify explicit reconcile tasks number is less than batch size.
				call := msg.(*sched.Call)
				suite.Equal(sched.Call_RECONCILE, call.GetType())
				suite.Equal(frameworkID, call.GetFrameworkId().GetValue())
				suite.Equal(
					2,
					len(call.GetReconcile().GetTasks()))
				suite.Equal(call.GetReconcile().GetTasks()[0].AgentId.GetValue(), testAgentID)
			}).
			Return(nil),
		suite.schedulerClient.EXPECT().
			Call(
				gomock.Eq(streamID),
				gomock.Any()).
			Do(func(_ string, msg proto.Message) {
				// Verify implicit reconcile call.
				call := msg.(*sched.Call)
				suite.Equal(sched.Call_RECONCILE, call.GetType())
				suite.Equal(frameworkID, call.GetFrameworkId().GetValue())
				suite.Equal(0, len(call.GetReconcile().GetTasks()))
				finishCh <- struct{}{}
			}).
			Return(nil),
	)

	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), true)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)
	suite.running.Store(true)
	suite.reconciler.Reconcile(&suite.running)
	time.Sleep(explicitReconcileBatchInterval)
	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), false)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), true)
	time.Sleep(oneExplicitReconcileRunDelay)
	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), false)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)
	suite.reconciler.Reconcile(&suite.running)
	time.Sleep(explicitReconcileBatchInterval)
	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), true)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)
	<-finishCh
}

func (suite *TaskReconcilerTestSuite) TestTaskReconciliationExplicitTurnTurn() {
	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), true)

	suite.reconciler.SetExplicitReconcileTurn(false)
	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), false)
}

func (suite *TaskReconcilerTestSuite) TestTaskReconcilerGetActiveJobsError() {
	suite.mockActiveJobsOps.EXPECT().
		GetAll(context.Background()).
		Return(nil, fmt.Errorf("Fake GetActiveJobs error"))

	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), true)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)
	suite.running.Store(true)
	suite.reconciler.Reconcile(&suite.running)
	time.Sleep(explicitReconcileBatchInterval)
	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), false)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)
}

func (suite *TaskReconcilerTestSuite) TestTaskReconcilerGetTasksForJobAndStates() {
	gomock.InOrder(
		suite.mockActiveJobsOps.EXPECT().
			GetAll(context.Background()).
			Return([]*peloton.JobID{suite.testJobID}, nil),
		suite.mockTaskStore.EXPECT().
			GetTasksForJobAndStates(
				context.Background(),
				suite.testJobID,
				[]task.TaskState{
					task.TaskState_LAUNCHED,
					task.TaskState_STARTING,
					task.TaskState_RUNNING,
					task.TaskState_KILLING,
				}).
			Return(nil, fmt.Errorf("Fake GetTasksForJobAndStates error")),
	)
	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), true)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)
	suite.running.Store(true)
	suite.reconciler.Reconcile(&suite.running)
	time.Sleep(explicitReconcileBatchInterval)
	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), false)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)
}
