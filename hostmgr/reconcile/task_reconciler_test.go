package reconcile

import (
	"fmt"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	mesos "mesos/v1"
	sched "mesos/v1/scheduler"
	"peloton/api/job"
	"peloton/api/peloton"
	"peloton/api/task"

	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
	mock_mpb "code.uber.internal/infra/peloton/yarpc/encoding/mpb/mocks"
)

const (
	streamID                       = "streamID"
	frameworkID                    = "frameworkID"
	testInstanceCount              = 5
	testJobID                      = "testJob0"
	testBatchSize                  = 3
	explicitReconcileBatchInterval = 100 * time.Millisecond
	initialReconcileDelay          = 300 * time.Millisecond
	reconcileInterval              = 300 * time.Millisecond
	runningStateStr                = "8"
	oneExplicitReconcileRunDelay   = 350 * time.Millisecond
)

// A mock implementation of FrameworkInfoProvider
type mockFrameworkInfoProvider struct{}

func (m *mockFrameworkInfoProvider) GetMesosStreamID() string {
	return streamID
}

func (m *mockFrameworkInfoProvider) GetFrameworkID() *mesos.FrameworkID {
	tmp := frameworkID
	return &mesos.FrameworkID{Value: &tmp}
}

type TaskReconcilerTestSuite struct {
	suite.Suite

	ctrl            *gomock.Controller
	testScope       tally.TestScope
	schedulerClient *mock_mpb.MockSchedulerClient
	reconciler      *taskReconciler
	mockJobStore    *store_mocks.MockJobStore
	mockTaskStore   *store_mocks.MockTaskStore
	testJobID       *peloton.JobID
	testJobConfig   *job.JobConfig
	allJobConfigs   map[string]*job.JobConfig
	taskInfos       map[uint32]*task.TaskInfo
}

func (suite *TaskReconcilerTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.testScope = tally.NewTestScope("", map[string]string{})
	suite.schedulerClient = mock_mpb.NewMockSchedulerClient(suite.ctrl)
	suite.mockJobStore = store_mocks.NewMockJobStore(suite.ctrl)
	suite.mockTaskStore = store_mocks.NewMockTaskStore(suite.ctrl)
	suite.testJobID = &peloton.JobID{
		Value: testJobID,
	}
	suite.testJobConfig = &job.JobConfig{
		Name:          suite.testJobID.Value,
		InstanceCount: testInstanceCount,
	}
	suite.allJobConfigs = make(map[string]*job.JobConfig)
	suite.allJobConfigs[testJobID] = suite.testJobConfig
	suite.taskInfos = make(map[uint32]*task.TaskInfo)
	for i := uint32(0); i < testInstanceCount; i++ {
		suite.taskInfos[i] = suite.createTestTaskInfo(
			task.TaskState_RUNNING, i)
	}

	suite.reconciler = &taskReconciler{
		schedulerClient:                suite.schedulerClient,
		metrics:                        NewMetrics(suite.testScope),
		frameworkInfoProvider:          &mockFrameworkInfoProvider{},
		jobStore:                       suite.mockJobStore,
		taskStore:                      suite.mockTaskStore,
		initialReconcileDelay:          initialReconcileDelay,
		reconcileInterval:              reconcileInterval,
		explicitReconcileBatchInterval: explicitReconcileBatchInterval,
		explicitReconcileBatchSize:     testBatchSize,
		stopChan:                       make(chan struct{}, 1),
	}
	suite.reconciler.isExplicitReconcileTurn.Store(true)
}

func (suite *TaskReconcilerTestSuite) TearDownTest() {
	log.Debug("tearing down")
}

func (suite *TaskReconcilerTestSuite) createTestTaskInfo(
	state task.TaskState,
	instanceID uint32) *task.TaskInfo {

	var taskID = fmt.Sprintf("%s-%d", suite.testJobID.Value, instanceID)
	return &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			TaskId:    &mesos.TaskID{Value: &taskID},
			State:     state,
			GoalState: task.TaskState_SUCCEEDED,
		},
		Config:     suite.testJobConfig.GetDefaultConfig(),
		InstanceId: instanceID,
		JobId:      suite.testJobID,
	}
}

func TestTaskReconcilerTestSuite(t *testing.T) {
	suite.Run(t, new(TaskReconcilerTestSuite))
}

func (suite *TaskReconcilerTestSuite) TestTaskReconcilationPeriodicalCalls() {
	defer suite.ctrl.Finish()
	gomock.InOrder(
		suite.mockJobStore.EXPECT().
			GetAllJobs().Return(suite.allJobConfigs, nil),
		suite.mockTaskStore.EXPECT().
			GetTasksForJobAndState(suite.testJobID, runningStateStr).
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
			}).
			Return(nil),
	)

	suite.Equal(suite.reconciler.Running.Load(), false)
	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), true)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)
	suite.reconciler.Start()
	time.Sleep(oneExplicitReconcileRunDelay)
	suite.Equal(suite.reconciler.Running.Load(), true)
	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), false)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), true)
	time.Sleep(reconcileInterval)
	suite.Equal(suite.reconciler.Running.Load(), true)
	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), true)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)
	suite.reconciler.Stop()
	suite.Equal(suite.reconciler.Running.Load(), false)
}

func (suite *TaskReconcilerTestSuite) TestTaskReconcilationCallFailure() {
	defer suite.ctrl.Finish()
	gomock.InOrder(
		suite.mockJobStore.EXPECT().
			GetAllJobs().Return(suite.allJobConfigs, nil),
		suite.mockTaskStore.EXPECT().
			GetTasksForJobAndState(suite.testJobID, runningStateStr).
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
			}).
			Return(nil),
	)

	suite.Equal(suite.reconciler.Running.Load(), false)
	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), true)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)
	suite.reconciler.Start()
	time.Sleep(oneExplicitReconcileRunDelay)
	suite.Equal(suite.reconciler.Running.Load(), true)
	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), false)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)
	time.Sleep(reconcileInterval)
	suite.Equal(suite.reconciler.Running.Load(), true)
	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), true)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)
	suite.reconciler.Stop()
	suite.Equal(suite.reconciler.Running.Load(), false)
}

func (suite *TaskReconcilerTestSuite) TestReconcilerNotStartIfAlreadyRunning() {
	defer suite.ctrl.Finish()
	gomock.InOrder(
		suite.mockJobStore.EXPECT().
			GetAllJobs().Return(suite.allJobConfigs, nil),
		suite.mockTaskStore.EXPECT().
			GetTasksForJobAndState(suite.testJobID, runningStateStr).
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
			}).
			Return(nil),
	)

	suite.Equal(suite.reconciler.Running.Load(), false)
	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), true)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)
	suite.reconciler.Stop()
	suite.reconciler.Start()
	suite.reconciler.Start()
	time.Sleep(oneExplicitReconcileRunDelay)
	suite.Equal(suite.reconciler.Running.Load(), true)
	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), false)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), true)
	suite.reconciler.Stop()
	suite.reconciler.Stop()
	time.Sleep(reconcileInterval)
	suite.Equal(suite.reconciler.Running.Load(), false)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)
}
