package reconcile

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/atomic"
	"github.com/uber-go/tally"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	sched "code.uber.internal/infra/peloton/.gen/mesos/v1/scheduler"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"

	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
	"code.uber.internal/infra/peloton/util"
	mock_mpb "code.uber.internal/infra/peloton/yarpc/encoding/mpb/mocks"
	"github.com/pborman/uuid"
)

const (
	streamID                       = "streamID"
	frameworkID                    = "frameworkID"
	testInstanceCount              = 5
	testJobID                      = "testJob0"
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

	running         atomic.Bool
	ctrl            *gomock.Controller
	testScope       tally.TestScope
	schedulerClient *mock_mpb.MockSchedulerClient
	reconciler      *taskReconciler
	mockJobStore    *store_mocks.MockJobStore
	mockTaskStore   *store_mocks.MockTaskStore
	testJobID       *peloton.JobID
	testJobConfig   *job.JobConfig
	allJobRuntime   map[string]*job.RuntimeInfo
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
	suite.allJobRuntime = make(map[string]*job.RuntimeInfo)
	suite.allJobRuntime[testJobID] = &job.RuntimeInfo{}
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
		explicitReconcileBatchInterval: explicitReconcileBatchInterval,
		explicitReconcileBatchSize:     testBatchSize,
	}
	suite.reconciler.isExplicitReconcileTurn.Store(true)
}

func (suite *TaskReconcilerTestSuite) TearDownTest() {
	log.Debug("tearing down")
}

func (suite *TaskReconcilerTestSuite) createTestTaskInfo(
	state task.TaskState,
	instanceID uint32) *task.TaskInfo {

	mtID := util.BuildMesosTaskID(util.BuildTaskID(suite.testJobID, instanceID), uuid.NewUUID().String())

	return &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			MesosTaskId: mtID,
			State:       state,
			GoalState:   task.TaskGoalState_SUCCEED,
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
			GetAllJobs(context.Background()).Return(suite.allJobRuntime, nil),
		suite.mockTaskStore.EXPECT().
			GetTasksForJobAndState(context.Background(), suite.testJobID,
				strconv.Itoa(int(task.TaskState_value[task.TaskState_RUNNING.String()]))).
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

	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), true)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)
	suite.running.Store(true)
	// Run in a different goroutine.
	go suite.reconciler.Reconcile(&suite.running)
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
}

func (suite *TaskReconcilerTestSuite) TestTaskReconcilationCallFailure() {
	defer suite.ctrl.Finish()
	gomock.InOrder(
		suite.mockJobStore.EXPECT().
			GetAllJobs(context.Background()).Return(suite.allJobRuntime, nil),
		suite.mockTaskStore.EXPECT().
			GetTasksForJobAndState(context.Background(), suite.testJobID,
				strconv.Itoa(int(task.TaskState_value[task.TaskState_RUNNING.String()]))).
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

	suite.running.Store(true)

	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), true)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)
	go suite.reconciler.Reconcile(&suite.running)
	time.Sleep(oneExplicitReconcileRunDelay)
	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), false)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)
	suite.reconciler.Reconcile(&suite.running)
	time.Sleep(oneExplicitReconcileRunDelay)
	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), true)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)
}

func (suite *TaskReconcilerTestSuite) TestReconcilerNotStartIfAlreadyRunning() {
	defer suite.ctrl.Finish()
	gomock.InOrder(
		suite.mockJobStore.EXPECT().
			GetAllJobs(context.Background()).Return(suite.allJobRuntime, nil),
		suite.mockTaskStore.EXPECT().
			GetTasksForJobAndState(context.Background(), suite.testJobID,
				strconv.Itoa(int(task.TaskState_value[task.TaskState_RUNNING.String()]))).
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

	suite.running.Store(true)

	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), true)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)

	suite.reconciler.Reconcile(&suite.running)
	time.Sleep(explicitReconcileBatchInterval / 2)

	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), false)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), true)

	suite.running.Store(false)
	time.Sleep(oneExplicitReconcileRunDelay)
	suite.Equal(suite.reconciler.isExplicitReconcileTurn.Load(), false)
	suite.Equal(suite.reconciler.isExplicitReconcileRunning.Load(), false)
}
