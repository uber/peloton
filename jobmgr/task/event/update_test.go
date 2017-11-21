package event

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"

	event_mocks "code.uber.internal/infra/peloton/jobmgr/task/event/mocks"
	"code.uber.internal/infra/peloton/jobmgr/tracked/mocks"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
)

const (
	_waitTime          = 1 * time.Second
	_instanceID uint32 = 0
)

var (
	_jobID         = uuid.NewUUID().String()
	_uuidStr       = uuid.NewUUID().String()
	_mesosTaskID   = fmt.Sprintf("%s-%d-%s", _jobID, _instanceID, _uuidStr)
	_mesosReason   = mesos.TaskStatus_REASON_COMMAND_EXECUTOR_FAILED
	_pelotonTaskID = fmt.Sprintf("%s-%d", _jobID, _instanceID)
	_sla           = &job.SlaConfig{
		Preemptible: false,
	}
	_jobConfig = &job.JobConfig{
		Name:          _jobID,
		Sla:           _sla,
		InstanceCount: 1,
	}
	_pelotonJobID = &peloton.JobID{
		Value: _jobID,
	}
	_failureMsg  = "testFailure"
	_currentTime = "2017-01-02T15:04:05.456789016Z"
)

var nowMock = func() time.Time {
	now, _ := time.Parse(time.RFC3339Nano, _currentTime)
	return now
}

type TaskUpdaterTestSuite struct {
	suite.Suite

	updater            *statusUpdate
	ctrl               *gomock.Controller
	testScope          tally.TestScope
	mockJobStore       *store_mocks.MockJobStore
	mockTaskStore      *store_mocks.MockTaskStore
	mockTrackedManager *mocks.MockManager
	mockListener1      *event_mocks.MockListener
	mockListener2      *event_mocks.MockListener
}

func (suite *TaskUpdaterTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.testScope = tally.NewTestScope("", map[string]string{})
	suite.mockJobStore = store_mocks.NewMockJobStore(suite.ctrl)
	suite.mockTaskStore = store_mocks.NewMockTaskStore(suite.ctrl)
	suite.mockTrackedManager = mocks.NewMockManager(suite.ctrl)
	suite.mockListener1 = event_mocks.NewMockListener(suite.ctrl)
	suite.mockListener2 = event_mocks.NewMockListener(suite.ctrl)

	suite.updater = &statusUpdate{
		jobStore:       suite.mockJobStore,
		taskStore:      suite.mockTaskStore,
		listeners:      []Listener{suite.mockListener1, suite.mockListener2},
		trackedManager: suite.mockTrackedManager,
		rootCtx:        context.Background(),
		metrics:        NewMetrics(suite.testScope.SubScope("status_updater")),
	}
}

func (suite *TaskUpdaterTestSuite) TearDownTest() {
	log.Debug("tearing down")
}

func TestPelotonTaskUpdater(t *testing.T) {
	suite.Run(t, new(TaskUpdaterTestSuite))
}

func createTestTaskUpdateEvent(state mesos.TaskState) *pb_eventstream.Event {
	taskStatus := &mesos.TaskStatus{
		TaskId: &mesos.TaskID{
			Value: &_mesosTaskID,
		},
		State:   &state,
		Reason:  &_mesosReason,
		Message: &_failureMsg,
	}
	event := &pb_eventstream.Event{
		MesosTaskStatus: taskStatus,
		Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
	}
	return event
}

func createTestTaskInfo(state task.TaskState) *task.TaskInfo {
	taskInfo := &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			MesosTaskId: &mesos.TaskID{Value: &_mesosTaskID},
			State:       state,
			GoalState:   task.TaskState_SUCCEEDED,
		},
		Config: &task.TaskConfig{
			Name: _jobID,
			RestartPolicy: &task.RestartPolicy{
				MaxFailures: 3,
			},
		},
		InstanceId: uint32(_instanceID),
		JobId:      _pelotonJobID,
	}
	return taskInfo
}

// Test happy case of processing status update.
func (suite *TaskUpdaterTestSuite) TestProcessStatusUpdate() {
	defer suite.ctrl.Finish()

	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_RUNNING)
	taskInfo := createTestTaskInfo(task.TaskState_INITIALIZED)
	updateTaskInfo := createTestTaskInfo(task.TaskState_RUNNING)
	updateTaskInfo.GetRuntime().StartTime = _currentTime

	gomock.InOrder(
		suite.mockTaskStore.EXPECT().
			GetTaskByID(context.Background(), _pelotonTaskID).
			Return(taskInfo, nil),
		suite.mockTrackedManager.EXPECT().
			UpdateTaskRuntime(context.Background(), _pelotonJobID, _instanceID, updateTaskInfo.GetRuntime()).
			Return(nil),
	)

	now = nowMock
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))

	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["status_updater.tasks_running_total+"].Value())
}

func (suite *TaskUpdaterTestSuite) TestProcessStatusUpdateSkipSameState() {
	defer suite.ctrl.Finish()

	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_RUNNING)
	taskInfo := createTestTaskInfo(task.TaskState_RUNNING)

	gomock.InOrder(
		suite.mockTaskStore.EXPECT().
			GetTaskByID(context.Background(), _pelotonTaskID).
			Return(taskInfo, nil),
	)

	now = nowMock
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["status_updater.tasks_running_total+"].Value())
}

// Test processing task failure status update w/ retry.
func (suite *TaskUpdaterTestSuite) TestProcessTaskFailedStatusUpdateWithRetry() {
	defer suite.ctrl.Finish()

	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_FAILED)
	taskInfo := createTestTaskInfo(task.TaskState_RUNNING)

	rescheduleMsg := "Rescheduled due to task failure status: testFailure"
	suite.mockTaskStore.EXPECT().
		GetTaskByID(context.Background(), _pelotonTaskID).
		Return(taskInfo, nil)
	suite.mockTrackedManager.EXPECT().
		UpdateTaskRuntime(context.Background(), _pelotonJobID, uint32(0), gomock.Any()).
		Do(func(ctx context.Context, _, _ interface{}, runtime *task.RuntimeInfo) {
			suite.Equal(
				runtime.State,
				task.TaskState_INITIALIZED,
			)
			suite.Equal(
				runtime.Reason,
				_mesosReason.String(),
			)
			suite.Equal(
				runtime.Message,
				rescheduleMsg,
			)
			suite.Equal(
				runtime.FailureCount,
				uint32(1),
			)
		}).
		Return(nil)
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["status_updater.tasks_failed_total+"].Value())
	time.Sleep(_waitTime)
}

// Test processing task failure status update w/o retry.
func (suite *TaskUpdaterTestSuite) TestProcessTaskFailedStatusUpdateNoRetry() {
	defer suite.ctrl.Finish()

	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_FAILED)
	taskInfo := createTestTaskInfo(task.TaskState_INITIALIZED)
	// Set max failure to be 0 so we don't retry schedule upon task failure.
	taskInfo.GetConfig().GetRestartPolicy().MaxFailures = 0
	updateTaskInfo := createTestTaskInfo(task.TaskState_FAILED)
	updateTaskInfo.GetConfig().GetRestartPolicy().MaxFailures = 0
	updateTaskInfo.GetRuntime().Reason = _mesosReason.String()
	updateTaskInfo.GetRuntime().Message = _failureMsg
	updateTaskInfo.GetRuntime().CompletionTime = _currentTime
	updateTaskInfo.GetRuntime().GoalState = task.TaskState_FAILED

	gomock.InOrder(
		suite.mockTaskStore.EXPECT().
			GetTaskByID(context.Background(), _pelotonTaskID).
			Return(taskInfo, nil),
		suite.mockTrackedManager.EXPECT().
			UpdateTaskRuntime(context.Background(), _pelotonJobID, _instanceID, updateTaskInfo.GetRuntime()).
			Return(nil),
	)

	now = nowMock
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
}

// Test processing task failure due to container launch failure.
func (suite *TaskUpdaterTestSuite) TestProcessTaskFailedSystemFailure() {
	defer suite.ctrl.Finish()

	failureReason := mesos.TaskStatus_REASON_CONTAINER_LAUNCH_FAILED
	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_FAILED)
	event.MesosTaskStatus.Reason = &failureReason
	taskInfo := createTestTaskInfo(task.TaskState_RUNNING)
	taskInfo.GetConfig().GetRestartPolicy().MaxFailures = 0

	rescheduleMsg := "Rescheduled due to task failure status: testFailure"
	suite.mockTaskStore.EXPECT().
		GetTaskByID(context.Background(), _pelotonTaskID).
		Return(taskInfo, nil)
	suite.mockTrackedManager.EXPECT().
		UpdateTaskRuntime(context.Background(), _pelotonJobID, uint32(0), gomock.Any()).
		Do(func(ctx context.Context, JobId *peloton.JobID, _ interface{}, updateTask *task.RuntimeInfo) {
			suite.Equal(JobId, _pelotonJobID)
			suite.Equal(
				updateTask.State,
				task.TaskState_INITIALIZED,
			)
			suite.Equal(
				updateTask.Reason,
				failureReason.String(),
			)
			suite.Equal(
				updateTask.Message,
				rescheduleMsg,
			)
			suite.Equal(
				updateTask.FailureCount,
				uint32(1),
			)
		}).
		Return(nil)
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["status_updater.tasks_failed_total+"].Value())
	time.Sleep(_waitTime)
}

// Test processing task LOST status update w/ retry.
func (suite *TaskUpdaterTestSuite) TestProcessTaskLostStatusUpdateWithRetry() {
	defer suite.ctrl.Finish()

	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_LOST)
	taskInfo := createTestTaskInfo(task.TaskState_RUNNING)

	rescheduleMsg := "Rescheduled due to task LOST: testFailure"
	suite.mockTaskStore.EXPECT().
		GetTaskByID(context.Background(), _pelotonTaskID).
		Return(taskInfo, nil)
	suite.mockTrackedManager.EXPECT().
		UpdateTaskRuntime(context.Background(), _pelotonJobID, uint32(0), gomock.Any()).
		Do(func(ctx context.Context, _, _ interface{}, runtime *task.RuntimeInfo) {
			suite.Equal(
				runtime.State,
				task.TaskState_INITIALIZED,
			)
			suite.Equal(
				runtime.Message,
				rescheduleMsg,
			)
		}).
		Return(nil)
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
	time.Sleep(_waitTime)
}

// Test processing task LOST status update w/o retry due to current state already in terminal state.
func (suite *TaskUpdaterTestSuite) TestProcessTaskLostStatusUpdateWithoutRetry() {
	defer suite.ctrl.Finish()

	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_LOST)
	taskInfo := createTestTaskInfo(task.TaskState_FAILED)

	suite.mockTaskStore.EXPECT().
		GetTaskByID(context.Background(), _pelotonTaskID).
		Return(taskInfo, nil)
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
	time.Sleep(_waitTime)
}

// Test processing orphan task status update.
func (suite *TaskUpdaterTestSuite) TestProcessOrphanTaskStatusUpdate() {
	defer suite.ctrl.Finish()

	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_LOST)
	taskInfo := createTestTaskInfo(task.TaskState_RUNNING)
	// generates new mesos task id that is different with the one in the
	// task status update.
	dbMesosTaskID := fmt.Sprintf("%s-%d-%s", _jobID, _instanceID, uuid.NewUUID().String())
	taskInfo.GetRuntime().MesosTaskId = &mesos.TaskID{Value: &dbMesosTaskID}

	suite.mockTaskStore.EXPECT().
		GetTaskByID(context.Background(), _pelotonTaskID).
		Return(taskInfo, nil)
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
}

// Test processing task KILL by PREEMPTION
func (suite *TaskUpdaterTestSuite) TestProcessPreemptedTaskStatusUpdate() {
	defer suite.ctrl.Finish()

	tt := []struct {
		killOnPreempt  bool
		preemptMessage string
	}{
		{
			killOnPreempt:  false,
			preemptMessage: "Task will be rescheduled",
		},
		{
			killOnPreempt:  true,
			preemptMessage: "Task will not be rescheduled",
		},
	}

	for _, t := range tt {
		event := createTestTaskUpdateEvent(mesos.TaskState_TASK_KILLED)
		taskInfo := createTestTaskInfo(task.TaskState_RUNNING)
		taskInfo.Runtime.GoalState = task.TaskState_PREEMPTING

		taskInfo.Config.PreemptionPolicy = &task.PreemptionPolicy{
			KillOnPreempt: t.killOnPreempt,
		}
		preemptMessage := t.preemptMessage

		preemptReason := "Task preempted"
		oldFailureCount := taskInfo.Runtime.FailureCount
		suite.mockTaskStore.EXPECT().
			GetTaskByID(context.Background(), _pelotonTaskID).
			Return(taskInfo, nil)
		suite.mockTrackedManager.EXPECT().
			UpdateTaskRuntime(context.Background(), _pelotonJobID, uint32(0), gomock.Any()).
			Do(func(ctx context.Context, _, _ interface{}, updateTask *task.RuntimeInfo) {
				suite.Equal(
					preemptMessage,
					updateTask.Message,
				)
				suite.Equal(
					preemptReason,
					updateTask.Reason,
				)
				suite.Equal(
					oldFailureCount,
					updateTask.FailureCount,
				)
			}).
			Return(nil)
		suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
	}
}

func (suite *TaskUpdaterTestSuite) TestUpdaterProcessListeners() {
	defer suite.ctrl.Finish()

	suite.mockListener1.EXPECT().OnEvents([]*pb_eventstream.Event{nil})
	suite.mockListener2.EXPECT().OnEvents([]*pb_eventstream.Event{nil})

	suite.updater.ProcessListeners(nil)
}

func (suite *TaskUpdaterTestSuite) TestUpdaterStart() {
	defer suite.ctrl.Finish()

	suite.mockListener1.EXPECT().Start()
	suite.mockListener2.EXPECT().Start()

	suite.updater.Start()
}

func (suite *TaskUpdaterTestSuite) TestIsErrorState() {
	suite.True(isUnexpected(task.TaskState_FAILED))
	suite.True(isUnexpected(task.TaskState_LOST))

	suite.False(isUnexpected(task.TaskState_KILLED))
	suite.False(isUnexpected(task.TaskState_LAUNCHING))
	suite.False(isUnexpected(task.TaskState_RUNNING))
	suite.False(isUnexpected(task.TaskState_SUCCEEDED))
	suite.False(isUnexpected(task.TaskState_INITIALIZED))
}
