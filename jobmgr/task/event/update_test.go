package event

import (
	"context"
	"fmt"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	res_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
	"code.uber.internal/infra/peloton/util"
)

const (
	_waitTime   = 1 * time.Second
	_instanceID = 0
)

var (
	_jobID         = uuid.NewUUID().String()
	_uuidStr       = uuid.NewUUID().String()
	_mesosTaskID   = fmt.Sprintf("%s-%d-%s", _jobID, _instanceID, _uuidStr)
	_mesosReason   = mesos.TaskStatus_REASON_CONTAINER_LAUNCH_FAILED
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
	_failureMsg = "testFailure"
)

type TaskUpdaterTestSuite struct {
	suite.Suite

	updater          *statusUpdate
	ctrl             *gomock.Controller
	testScope        tally.TestScope
	mockResmgrClient *res_mocks.MockResourceManagerServiceYarpcClient
	mockJobStore     *store_mocks.MockJobStore
	mockTaskStore    *store_mocks.MockTaskStore
}

func (suite *TaskUpdaterTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.testScope = tally.NewTestScope("", map[string]string{})
	suite.mockResmgrClient = res_mocks.NewMockResourceManagerServiceYarpcClient(suite.ctrl)
	suite.mockJobStore = store_mocks.NewMockJobStore(suite.ctrl)
	suite.mockTaskStore = store_mocks.NewMockTaskStore(suite.ctrl)
	suite.testScope = tally.NewTestScope("", map[string]string{})

	suite.updater = &statusUpdate{
		jobStore:     suite.mockJobStore,
		taskStore:    suite.mockTaskStore,
		rootCtx:      context.Background(),
		resmgrClient: suite.mockResmgrClient,
		metrics:      NewMetrics(suite.testScope),
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
			TaskId:    &mesos.TaskID{Value: &_mesosTaskID},
			State:     state,
			GoalState: task.TaskState_SUCCEEDED,
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

	gomock.InOrder(
		suite.mockTaskStore.EXPECT().
			GetTaskByID(context.Background(), _pelotonTaskID).
			Return(taskInfo, nil),
		suite.mockTaskStore.EXPECT().
			UpdateTask(context.Background(), updateTaskInfo).
			Return(nil),
	)
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
}

// Test processing task failure status update w/ retry.
func (suite *TaskUpdaterTestSuite) TestProcessTaskFailedStatusUpdateWithRetry() {
	defer suite.ctrl.Finish()

	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_FAILED)
	taskInfo := createTestTaskInfo(task.TaskState_RUNNING)

	tasks := []*task.TaskInfo{taskInfo}
	gangs := util.ConvertToResMgrGangs(tasks, _jobConfig)
	rescheduleMsg := "Rescheduled due to task failure status: testFailure"
	suite.mockTaskStore.EXPECT().
		GetTaskByID(context.Background(), _pelotonTaskID).
		Return(taskInfo, nil)
	suite.mockJobStore.EXPECT().
		GetJobConfig(context.Background(), _pelotonJobID).
		Return(_jobConfig, nil)
	suite.mockResmgrClient.EXPECT().
		EnqueueGangs(
			gomock.Any(),
			gomock.Eq(&resmgrsvc.EnqueueGangsRequest{
				Gangs: gangs,
			})).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil)
	suite.mockTaskStore.EXPECT().
		UpdateTask(context.Background(), gomock.Any()).
		Do(func(ctx context.Context, updateTask *task.TaskInfo) {
			suite.Equal(updateTask.JobId, _pelotonJobID)
			suite.Equal(
				updateTask.Runtime.State,
				task.TaskState_INITIALIZED,
			)
			suite.Equal(
				updateTask.Runtime.Reason,
				_mesosReason.String(),
			)
			suite.Equal(
				updateTask.Runtime.Message,
				rescheduleMsg,
			)
			suite.Equal(
				updateTask.Runtime.FailuresCount,
				uint32(1),
			)
		}).
		Return(nil)
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
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

	gomock.InOrder(
		suite.mockTaskStore.EXPECT().
			GetTaskByID(context.Background(), _pelotonTaskID).
			Return(taskInfo, nil),
		suite.mockTaskStore.EXPECT().
			UpdateTask(context.Background(), updateTaskInfo).
			Return(nil),
	)
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
}

// Test processing task failure status update w/ retry.
func (suite *TaskUpdaterTestSuite) TestProcessTaskFailedRetryDBFailure() {
	defer suite.ctrl.Finish()

	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_FAILED)
	taskInfo := createTestTaskInfo(task.TaskState_RUNNING)

	var resmgrTasks []*resmgr.Task
	resmgrTasks = append(
		resmgrTasks,
		util.ConvertTaskToResMgrTask(taskInfo, _jobConfig),
	)
	rescheduleMsg := "Rescheduled due to task failure status: testFailure"

	suite.mockTaskStore.EXPECT().
		GetTaskByID(context.Background(), _pelotonTaskID).
		Return(taskInfo, nil)
	suite.mockJobStore.EXPECT().
		GetJobConfig(context.Background(), _pelotonJobID).
		Return(_jobConfig, errors.New("testError"))
	suite.mockTaskStore.EXPECT().
		UpdateTask(context.Background(), gomock.Any()).
		Do(func(ctx context.Context, updateTask *task.TaskInfo) {
			suite.Equal(updateTask.JobId, _pelotonJobID)
			suite.Equal(
				updateTask.Runtime.State,
				task.TaskState_INITIALIZED,
			)
			suite.Equal(
				updateTask.Runtime.Reason,
				_mesosReason.String(),
			)
			suite.Equal(
				updateTask.Runtime.Message,
				rescheduleMsg,
			)
			suite.Equal(
				updateTask.Runtime.FailuresCount,
				uint32(1),
			)
		}).
		Return(nil)
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
	time.Sleep(_waitTime)
}

// Test processing task LOST status update w/ retry.
func (suite *TaskUpdaterTestSuite) TestProcessTaskLostStatusUpdateWithRetry() {
	defer suite.ctrl.Finish()

	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_LOST)
	taskInfo := createTestTaskInfo(task.TaskState_RUNNING)

	tasks := []*task.TaskInfo{taskInfo}
	gangs := util.ConvertToResMgrGangs(tasks, _jobConfig)
	rescheduleMsg := "Rescheduled due to task LOST: testFailure"
	suite.mockTaskStore.EXPECT().
		GetTaskByID(context.Background(), _pelotonTaskID).
		Return(taskInfo, nil)
	suite.mockJobStore.EXPECT().
		GetJobConfig(context.Background(), _pelotonJobID).
		Return(_jobConfig, nil)
	suite.mockResmgrClient.EXPECT().
		EnqueueGangs(
			gomock.Any(),
			gomock.Eq(&resmgrsvc.EnqueueGangsRequest{
				Gangs: gangs,
			})).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil)
	suite.mockTaskStore.EXPECT().
		UpdateTask(context.Background(), gomock.Any()).
		Do(func(ctx context.Context, updateTask *task.TaskInfo) {
			suite.Equal(updateTask.JobId, _pelotonJobID)
			suite.Equal(
				updateTask.Runtime.State,
				task.TaskState_INITIALIZED,
			)
			suite.Equal(
				updateTask.Runtime.Message,
				rescheduleMsg,
			)
		}).
		Return(nil)
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
	taskInfo.GetRuntime().TaskId = &mesos.TaskID{Value: &dbMesosTaskID}

	suite.mockTaskStore.EXPECT().
		GetTaskByID(context.Background(), _pelotonTaskID).
		Return(taskInfo, nil)
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
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
