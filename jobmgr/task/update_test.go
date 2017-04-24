package task

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
	"go.uber.org/yarpc"

	mesos "mesos/v1"
	"peloton/api/job"
	"peloton/api/peloton"
	"peloton/api/task"
	"peloton/private/resmgr"
	"peloton/private/resmgrsvc"

	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
	"code.uber.internal/infra/peloton/util"
	yarpc_mocks "code.uber.internal/infra/peloton/vendor_mocks/go.uber.org/yarpc/encoding/json/mocks"
)

const (
	_waitTime = 1 * time.Second
)

type TaskUpdaterTestSuite struct {
	suite.Suite

	updater          *statusUpdate
	ctrl             *gomock.Controller
	testScope        tally.TestScope
	mockResmgrClient *yarpc_mocks.MockClient
	mockJobStore     *store_mocks.MockJobStore
	mockTaskStore    *store_mocks.MockTaskStore
}

func (suite *TaskUpdaterTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.testScope = tally.NewTestScope("", map[string]string{})
	suite.mockResmgrClient = yarpc_mocks.NewMockClient(suite.ctrl)
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

// Test happy case of processing status update.
func (suite *TaskUpdaterTestSuite) TestProcessStatusUpdate() {
	defer suite.ctrl.Finish()

	jobID := uuid.NewUUID().String()
	uuidStr := uuid.NewUUID().String()
	instanceID := 0
	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID, instanceID, uuidStr)
	pelotonTaskID := fmt.Sprintf("%s-%d", jobID, instanceID)
	state := mesos.TaskState_TASK_RUNNING
	taskStatus := &mesos.TaskStatus{
		TaskId: &mesos.TaskID{
			Value: &mesosTaskID,
		},
		State: &state,
	}
	taskInfo := &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			TaskId:    &mesos.TaskID{Value: &mesosTaskID},
			State:     task.TaskState_INITIALIZED,
			GoalState: task.TaskState_SUCCEEDED,
		},
	}
	updateTaskInfo := &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			TaskId:    &mesos.TaskID{Value: &mesosTaskID},
			State:     task.TaskState_RUNNING,
			GoalState: task.TaskState_SUCCEEDED,
		},
	}

	gomock.InOrder(
		suite.mockTaskStore.EXPECT().
			GetTaskByID(pelotonTaskID).
			Return(taskInfo, nil),
		suite.mockTaskStore.EXPECT().
			UpdateTask(updateTaskInfo).
			Return(nil),
	)
	suite.NoError(suite.updater.ProcessStatusUpdate(taskStatus))
}

// Test processing task failure status update w/ retry.
func (suite *TaskUpdaterTestSuite) TestProcessTaskFailedStatusUpdateWithRetry() {
	defer suite.ctrl.Finish()

	jobID := uuid.NewUUID().String()
	uuidStr := uuid.NewUUID().String()
	instanceID := 0
	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID, instanceID, uuidStr)
	pelotonTaskID := fmt.Sprintf("%s-%d", jobID, instanceID)
	state := mesos.TaskState_TASK_FAILED
	mesosReason := mesos.TaskStatus_REASON_CONTAINER_LAUNCH_FAILED
	failureMsg := "testFailure"
	taskStatus := &mesos.TaskStatus{
		TaskId: &mesos.TaskID{
			Value: &mesosTaskID,
		},
		State:   &state,
		Reason:  &mesosReason,
		Message: &failureMsg,
	}
	sla := &job.SlaConfig{
		Preemptible: false,
	}
	jobConfig := &job.JobConfig{
		Name:          jobID,
		Sla:           sla,
		InstanceCount: 1,
	}
	pelotonJobID := &peloton.JobID{
		Value: jobID,
	}
	taskInfo := &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			TaskId:    &mesos.TaskID{Value: &mesosTaskID},
			State:     task.TaskState_RUNNING,
			GoalState: task.TaskState_SUCCEEDED,
		},
		Config: &task.TaskConfig{
			Name:            jobID,
			MaxTaskFailures: 3,
		},
		InstanceId: uint32(instanceID),
		JobId: &peloton.JobID{
			Value: jobID,
		},
	}

	var resmgrTasks []*resmgr.Task
	resmgrTasks = append(
		resmgrTasks,
		util.ConvertTaskToResMgrTask(taskInfo, jobConfig),
	)
	enqueueTasksStr := "ResourceManagerService.EnqueueTasks"
	rescheduleMsg := "Rescheduled due to task failure status: testFailure"
	suite.mockTaskStore.EXPECT().
		GetTaskByID(pelotonTaskID).
		Return(taskInfo, nil)
	suite.mockJobStore.EXPECT().
		GetJobConfig(pelotonJobID).
		Return(jobConfig, nil)
	suite.mockResmgrClient.EXPECT().
		Call(
			gomock.Any(),
			gomock.Eq(yarpc.NewReqMeta().Procedure(enqueueTasksStr)),
			gomock.Eq(&resmgrsvc.EnqueueTasksRequest{
				Tasks: resmgrTasks,
			}),
			gomock.Any()).
		Return(nil, nil)
	suite.mockTaskStore.EXPECT().
		UpdateTask(gomock.Any()).
		Do(func(updateTask *task.TaskInfo) {
			suite.Equal(updateTask.JobId, pelotonJobID)
			suite.Equal(
				updateTask.Runtime.State,
				task.TaskState_INITIALIZED,
			)
			suite.Equal(
				updateTask.Runtime.Reason,
				mesosReason.String(),
			)
			suite.Equal(
				updateTask.Runtime.Message,
				rescheduleMsg,
			)
			suite.Equal(
				updateTask.Runtime.TaskFailuresCount,
				uint32(1),
			)
		}).
		Return(nil)
	suite.NoError(suite.updater.ProcessStatusUpdate(taskStatus))
	time.Sleep(_waitTime)
}

// Test processing task failure status update w/o retry.
func (suite *TaskUpdaterTestSuite) TestProcessTaskFailedStatusUpdateNoRetry() {
	defer suite.ctrl.Finish()

	jobID := uuid.NewUUID().String()
	uuidStr := uuid.NewUUID().String()
	instanceID := 0
	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID, instanceID, uuidStr)
	pelotonTaskID := fmt.Sprintf("%s-%d", jobID, instanceID)
	state := mesos.TaskState_TASK_FAILED
	mesosReason := mesos.TaskStatus_REASON_CONTAINER_LAUNCH_FAILED
	taskStatus := &mesos.TaskStatus{
		TaskId: &mesos.TaskID{
			Value: &mesosTaskID,
		},
		State:  &state,
		Reason: &mesosReason,
	}
	taskInfo := &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			TaskId:    &mesos.TaskID{Value: &mesosTaskID},
			State:     task.TaskState_INITIALIZED,
			GoalState: task.TaskState_SUCCEEDED,
		},
	}
	updateTaskInfo := &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			TaskId:    &mesos.TaskID{Value: &mesosTaskID},
			State:     task.TaskState_FAILED,
			GoalState: task.TaskState_SUCCEEDED,
			Reason:    mesosReason.String(),
		},
	}

	gomock.InOrder(
		suite.mockTaskStore.EXPECT().
			GetTaskByID(pelotonTaskID).
			Return(taskInfo, nil),
		suite.mockTaskStore.EXPECT().
			UpdateTask(updateTaskInfo).
			Return(nil),
	)
	suite.NoError(suite.updater.ProcessStatusUpdate(taskStatus))
}

// Test processing task failure status update w/ retry.
func (suite *TaskUpdaterTestSuite) TestProcessTaskFailedRetryDBFailure() {
	defer suite.ctrl.Finish()

	jobID := uuid.NewUUID().String()
	uuidStr := uuid.NewUUID().String()
	instanceID := 0
	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID, instanceID, uuidStr)
	pelotonTaskID := fmt.Sprintf("%s-%d", jobID, instanceID)
	state := mesos.TaskState_TASK_FAILED
	mesosReason := mesos.TaskStatus_REASON_CONTAINER_LAUNCH_FAILED
	failureMsg := "testFailure"
	taskStatus := &mesos.TaskStatus{
		TaskId: &mesos.TaskID{
			Value: &mesosTaskID,
		},
		State:   &state,
		Reason:  &mesosReason,
		Message: &failureMsg,
	}
	sla := &job.SlaConfig{
		Preemptible: false,
	}
	jobConfig := &job.JobConfig{
		Name:          jobID,
		Sla:           sla,
		InstanceCount: 1,
	}
	pelotonJobID := &peloton.JobID{
		Value: jobID,
	}
	taskInfo := &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			TaskId:    &mesos.TaskID{Value: &mesosTaskID},
			State:     task.TaskState_RUNNING,
			GoalState: task.TaskState_SUCCEEDED,
		},
		Config: &task.TaskConfig{
			Name:            jobID,
			MaxTaskFailures: 3,
		},
		InstanceId: uint32(instanceID),
		JobId: &peloton.JobID{
			Value: jobID,
		},
	}

	var resmgrTasks []*resmgr.Task
	resmgrTasks = append(
		resmgrTasks,
		util.ConvertTaskToResMgrTask(taskInfo, jobConfig),
	)
	rescheduleMsg := "Rescheduled due to task failure status: testFailure"

	suite.mockTaskStore.EXPECT().
		GetTaskByID(pelotonTaskID).
		Return(taskInfo, nil)
	suite.mockJobStore.EXPECT().
		GetJobConfig(pelotonJobID).
		Return(jobConfig, errors.New("testError"))
	suite.mockTaskStore.EXPECT().
		UpdateTask(gomock.Any()).
		Do(func(updateTask *task.TaskInfo) {
			suite.Equal(updateTask.JobId, pelotonJobID)
			suite.Equal(
				updateTask.Runtime.State,
				task.TaskState_INITIALIZED,
			)
			suite.Equal(
				updateTask.Runtime.Reason,
				mesosReason.String(),
			)
			suite.Equal(
				updateTask.Runtime.Message,
				rescheduleMsg,
			)
			suite.Equal(
				updateTask.Runtime.TaskFailuresCount,
				uint32(1),
			)
		}).
		Return(nil)
	suite.NoError(suite.updater.ProcessStatusUpdate(taskStatus))
	time.Sleep(_waitTime)
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
