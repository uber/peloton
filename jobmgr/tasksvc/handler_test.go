package tasksvc

import (
	"context"
	"fmt"
	"testing"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	host_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	res_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"
	logmanager_mocks "code.uber.internal/infra/peloton/jobmgr/logmanager/mocks"
	"code.uber.internal/infra/peloton/jobmgr/tracked"
	"code.uber.internal/infra/peloton/jobmgr/tracked/mocks"
	storage_mocks "code.uber.internal/infra/peloton/storage/mocks"
	"code.uber.internal/infra/peloton/util"
	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	"github.com/pborman/uuid"
)

const (
	testInstanceCount = 4
)

type TaskHandlerTestSuite struct {
	suite.Suite
	handler        *serviceHandler
	testJobID      *peloton.JobID
	testJobConfig  *job.JobConfig
	testJobRuntime *job.RuntimeInfo
	taskInfos      map[uint32]*task.TaskInfo
}

func (suite *TaskHandlerTestSuite) SetupTest() {
	mtx := NewMetrics(tally.NoopScope)
	suite.handler = &serviceHandler{
		metrics: mtx,
	}
	suite.testJobID = &peloton.JobID{
		Value: "test_job",
	}
	suite.testJobConfig = &job.JobConfig{
		Name:          suite.testJobID.Value,
		InstanceCount: testInstanceCount,
	}
	suite.testJobRuntime = &job.RuntimeInfo{
		State:     job.JobState_RUNNING,
		GoalState: job.JobState_SUCCEEDED,
	}
	var taskInfos = make(map[uint32]*task.TaskInfo)
	for i := uint32(0); i < testInstanceCount; i++ {
		taskInfos[i] = suite.createTestTaskInfo(
			task.TaskState_RUNNING, i)
	}
	suite.taskInfos = taskInfos
}

func (suite *TaskHandlerTestSuite) TearDownTest() {
	log.Debug("tearing down")
	suite.handler.mesosAgentWorkDir = ""
}

func TestPelotonTaskHandler(t *testing.T) {
	suite.Run(t, new(TaskHandlerTestSuite))
}

func (suite *TaskHandlerTestSuite) createTestTaskInfo(
	state task.TaskState,
	instanceID uint32) *task.TaskInfo {

	var taskID = fmt.Sprintf("%s-%d-%s", suite.testJobID.Value, instanceID, uuid.New())
	return &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			MesosTaskId: &mesos.TaskID{Value: &taskID},
			State:       state,
			GoalState:   task.TaskState_SUCCEEDED,
		},
		Config: &task.TaskConfig{
			RestartPolicy: &task.RestartPolicy{
				MaxFailures: 3,
			},
		},
		InstanceId: instanceID,
		JobId:      suite.testJobID,
	}
}

func (suite *TaskHandlerTestSuite) createTestTaskEvents() []*task.TaskEvent {
	var taskID0 = fmt.Sprintf("%s-%d", suite.testJobID.Value, 0)
	var taskID1 = fmt.Sprintf("%s-%d", suite.testJobID.Value, 1)
	return []*task.TaskEvent{
		{
			TaskId: &peloton.TaskID{
				Value: taskID0,
			},
			State:     task.TaskState_INITIALIZED,
			Message:   "",
			Timestamp: "2017-12-11T22:17:26Z",
			Hostname:  "peloton-test-host",
			Reason:    "",
		},
		{
			TaskId: &peloton.TaskID{
				Value: taskID1,
			},
			State:     task.TaskState_INITIALIZED,
			Message:   "",
			Timestamp: "2017-12-11T22:17:46Z",
			Hostname:  "peloton-test-host-1",
			Reason:    "",
		},
		{
			TaskId: &peloton.TaskID{
				Value: taskID0,
			},
			State:     task.TaskState_FAILED,
			Message:   "",
			Timestamp: "2017-12-11T22:17:36Z",
			Hostname:  "peloton-test-host",
			Reason:    "",
		},
		{
			TaskId: &peloton.TaskID{
				Value: taskID1,
			},
			State:     task.TaskState_LAUNCHED,
			Message:   "",
			Timestamp: "2017-12-11T22:17:50Z",
			Hostname:  "peloton-test-host-1",
			Reason:    "",
		},
		{
			TaskId: &peloton.TaskID{
				Value: taskID1,
			},
			State:     task.TaskState_RUNNING,
			Message:   "",
			Timestamp: "2017-12-11T22:17:56Z",
			Hostname:  "peloton-test-host-1",
			Reason:    "",
		},
	}
}

func (suite *TaskHandlerTestSuite) TestGetTaskInfosByRangesFromDBReturnsError() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore

	jobID := &peloton.JobID{}

	mockTaskStore.EXPECT().GetTasksForJob(gomock.Any(), jobID).Return(nil, errors.New("my-error"))
	_, err := suite.handler.getTaskInfosByRangesFromDB(context.Background(), jobID, nil, nil)

	suite.EqualError(err, "my-error")
}

func (suite *TaskHandlerTestSuite) createTaskEventForGetTasks(instanceID uint32, taskRuns uint32) ([]*task.TaskEvent, []*task.TaskEvent) {
	var events []*task.TaskEvent
	var getReturnEvents []*task.TaskEvent
	taskInfos := make([]*task.TaskInfo, taskRuns)
	for i := uint32(0); i < taskRuns; i++ {
		var prevTaskID *peloton.TaskID
		taskInfos[i] = suite.createTestTaskInfo(task.TaskState_FAILED, instanceID)
		if i > uint32(0) {
			prevTaskID = &peloton.TaskID{
				Value: taskInfos[i-1].GetRuntime().GetMesosTaskId().GetValue(),
			}
		}

		event := &task.TaskEvent{
			TaskId: &peloton.TaskID{
				Value: taskInfos[i].GetRuntime().GetMesosTaskId().GetValue(),
			},
			PrevTaskId: prevTaskID,
			State:      task.TaskState_PENDING,
			Message:    "",
			Timestamp:  "2017-12-11T22:17:26Z",
			Hostname:   "peloton-test-host",
			Reason:     "",
		}
		events = append(events, event)

		event = &task.TaskEvent{
			TaskId: &peloton.TaskID{
				Value: taskInfos[i].GetRuntime().GetMesosTaskId().GetValue(),
			},
			PrevTaskId: prevTaskID,
			State:      task.TaskState_RUNNING,
			Message:    "",
			Timestamp:  "2017-12-11T22:17:26Z",
			Hostname:   "peloton-test-host",
			Reason:     "",
		}
		events = append(events, event)

		if i == uint32(0) {
			event = &task.TaskEvent{
				TaskId: &peloton.TaskID{
					Value: taskInfos[i].GetRuntime().GetMesosTaskId().GetValue(),
				},
				PrevTaskId: prevTaskID,
				State:      task.TaskState_FAILED,
				Message:    "",
				Timestamp:  "2017-12-11T22:17:26Z",
				Hostname:   "peloton-test-host",
				Reason:     "",
			}
		} else {
			event = &task.TaskEvent{
				TaskId: &peloton.TaskID{
					Value: taskInfos[i].GetRuntime().GetMesosTaskId().GetValue(),
				},
				PrevTaskId: prevTaskID,
				State:      task.TaskState_FAILED,
				Message:    "",
				Timestamp:  "2017-12-11T22:17:26Z",
				Hostname:   "peloton-test-host",
				Reason:     "",
				AgentId:    "peloton-test-agent",
			}
		}

		events = append(events, event)
		getReturnEvents = append(getReturnEvents, event)
	}

	return events, getReturnEvents
}

func (suite *TaskHandlerTestSuite) TestGetTasks() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore

	instanceID := uint32(0)
	taskRuns := uint32(3)
	lastTaskInfo := suite.createTestTaskInfo(task.TaskState_FAILED, instanceID)
	taskInfoMap := make(map[uint32]*task.TaskInfo)
	taskInfoMap[instanceID] = lastTaskInfo
	events, _ := suite.createTaskEventForGetTasks(instanceID, taskRuns)

	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJobConfig(gomock.Any(), suite.testJobID).Return(suite.testJobConfig, nil),
		mockTaskStore.EXPECT().
			GetTaskForJob(gomock.Any(), suite.testJobID, instanceID).Return(taskInfoMap, nil),
		mockTaskStore.EXPECT().
			GetTaskEvents(gomock.Any(), suite.testJobID, instanceID).Return(events, nil),
	)

	var req = &task.GetRequest{
		JobId:      suite.testJobID,
		InstanceId: instanceID,
	}

	resp, err := suite.handler.Get(context.Background(), req)
	suite.NoError(err)
	suite.Equal(uint32(len(resp.Results)), taskRuns)
	for _, result := range resp.Results {
		suite.Equal(result.GetRuntime().GetState(), task.TaskState_FAILED)
	}
}

func (suite *TaskHandlerTestSuite) TestStopAllTasks() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore
	trackedMock := mocks.NewMockManager(ctrl)
	suite.handler.trackedManager = trackedMock

	expectedTaskIds := make(map[*mesos.TaskID]bool)
	for _, taskInfo := range suite.taskInfos {
		expectedTaskIds[taskInfo.GetRuntime().GetMesosTaskId()] = true
	}

	expectedJobRuntime := &job.RuntimeInfo{
		State:     job.JobState_RUNNING,
		GoalState: job.JobState_KILLED,
	}

	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJobConfig(gomock.Any(), suite.testJobID).Return(suite.testJobConfig, nil),
		mockJobStore.EXPECT().
			GetJobRuntime(gomock.Any(), suite.testJobID).Return(suite.testJobRuntime, nil),
		trackedMock.EXPECT().
			UpdateJobRuntime(gomock.Any(), suite.testJobID, expectedJobRuntime, suite.testJobConfig).Return(nil),
	)

	var request = &task.StopRequest{
		JobId: suite.testJobID,
	}
	resp, err := suite.handler.Stop(
		context.Background(),
		request,
	)
	suite.NoError(err)
	suite.Equal(len(resp.GetInvalidInstanceIds()), 0)
	suite.Equal(len(resp.GetStoppedInstanceIds()), testInstanceCount)
}

func (suite *TaskHandlerTestSuite) TestStopTasksWithRanges() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore
	trackedMock := mocks.NewMockManager(ctrl)
	suite.handler.trackedManager = trackedMock

	singleTaskInfo := make(map[uint32]*task.TaskInfo)
	singleTaskInfo[1] = suite.taskInfos[1]
	singleRuntime := make(map[uint32]*task.RuntimeInfo)
	singleRuntime[1] = suite.taskInfos[1].Runtime

	taskRanges := []*task.InstanceRange{
		{
			From: 1,
			To:   2,
		},
	}

	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJobConfig(gomock.Any(), suite.testJobID).Return(suite.testJobConfig, nil),
		mockTaskStore.EXPECT().
			GetTasksForJobByRange(gomock.Any(), suite.testJobID, taskRanges[0]).Return(singleTaskInfo, nil),
		trackedMock.EXPECT().UpdateTaskRuntimes(gomock.Any(), suite.taskInfos[1].GetJobId(), singleRuntime, tracked.UpdateAndSchedule).Return(nil),
	)

	var request = &task.StopRequest{
		JobId:  suite.testJobID,
		Ranges: taskRanges,
	}
	resp, err := suite.handler.Stop(
		context.Background(),
		request,
	)
	suite.NoError(err)
	suite.Equal(len(resp.GetInvalidInstanceIds()), 0)
	suite.Equal(resp.GetStoppedInstanceIds(), []uint32{1})
}

func (suite *TaskHandlerTestSuite) TestStopTasksSkipKillNotRunningTask() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore
	trackedMock := mocks.NewMockManager(ctrl)
	suite.handler.trackedManager = trackedMock

	taskInfos := make(map[uint32]*task.TaskInfo)
	taskInfos[1] = suite.taskInfos[1]
	taskInfos[2] = suite.createTestTaskInfo(task.TaskState_FAILED, uint32(2))

	taskRanges := []*task.InstanceRange{
		{
			From: 1,
			To:   3,
		},
	}

	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJobConfig(gomock.Any(), suite.testJobID).Return(suite.testJobConfig, nil),
		mockTaskStore.EXPECT().
			GetTasksForJobByRange(gomock.Any(), suite.testJobID, taskRanges[0]).Return(taskInfos, nil),
		trackedMock.EXPECT().UpdateTaskRuntimes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil),
	)

	var request = &task.StopRequest{
		JobId:  suite.testJobID,
		Ranges: taskRanges,
	}
	resp, err := suite.handler.Stop(
		context.Background(),
		request,
	)
	suite.NoError(err)
	suite.Equal(len(resp.GetInvalidInstanceIds()), 0)
	suite.Equal(len(resp.GetStoppedInstanceIds()), 2)
}

func (suite *TaskHandlerTestSuite) TestStopTasksWithInvalidRanges() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore

	singleTaskInfo := make(map[uint32]*task.TaskInfo)
	singleTaskInfo[1] = suite.taskInfos[1]
	emptyTaskInfo := make(map[uint32]*task.TaskInfo)

	taskRanges := []*task.InstanceRange{
		{
			From: 1,
			To:   2,
		},
		{
			From: 5,
			To:   testInstanceCount + 1,
		},
	}
	correctedRange := &task.InstanceRange{
		From: 5,
		To:   testInstanceCount,
	}

	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJobConfig(gomock.Any(), suite.testJobID).Return(suite.testJobConfig, nil),
		mockTaskStore.EXPECT().
			GetTasksForJobByRange(gomock.Any(), suite.testJobID, taskRanges[0]).Return(singleTaskInfo, nil),
		mockTaskStore.EXPECT().
			GetTasksForJobByRange(gomock.Any(), suite.testJobID, correctedRange).
			Return(emptyTaskInfo, errors.New("test error")),
	)

	var request = &task.StopRequest{
		JobId:  suite.testJobID,
		Ranges: taskRanges,
	}
	resp, err := suite.handler.Stop(
		context.Background(),
		request,
	)
	suite.NoError(err)
	suite.Equal(len(resp.GetStoppedInstanceIds()), 0)
	suite.Equal(resp.GetError().GetOutOfRange().GetJobId().GetValue(), "test_job")
	suite.Equal(
		resp.GetError().GetOutOfRange().GetInstanceCount(),
		uint32(testInstanceCount))
}

func (suite *TaskHandlerTestSuite) TestStopTasksWithInvalidJobID() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore

	singleTaskInfo := make(map[uint32]*task.TaskInfo)
	singleTaskInfo[1] = suite.taskInfos[1]
	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJobConfig(gomock.Any(), suite.testJobID).Return(nil, errors.New("test error")),
	)

	var request = &task.StopRequest{
		JobId: suite.testJobID,
	}
	resp, err := suite.handler.Stop(
		context.Background(),
		request,
	)
	suite.NoError(err)
	suite.Equal(resp.GetError().GetNotFound().GetId().GetValue(), "test_job")
	suite.Equal(len(resp.GetInvalidInstanceIds()), 0)
	suite.Equal(len(resp.GetStoppedInstanceIds()), 0)
}

func (suite *TaskHandlerTestSuite) TestStopAllTasksWithUpdateFailure() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore
	trackedMock := mocks.NewMockManager(ctrl)
	suite.handler.trackedManager = trackedMock

	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJobConfig(gomock.Any(), suite.testJobID).Return(suite.testJobConfig, nil),
		mockJobStore.EXPECT().
			GetJobRuntime(gomock.Any(), suite.testJobID).Return(suite.testJobRuntime, nil),
		trackedMock.EXPECT().
			UpdateJobRuntime(gomock.Any(), suite.testJobID, gomock.Any(), suite.testJobConfig).Return(fmt.Errorf("db update failure")),
	)

	var request = &task.StopRequest{
		JobId: suite.testJobID,
	}
	resp, err := suite.handler.Stop(
		context.Background(),
		request,
	)
	suite.NoError(err)
	suite.Equal(len(resp.GetInvalidInstanceIds()), testInstanceCount)
	suite.Equal(len(resp.GetStoppedInstanceIds()), 0)
	suite.NotNil(resp.GetError().GetUpdateError())
}

func (suite *TaskHandlerTestSuite) TestStartAllTasks() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockResmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	suite.handler.resmgrClient = mockResmgrClient
	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore
	trackedMock := mocks.NewMockManager(ctrl)
	suite.handler.trackedManager = trackedMock

	expectedTaskIds := make(map[*mesos.TaskID]bool)
	for _, taskInfo := range suite.taskInfos {
		expectedTaskIds[taskInfo.GetRuntime().GetMesosTaskId()] = true
	}

	var taskInfos = make(map[uint32]*task.TaskInfo)
	var tasksInfoList []*task.TaskInfo
	for i := uint32(0); i < testInstanceCount; i++ {
		taskInfos[i] = suite.createTestTaskInfo(
			task.TaskState_FAILED, i)
		tasksInfoList = append(tasksInfoList, taskInfos[i])
	}

	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJobConfig(gomock.Any(), suite.testJobID).Return(suite.testJobConfig, nil),
		mockJobStore.EXPECT().
			GetJobRuntime(gomock.Any(), suite.testJobID).Return(suite.testJobRuntime, nil),
		mockJobStore.EXPECT().
			UpdateJobRuntime(gomock.Any(), suite.testJobID, gomock.Any()).Return(nil),
		mockTaskStore.EXPECT().
			GetTasksForJob(gomock.Any(), suite.testJobID).Return(taskInfos, nil),
	)

	trackedMock.EXPECT().UpdateTaskRuntimes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	var request = &task.StartRequest{
		JobId: suite.testJobID,
	}
	resp, err := suite.handler.Start(
		context.Background(),
		request,
	)

	for _, taskInfo := range tasksInfoList {
		suite.Equal(taskInfo.GetRuntime().GetState(), task.TaskState_INITIALIZED)
	}
	suite.NoError(err)
	suite.Nil(resp.GetError())
	suite.Equal(len(resp.GetInvalidInstanceIds()), 0)
	suite.Equal(len(resp.GetStartedInstanceIds()), testInstanceCount)
}

func (suite *TaskHandlerTestSuite) TestStartTasksWithRanges() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockResmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	suite.handler.resmgrClient = mockResmgrClient
	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore
	trackedMock := mocks.NewMockManager(ctrl)
	suite.handler.trackedManager = trackedMock

	expectedTaskIds := make(map[*mesos.TaskID]bool)
	for _, taskInfo := range suite.taskInfos {
		expectedTaskIds[taskInfo.GetRuntime().GetMesosTaskId()] = true
	}

	singleTaskInfo := make(map[uint32]*task.TaskInfo)
	singleTaskInfo[1] = suite.createTestTaskInfo(
		task.TaskState_FAILED, 1)
	singleTaskInfo[1].GetRuntime().GoalState = task.TaskState_FAILED

	taskRanges := []*task.InstanceRange{
		{
			From: 1,
			To:   2,
		},
	}

	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJobConfig(gomock.Any(), suite.testJobID).Return(suite.testJobConfig, nil),
		mockJobStore.EXPECT().
			GetJobRuntime(gomock.Any(), suite.testJobID).Return(suite.testJobRuntime, nil),
		mockJobStore.EXPECT().
			UpdateJobRuntime(gomock.Any(), suite.testJobID, gomock.Any()).Return(nil),
		mockTaskStore.EXPECT().
			GetTasksForJobByRange(gomock.Any(), suite.testJobID, taskRanges[0]).Return(singleTaskInfo, nil),
	)

	gomock.InOrder(
		trackedMock.EXPECT().UpdateTaskRuntimes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil),
	)

	var request = &task.StartRequest{
		JobId:  suite.testJobID,
		Ranges: taskRanges,
	}
	resp, err := suite.handler.Start(
		context.Background(),
		request,
	)
	suite.Equal(singleTaskInfo[1].GetRuntime().GetState(), task.TaskState_INITIALIZED)
	suite.Equal(singleTaskInfo[1].GetRuntime().GetGoalState(), task.TaskState_SUCCEEDED)
	suite.NoError(err)
	suite.Nil(resp.GetError())
	suite.Equal(len(resp.GetInvalidInstanceIds()), 0)
	suite.Equal(resp.GetStartedInstanceIds(), []uint32{1})
}

func (suite *TaskHandlerTestSuite) TestGetEvents() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockResmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	suite.handler.resmgrClient = mockResmgrClient
	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore
	trackedMock := mocks.NewMockManager(ctrl)
	suite.handler.trackedManager = trackedMock

	taskEvents := suite.createTestTaskEvents()

	gomock.InOrder(
		mockTaskStore.EXPECT().
			GetTaskEvents(gomock.Any(), gomock.Any(), gomock.Any()).Return(taskEvents, nil),
	)
	var request = &task.GetEventsRequest{
		JobId:      suite.testJobID,
		InstanceId: 0,
	}
	resp, err := suite.handler.GetEvents(
		context.Background(),
		request,
	)
	suite.NoError(err)
	suite.Nil(resp.GetError())
	eventsList := resp.GetResult()
	suite.Equal(len(eventsList), 2)
	task0Events := eventsList[0].GetEvent()
	task1Events := eventsList[1].GetEvent()
	suite.Equal(len(task0Events), 2)
	suite.Equal(len(task1Events), 3)
	taskID1 := fmt.Sprintf("%s-%d", suite.testJobID.Value, 1)
	expectedTask1Events := []*task.TaskEvent{
		{
			TaskId: &peloton.TaskID{
				Value: taskID1,
			},
			State:     task.TaskState_INITIALIZED,
			Message:   "",
			Timestamp: "2017-12-11T22:17:46Z",
			Hostname:  "peloton-test-host-1",
			Reason:    "",
		},
		{
			TaskId: &peloton.TaskID{
				Value: taskID1,
			},
			State:     task.TaskState_LAUNCHED,
			Message:   "",
			Timestamp: "2017-12-11T22:17:50Z",
			Hostname:  "peloton-test-host-1",
			Reason:    "",
		},
		{
			TaskId: &peloton.TaskID{
				Value: taskID1,
			},
			State:     task.TaskState_RUNNING,
			Message:   "",
			Timestamp: "2017-12-11T22:17:56Z",
			Hostname:  "peloton-test-host-1",
			Reason:    "",
		},
	}
	suite.Equal(task1Events, expectedTask1Events)
}

func (suite *TaskHandlerTestSuite) TestStartTasksWithInvalidRanges() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockResmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	suite.handler.resmgrClient = mockResmgrClient
	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore

	singleTaskInfo := make(map[uint32]*task.TaskInfo)
	singleTaskInfo[1] = suite.taskInfos[1]
	emptyTaskInfo := make(map[uint32]*task.TaskInfo)

	taskRanges := []*task.InstanceRange{
		{
			From: 1,
			To:   2,
		},
		{
			From: 3,
			To:   testInstanceCount + 1,
		},
	}
	correctedTaskRange := &task.InstanceRange{
		From: 3,
		To:   testInstanceCount,
	}

	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJobConfig(gomock.Any(), suite.testJobID).Return(suite.testJobConfig, nil),
		mockJobStore.EXPECT().
			GetJobRuntime(gomock.Any(), suite.testJobID).Return(suite.testJobRuntime, nil),
		mockJobStore.EXPECT().
			UpdateJobRuntime(gomock.Any(), suite.testJobID, gomock.Any()).Return(nil),
		mockTaskStore.EXPECT().
			GetTasksForJobByRange(gomock.Any(), suite.testJobID, taskRanges[0]).Return(singleTaskInfo, nil),
		mockTaskStore.EXPECT().
			GetTasksForJobByRange(gomock.Any(), suite.testJobID, correctedTaskRange).
			Return(emptyTaskInfo, errors.New("test error")),
	)

	var request = &task.StartRequest{
		JobId:  suite.testJobID,
		Ranges: taskRanges,
	}
	resp, err := suite.handler.Start(
		context.Background(),
		request,
	)
	suite.NoError(err)
	suite.Equal(len(resp.GetStartedInstanceIds()), 0)
	suite.Equal(resp.GetError().GetOutOfRange().GetJobId().GetValue(), "test_job")
	suite.Equal(
		resp.GetError().GetOutOfRange().GetInstanceCount(),
		uint32(testInstanceCount))
}

func (suite *TaskHandlerTestSuite) TestStartTasksWithRangesForLaunchedTask() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockResmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	suite.handler.resmgrClient = mockResmgrClient
	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore
	trackedMock := mocks.NewMockManager(ctrl)
	suite.handler.trackedManager = trackedMock

	expectedTaskIds := make(map[*mesos.TaskID]bool)
	for _, taskInfo := range suite.taskInfos {
		expectedTaskIds[taskInfo.GetRuntime().GetMesosTaskId()] = true
	}

	singleTaskInfo := make(map[uint32]*task.TaskInfo)
	singleTaskInfo[1] = suite.createTestTaskInfo(
		task.TaskState_LAUNCHED, 1)

	taskRanges := []*task.InstanceRange{
		{
			From: 1,
			To:   2,
		},
	}

	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJobConfig(gomock.Any(), suite.testJobID).Return(suite.testJobConfig, nil),
		mockJobStore.EXPECT().
			GetJobRuntime(gomock.Any(), suite.testJobID).Return(suite.testJobRuntime, nil),
		mockJobStore.EXPECT().
			UpdateJobRuntime(gomock.Any(), suite.testJobID, gomock.Any()).Return(nil),
		mockTaskStore.EXPECT().
			GetTasksForJobByRange(gomock.Any(), suite.testJobID, taskRanges[0]).Return(singleTaskInfo, nil),
		trackedMock.EXPECT().UpdateTaskRuntimes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil),
	)

	var request = &task.StartRequest{
		JobId:  suite.testJobID,
		Ranges: taskRanges,
	}
	resp, err := suite.handler.Start(
		context.Background(),
		request,
	)
	suite.Equal(singleTaskInfo[1].GetRuntime().GetState(), task.TaskState_INITIALIZED)
	suite.NoError(err)
	suite.Nil(resp.GetError())
	suite.Equal(len(resp.GetInvalidInstanceIds()), 0)
	suite.Equal(resp.GetStartedInstanceIds(), []uint32{1})
}

func (suite *TaskHandlerTestSuite) TestBrowseSandboxPreviousTaskRun() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore

	instanceID := uint32(0)
	taskRuns := uint32(3)
	events, getReturnEvents := suite.createTaskEventForGetTasks(instanceID, taskRuns)

	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJobConfig(gomock.Any(), suite.testJobID).Return(suite.testJobConfig, nil),
		mockTaskStore.EXPECT().
			GetTaskEvents(gomock.Any(), suite.testJobID, instanceID).Return(events, nil),
	)

	var req = &task.BrowseSandboxRequest{
		JobId:      suite.testJobID,
		InstanceId: instanceID,
		TaskId:     getReturnEvents[0].GetTaskId().GetValue(),
	}
	resp, err := suite.handler.BrowseSandbox(context.Background(), req)
	suite.NoError(err)
	suite.NotNil(resp.GetError().GetNotRunning())
}

func (suite *TaskHandlerTestSuite) TestBrowseSandboxWithoutHostname() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore

	singleTaskInfo := make(map[uint32]*task.TaskInfo)
	singleTaskInfo[0] = suite.taskInfos[0]

	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJobConfig(gomock.Any(), suite.testJobID).Return(suite.testJobConfig, nil),
		mockTaskStore.EXPECT().
			GetTaskForJob(gomock.Any(), suite.testJobID, uint32(0)).Return(singleTaskInfo, nil),
	)

	var request = &task.BrowseSandboxRequest{
		JobId:      suite.testJobID,
		InstanceId: 0,
	}
	resp, err := suite.handler.BrowseSandbox(context.Background(), request)
	suite.NoError(err)
	suite.NotNil(resp.GetError().GetNotRunning())
}

func (suite *TaskHandlerTestSuite) TestBrowseSandboxWithEmptyFrameworkID() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore
	mockFrameworkStore := store_mocks.NewMockFrameworkInfoStore(ctrl)
	suite.handler.frameworkInfoStore = mockFrameworkStore

	singleTaskInfo := make(map[uint32]*task.TaskInfo)
	singleTaskInfo[0] = suite.taskInfos[0]
	singleTaskInfo[0].GetRuntime().Host = "host-0"
	singleTaskInfo[0].GetRuntime().AgentID = &mesos.AgentID{
		Value: util.PtrPrintf("host-agent-0"),
	}

	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJobConfig(gomock.Any(), suite.testJobID).Return(suite.testJobConfig, nil),
		mockTaskStore.EXPECT().
			GetTaskForJob(gomock.Any(), suite.testJobID, uint32(0)).Return(singleTaskInfo, nil),
		mockFrameworkStore.EXPECT().GetFrameworkID(gomock.Any(), _frameworkName).Return("", nil),
	)

	var request = &task.BrowseSandboxRequest{
		JobId:      suite.testJobID,
		InstanceId: 0,
	}
	resp, err := suite.handler.BrowseSandbox(context.Background(), request)
	suite.NoError(err)
	suite.NotNil(resp.GetError().GetFailure())
}

func (suite *TaskHandlerTestSuite) TestBrowseSandboxListSandboxFileFailure() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	instanceID := uint32(0)
	taskRuns := uint32(3)
	events, getReturnEvents := suite.createTaskEventForGetTasks(instanceID, taskRuns)
	hostName := "peloton-test-host"
	agentID := "peloton-test-agent"
	frameworkID := "1234"
	mesosAgentDir := "mesosAgentDir"

	suite.handler.mesosAgentWorkDir = mesosAgentDir

	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore
	mockFrameworkInfoStore := storage_mocks.NewMockFrameworkInfoStore(ctrl)
	suite.handler.frameworkInfoStore = mockFrameworkInfoStore
	mockLogManager := logmanager_mocks.NewMockLogManager(ctrl)
	suite.handler.logManager = mockLogManager

	var req = &task.BrowseSandboxRequest{
		JobId:      suite.testJobID,
		InstanceId: instanceID,
		TaskId:     getReturnEvents[1].GetTaskId().GetValue(),
	}

	gomock.InOrder(
		mockJobStore.EXPECT().GetJobConfig(gomock.Any(), suite.testJobID).Return(suite.testJobConfig, nil),
		mockTaskStore.EXPECT().GetTaskEvents(gomock.Any(), suite.testJobID, instanceID).Return(events, nil),
		mockFrameworkInfoStore.EXPECT().GetFrameworkID(gomock.Any(), _frameworkName).Return(frameworkID, nil),
		mockLogManager.EXPECT().ListSandboxFilesPaths(mesosAgentDir, frameworkID, hostName, agentID, req.GetTaskId()).Return(nil, errors.New("enable to fetch sandbox files from mesos agent")),
	)

	resp, _ := suite.handler.BrowseSandbox(context.Background(), req)
	suite.NotEmpty(resp.GetError().GetFailure())
}

func (suite *TaskHandlerTestSuite) TesBrowseSandboxGetMesosMasterInfoFailure() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	instanceID := uint32(0)
	taskRuns := uint32(3)
	events, getReturnEvents := suite.createTaskEventForGetTasks(instanceID, taskRuns)
	sandboxFilesPaths := []string{"path1", "path2"}
	hostName := "peloton-test-host"
	agentID := "peloton-test-agent"
	frameworkID := "1234"
	mesosAgentDir := "mesosAgentDir"

	suite.handler.mesosAgentWorkDir = mesosAgentDir

	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore
	mockFrameworkInfoStore := storage_mocks.NewMockFrameworkInfoStore(ctrl)
	suite.handler.frameworkInfoStore = mockFrameworkInfoStore
	mockLogManager := logmanager_mocks.NewMockLogManager(ctrl)
	suite.handler.logManager = mockLogManager
	mockHostMgr := host_mocks.NewMockInternalHostServiceYARPCClient(ctrl)
	suite.handler.hostMgrClient = mockHostMgr

	var req = &task.BrowseSandboxRequest{
		JobId:      suite.testJobID,
		InstanceId: instanceID,
		TaskId:     getReturnEvents[1].GetTaskId().GetValue(),
	}

	gomock.InOrder(
		mockJobStore.EXPECT().GetJobConfig(gomock.Any(), suite.testJobID).Return(suite.testJobConfig, nil),
		mockTaskStore.EXPECT().GetTaskEvents(gomock.Any(), suite.testJobID, instanceID).Return(events, nil),
		mockFrameworkInfoStore.EXPECT().GetFrameworkID(gomock.Any(), _frameworkName).Return(frameworkID, nil),
		mockLogManager.EXPECT().ListSandboxFilesPaths(mesosAgentDir, frameworkID, hostName, agentID, req.GetTaskId()).Return(sandboxFilesPaths, nil),
		mockHostMgr.EXPECT().GetMesosMasterHostPort(gomock.Any(), &hostsvc.MesosMasterHostPortRequest{}).Return(nil, errors.New("unable to fetch mesos master info")),
	)

	resp, _ := suite.handler.BrowseSandbox(context.Background(), req)
	suite.NotEmpty(resp.GetError().GetFailure())
}

func (suite *TaskHandlerTestSuite) TestBrowseSandboxListFilesSuccess() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	instanceID := uint32(0)
	taskRuns := uint32(3)
	events, getReturnEvents := suite.createTaskEventForGetTasks(instanceID, taskRuns)
	sandboxFilesPaths := []string{"path1", "path2"}
	hostName := "peloton-test-host"
	agentID := "peloton-test-agent"
	frameworkID := "1234"
	mesosAgentDir := "mesosAgentDir"

	suite.handler.mesosAgentWorkDir = mesosAgentDir

	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore
	mockFrameworkInfoStore := storage_mocks.NewMockFrameworkInfoStore(ctrl)
	suite.handler.frameworkInfoStore = mockFrameworkInfoStore
	mockLogManager := logmanager_mocks.NewMockLogManager(ctrl)
	suite.handler.logManager = mockLogManager
	mockHostMgr := host_mocks.NewMockInternalHostServiceYARPCClient(ctrl)
	suite.handler.hostMgrClient = mockHostMgr

	var req = &task.BrowseSandboxRequest{
		JobId:      suite.testJobID,
		InstanceId: instanceID,
		TaskId:     getReturnEvents[1].GetTaskId().GetValue(),
	}

	var res = &task.BrowseSandboxResponse{
		Hostname:            hostName,
		Port:                "5051",
		Error:               nil,
		Paths:               sandboxFilesPaths,
		MesosMasterHostname: "master",
		MesosMasterPort:     "5050",
	}

	gomock.InOrder(
		mockJobStore.EXPECT().GetJobConfig(gomock.Any(), suite.testJobID).Return(suite.testJobConfig, nil),
		mockTaskStore.EXPECT().GetTaskEvents(gomock.Any(), suite.testJobID, instanceID).Return(events, nil),
		mockFrameworkInfoStore.EXPECT().GetFrameworkID(gomock.Any(), _frameworkName).Return(frameworkID, nil),
		mockLogManager.EXPECT().ListSandboxFilesPaths(mesosAgentDir, frameworkID, hostName, agentID, req.GetTaskId()).Return(sandboxFilesPaths, nil),
		mockHostMgr.EXPECT().GetMesosMasterHostPort(gomock.Any(), &hostsvc.MesosMasterHostPortRequest{}).Return(&hostsvc.MesosMasterHostPortResponse{
			Hostname: "master",
			Port:     "5050",
		}, nil),
	)

	resp, err := suite.handler.BrowseSandbox(context.Background(), req)
	suite.NoError(err)
	suite.Equal(resp.Paths, sandboxFilesPaths)
	suite.Equal(resp, res)
}

func (suite *TaskHandlerTestSuite) TestRefreshTask() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore
	trackedMock := mocks.NewMockManager(ctrl)
	suite.handler.trackedManager = trackedMock

	runtimes := make(map[uint32]*task.RuntimeInfo)
	for instID, taskInfo := range suite.taskInfos {
		runtimes[instID] = taskInfo.GetRuntime()
	}

	mockJobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.testJobID).Return(suite.testJobConfig, nil)
	mockTaskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.testJobID, &task.InstanceRange{
			From: 0,
			To:   suite.testJobConfig.GetInstanceCount(),
		}).Return(runtimes, nil)
	trackedMock.EXPECT().SetTasks(suite.testJobID, runtimes, tracked.UpdateAndSchedule).Return()

	var request = &task.RefreshRequest{
		JobId: suite.testJobID,
	}
	_, err := suite.handler.Refresh(context.Background(), request)
	suite.NoError(err)

	mockJobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.testJobID).Return(nil, fmt.Errorf("fake db error"))
	_, err = suite.handler.Refresh(context.Background(), request)
	suite.Error(err)

	mockJobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.testJobID).Return(suite.testJobConfig, nil)
	mockTaskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.testJobID, &task.InstanceRange{
			From: 0,
			To:   suite.testJobConfig.GetInstanceCount(),
		}).Return(nil, fmt.Errorf("fake db error"))
	_, err = suite.handler.Refresh(context.Background(), request)
	suite.Error(err)

	mockJobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.testJobID).Return(suite.testJobConfig, nil)
	mockTaskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.testJobID, &task.InstanceRange{
			From: 0,
			To:   suite.testJobConfig.GetInstanceCount(),
		}).Return(nil, nil)
	_, err = suite.handler.Refresh(context.Background(), request)
	suite.Error(err)
}

func (suite *TaskHandlerTestSuite) TestListTask() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	testReason := "test reason"
	var taskEntries []*resmgrsvc.GetActiveTasksResponse_TaskEntry
	taskInfos := make(map[uint32]*task.TaskInfo)
	runningTasks := uint32(testInstanceCount) / 2
	pendingTasks := uint32(testInstanceCount) - runningTasks
	for i := uint32(0); i < testInstanceCount; i++ {
		if i < runningTasks {
			taskInfos[i] = suite.createTestTaskInfo(
				task.TaskState_RUNNING, i)
		} else {
			taskInfos[i] = suite.createTestTaskInfo(
				task.TaskState_PENDING, i)
			taskEntries = append(taskEntries, &resmgrsvc.GetActiveTasksResponse_TaskEntry{
				TaskID: fmt.Sprintf("%s-%d", suite.testJobID.Value, i),
				Reason: testReason,
			})
		}
	}

	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore
	mockResmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	suite.handler.resmgrClient = mockResmgrClient

	mockJobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.testJobID).
		Return(suite.testJobConfig, nil)
	mockTaskStore.EXPECT().
		GetTasksForJob(gomock.Any(), suite.testJobID).
		Return(taskInfos, nil)
	mockResmgrClient.EXPECT().
		GetActiveTasks(gomock.Any(), &resmgrsvc.GetActiveTasksRequest{
			JobID:  suite.testJobID.GetValue(),
			States: getResourceManagerProcessingStates(),
		}).Return(&resmgrsvc.GetActiveTasksResponse{
		TasksByState: map[string]*resmgrsvc.GetActiveTasksResponse_TaskEntries{
			task.TaskState_PLACING.String(): {TaskEntry: taskEntries}},
	}, nil)

	result, err := suite.handler.List(context.Background(), &task.ListRequest{
		JobId: suite.testJobID,
	})
	suite.NoError(err)
	for _, taskInfo := range result.GetResult().GetValue() {
		if taskInfo.GetRuntime().GetState() == task.TaskState_RUNNING {
			suite.Equal(taskInfo.GetRuntime().GetReason(), "")
			runningTasks--
		}
		if taskInfo.GetRuntime().GetState() == task.TaskState_PENDING {
			suite.Equal(taskInfo.GetRuntime().GetReason(), testReason)
			pendingTasks--
		}
	}
	suite.Equal(runningTasks, uint32(0))
	suite.Equal(pendingTasks, uint32(0))
}

func (suite *TaskHandlerTestSuite) TestQueryTask() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	testReason := "test reason"
	var taskEntries []*resmgrsvc.GetActiveTasksResponse_TaskEntry
	taskInfos := make([]*task.TaskInfo, testInstanceCount)
	runningTasks := testInstanceCount / 2
	pendingTasks := testInstanceCount - runningTasks
	for i := 0; i < testInstanceCount; i++ {
		if i < runningTasks {
			taskInfos[i] = suite.createTestTaskInfo(
				task.TaskState_RUNNING, uint32(i))
		} else {
			taskInfos[i] = suite.createTestTaskInfo(
				task.TaskState_PENDING, uint32(i))
			taskEntries = append(taskEntries, &resmgrsvc.GetActiveTasksResponse_TaskEntry{
				TaskID: fmt.Sprintf("%s-%d", suite.testJobID.Value, i),
				Reason: testReason,
			})
		}
	}

	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore
	mockResmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	suite.handler.resmgrClient = mockResmgrClient

	mockJobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.testJobID).
		Return(suite.testJobConfig, nil)
	mockTaskStore.EXPECT().
		QueryTasks(gomock.Any(), suite.testJobID, nil).
		Return(taskInfos, uint32(testInstanceCount), nil)
	mockResmgrClient.EXPECT().
		GetActiveTasks(gomock.Any(), &resmgrsvc.GetActiveTasksRequest{
			JobID:  suite.testJobID.GetValue(),
			States: getResourceManagerProcessingStates(),
		}).Return(&resmgrsvc.GetActiveTasksResponse{
		TasksByState: map[string]*resmgrsvc.GetActiveTasksResponse_TaskEntries{
			task.TaskState_PLACING.String(): {TaskEntry: taskEntries}},
	}, nil)

	result, err := suite.handler.Query(context.Background(), &task.QueryRequest{
		JobId: suite.testJobID,
	})
	suite.NoError(err)
	for _, taskInfo := range result.Records {
		if taskInfo.GetRuntime().GetState() == task.TaskState_RUNNING {
			suite.Equal(taskInfo.GetRuntime().GetReason(), "")
			runningTasks--
		}
		if taskInfo.GetRuntime().GetState() == task.TaskState_PENDING {
			suite.Equal(taskInfo.GetRuntime().GetReason(), testReason)
			pendingTasks--
		}
	}
	suite.Equal(runningTasks, 0)
	suite.Equal(pendingTasks, 0)
}
