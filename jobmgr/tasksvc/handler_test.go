package tasksvc

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"go.uber.org/yarpc/yarpcerrors"

	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	res_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"

	job_mocks "code.uber.internal/infra/peloton/jobmgr/job/mocks"
	"code.uber.internal/infra/peloton/jobmgr/tracked/mocks"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
	"code.uber.internal/infra/peloton/util"
)

const (
	testInstanceCount = 4
)

type TaskHandlerTestSuite struct {
	suite.Suite
	handler       *serviceHandler
	testJobID     *peloton.JobID
	testJobConfig *job.JobConfig
	testJobInfo   *job.JobInfo
	taskInfos     map[uint32]*task.TaskInfo
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
	suite.testJobInfo = &job.JobInfo{
		Config: suite.testJobConfig,
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
}

func TestPelotonTaskHandler(t *testing.T) {
	suite.Run(t, new(TaskHandlerTestSuite))
}

func (suite *TaskHandlerTestSuite) createTestTaskInfo(
	state task.TaskState,
	instanceID uint32) *task.TaskInfo {

	var taskID = util.BuildTaskID(suite.testJobID, instanceID)
	return &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			MesosTaskId: &mesos.TaskID{Value: &taskID.Value},
			State:       state,
			GoalState:   task.TaskGoalState_SUCCEED,
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

	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJob(gomock.Any(), suite.testJobID).Return(suite.testJobInfo, nil),
		mockTaskStore.EXPECT().
			GetTasksForJob(gomock.Any(), suite.testJobID).Return(suite.taskInfos, nil),
	)
	for i := 0; i < testInstanceCount; i++ {
		gomock.InOrder(
			trackedMock.EXPECT().UpdateTaskRuntime(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil),
		)
	}

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

	taskRanges := []*task.InstanceRange{
		{
			From: 1,
			To:   2,
		},
	}

	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJob(gomock.Any(), suite.testJobID).Return(suite.testJobInfo, nil),
		mockTaskStore.EXPECT().
			GetTasksForJobByRange(gomock.Any(), suite.testJobID, taskRanges[0]).Return(singleTaskInfo, nil),
		trackedMock.EXPECT().UpdateTaskRuntime(gomock.Any(), suite.taskInfos[1].GetJobId(), uint32(1), suite.taskInfos[1].Runtime).Return(nil),
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
			GetJob(gomock.Any(), suite.testJobID).Return(suite.testJobInfo, nil),
		mockTaskStore.EXPECT().
			GetTasksForJobByRange(gomock.Any(), suite.testJobID, taskRanges[0]).Return(taskInfos, nil),
		trackedMock.EXPECT().UpdateTaskRuntime(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil),
		trackedMock.EXPECT().UpdateTaskRuntime(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil),
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
			GetJob(gomock.Any(), suite.testJobID).Return(suite.testJobInfo, nil),
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
			GetJob(gomock.Any(), suite.testJobID).Return(nil, errors.New("test error")),
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

func (suite *TaskHandlerTestSuite) TestStopAllTasksWithTaskUpdateFailure() {
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
			GetJob(gomock.Any(), suite.testJobID).Return(suite.testJobInfo, nil),
		mockTaskStore.EXPECT().
			GetTasksForJob(gomock.Any(), suite.testJobID).Return(suite.taskInfos, nil),
		trackedMock.EXPECT().UpdateTaskRuntime(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(yarpcerrors.AlreadyExistsErrorf("retry error")),
		trackedMock.EXPECT().
			GetTaskRuntime(gomock.Any(), gomock.Any(), gomock.Any()).Return(suite.taskInfos[0].Runtime, nil),
		trackedMock.EXPECT().UpdateTaskRuntime(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(fmt.Errorf("db update failure")),
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
	suite.Equal(len(resp.GetStoppedInstanceIds()), 0)
	suite.NotNil(resp.GetError().GetUpdateError())
}

func (suite *TaskHandlerTestSuite) TestRestartAllTasks() {
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

	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJob(gomock.Any(), suite.testJobID).Return(suite.testJobInfo, nil),
		mockTaskStore.EXPECT().
			GetTasksForJob(gomock.Any(), suite.testJobID).Return(suite.taskInfos, nil),
	)
	for i := 0; i < testInstanceCount; i++ {
		gomock.InOrder(
			trackedMock.EXPECT().UpdateTaskRuntime(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil),
		)
	}

	var request = &task.RestartRequest{
		JobId: suite.testJobID,
	}
	resp, err := suite.handler.Restart(
		context.Background(),
		request,
	)
	suite.NoError(err)
	suite.Nil(resp.GetNotFound())
	suite.Nil(resp.GetOutOfRange())
}

func (suite *TaskHandlerTestSuite) TestRestartTasksWithRanges() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore
	trackedMock := mocks.NewMockManager(ctrl)
	suite.handler.trackedManager = trackedMock

	tasksInfo1 := make(map[uint32]*task.TaskInfo)
	tasksInfo1[1] = suite.taskInfos[1]
	tasksInfo2 := make(map[uint32]*task.TaskInfo)
	tasksInfo2[3] = suite.taskInfos[3]

	taskRanges := []*task.InstanceRange{
		{From: 1, To: 2},
		{From: 3, To: 4},
	}

	mockJobStore.EXPECT().GetJob(gomock.Any(), suite.testJobID).Return(suite.testJobInfo, nil)
	mockTaskStore.EXPECT().GetTasksForJobByRange(gomock.Any(), suite.testJobID, taskRanges[0]).Return(tasksInfo1, nil)
	mockTaskStore.EXPECT().GetTasksForJobByRange(gomock.Any(), suite.testJobID, taskRanges[1]).Return(tasksInfo2, nil)
	trackedMock.EXPECT().UpdateTaskRuntime(gomock.Any(), suite.taskInfos[1].GetJobId(), uint32(1), suite.taskInfos[1].Runtime).Return(nil)
	trackedMock.EXPECT().UpdateTaskRuntime(gomock.Any(), suite.taskInfos[3].GetJobId(), uint32(3), suite.taskInfos[3].Runtime).Return(nil)

	var request = &task.RestartRequest{
		JobId:  suite.testJobID,
		Ranges: taskRanges,
	}
	resp, err := suite.handler.Restart(
		context.Background(),
		request,
	)
	suite.NoError(err)
	suite.Nil(resp.GetNotFound())
	suite.Nil(resp.GetOutOfRange())
}

func (suite *TaskHandlerTestSuite) TestRestartTasksWithInvalidRanges() {
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
			GetJob(gomock.Any(), suite.testJobID).Return(suite.testJobInfo, nil),
		mockTaskStore.EXPECT().
			GetTasksForJobByRange(gomock.Any(), suite.testJobID, taskRanges[0]).Return(singleTaskInfo, nil),
		mockTaskStore.EXPECT().
			GetTasksForJobByRange(gomock.Any(), suite.testJobID, correctedRange).
			Return(emptyTaskInfo, errors.New("test error")),
	)

	var request = &task.RestartRequest{
		JobId:  suite.testJobID,
		Ranges: taskRanges,
	}
	resp, err := suite.handler.Restart(
		context.Background(),
		request,
	)
	suite.NoError(err)
	suite.Equal(resp.GetOutOfRange().GetJobId().GetValue(), "test_job")
	suite.Equal(resp.GetOutOfRange().GetInstanceCount(), uint32(testInstanceCount))
}

func (suite *TaskHandlerTestSuite) TestRestartTasksWithInvalidJobID() {
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
			GetJob(gomock.Any(), suite.testJobID).Return(nil, errors.New("test error")),
	)

	var request = &task.RestartRequest{
		JobId: suite.testJobID,
	}
	resp, err := suite.handler.Restart(
		context.Background(),
		request,
	)
	suite.NoError(err)
	suite.Equal(resp.GetNotFound().GetId().GetValue(), "test_job")
}

func (suite *TaskHandlerTestSuite) TestRestartTasksSkipsTasksWithUpdateFailure() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore
	trackedMock := mocks.NewMockManager(ctrl)
	suite.handler.trackedManager = trackedMock

	singleTaskInfo := make(map[uint32]*task.TaskInfo)
	singleTaskInfo[0] = suite.taskInfos[0]

	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJob(gomock.Any(), suite.testJobID).Return(suite.testJobInfo, nil),
		mockTaskStore.EXPECT().
			GetTasksForJob(gomock.Any(), suite.testJobID).Return(singleTaskInfo, nil),
		trackedMock.EXPECT().
			UpdateTaskRuntime(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("asd")),
	)

	var request = &task.RestartRequest{
		JobId: suite.testJobID,
	}
	resp, err := suite.handler.Restart(
		context.Background(),
		request,
	)

	suite.NoError(err)
	suite.Nil(resp.GetNotFound())
	suite.Nil(resp.GetOutOfRange())
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
	updaterMock := job_mocks.NewMockRuntimeUpdater(ctrl)
	suite.handler.runtimeUpdater = updaterMock

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
			GetJob(gomock.Any(), suite.testJobID).Return(suite.testJobInfo, nil),
		mockTaskStore.EXPECT().
			GetTasksForJob(gomock.Any(), suite.testJobID).Return(taskInfos, nil),
	)

	for i := 0; i < testInstanceCount; i++ {
		gomock.InOrder(
			trackedMock.EXPECT().UpdateTaskRuntime(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil),
		)
	}
	gomock.InOrder(
		updaterMock.EXPECT().UpdateJob(gomock.Any(), suite.testJobID).Return(nil),
	)

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
	updaterMock := job_mocks.NewMockRuntimeUpdater(ctrl)
	suite.handler.runtimeUpdater = updaterMock

	expectedTaskIds := make(map[*mesos.TaskID]bool)
	for _, taskInfo := range suite.taskInfos {
		expectedTaskIds[taskInfo.GetRuntime().GetMesosTaskId()] = true
	}

	singleTaskInfo := make(map[uint32]*task.TaskInfo)
	singleTaskInfo[1] = suite.createTestTaskInfo(
		task.TaskState_FAILED, 1)

	taskRanges := []*task.InstanceRange{
		{
			From: 1,
			To:   2,
		},
	}

	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJob(gomock.Any(), suite.testJobID).Return(suite.testJobInfo, nil),
		mockTaskStore.EXPECT().
			GetTasksForJobByRange(gomock.Any(), suite.testJobID, taskRanges[0]).Return(singleTaskInfo, nil),
	)

	gomock.InOrder(
		trackedMock.EXPECT().UpdateTaskRuntime(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil),
		updaterMock.EXPECT().UpdateJob(gomock.Any(), suite.testJobID).Return(nil),
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
			GetJob(gomock.Any(), suite.testJobID).Return(suite.testJobInfo, nil),
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
	updaterMock := job_mocks.NewMockRuntimeUpdater(ctrl)
	suite.handler.runtimeUpdater = updaterMock

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
			GetJob(gomock.Any(), suite.testJobID).Return(suite.testJobInfo, nil),
		mockTaskStore.EXPECT().
			GetTasksForJobByRange(gomock.Any(), suite.testJobID, taskRanges[0]).Return(singleTaskInfo, nil),
		trackedMock.EXPECT().UpdateTaskRuntime(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil),
		updaterMock.EXPECT().UpdateJob(gomock.Any(), suite.testJobID).Return(nil),
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
			GetJob(gomock.Any(), suite.testJobID).Return(suite.testJobInfo, nil),
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
			GetJob(gomock.Any(), suite.testJobID).Return(suite.testJobInfo, nil),
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
