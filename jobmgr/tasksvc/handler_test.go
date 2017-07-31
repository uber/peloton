package tasksvc

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/volume"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	res_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"
	jobmgr_job "code.uber.internal/infra/peloton/jobmgr/job"
	launcher_mocks "code.uber.internal/infra/peloton/jobmgr/task/launcher/mocks"

	"code.uber.internal/infra/peloton/jobmgr/task/goalstate/mocks"
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

func TestPelotonTaskHanlder(t *testing.T) {
	suite.Run(t, new(TaskHandlerTestSuite))
}

func (suite *TaskHandlerTestSuite) createTestTaskInfo(
	state task.TaskState,
	instanceID uint32) *task.TaskInfo {

	var taskID = fmt.Sprintf("%s-%d", suite.testJobID.Value, instanceID)
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

func (suite *TaskHandlerTestSuite) TestStopAllTasks() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore
	keeperMock := mocks.NewMockKeeper(ctrl)
	suite.handler.goalstateKeeper = keeperMock

	expectedTaskIds := make(map[*mesos.TaskID]bool)
	for _, taskInfo := range suite.taskInfos {
		expectedTaskIds[taskInfo.GetRuntime().GetMesosTaskId()] = true
	}

	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJobConfig(gomock.Any(), suite.testJobID).Return(suite.testJobConfig, nil),
		mockTaskStore.EXPECT().
			GetTasksForJob(gomock.Any(), suite.testJobID).Return(suite.taskInfos, nil),
		mockTaskStore.EXPECT().UpdateTask(gomock.Any(), gomock.Any()).Return(nil),
		keeperMock.EXPECT().UpdateTaskGoalState(gomock.Any(), gomock.Any()).Return(nil),
		mockTaskStore.EXPECT().UpdateTask(gomock.Any(), gomock.Any()).Return(nil),
		keeperMock.EXPECT().UpdateTaskGoalState(gomock.Any(), gomock.Any()).Return(nil),
		mockTaskStore.EXPECT().UpdateTask(gomock.Any(), gomock.Any()).Return(nil),
		keeperMock.EXPECT().UpdateTaskGoalState(gomock.Any(), gomock.Any()).Return(nil),
		mockTaskStore.EXPECT().UpdateTask(gomock.Any(), gomock.Any()).Return(nil),
		keeperMock.EXPECT().UpdateTaskGoalState(gomock.Any(), gomock.Any()).Return(nil),
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
	keeperMock := mocks.NewMockKeeper(ctrl)
	suite.handler.goalstateKeeper = keeperMock

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
			GetJobConfig(gomock.Any(), suite.testJobID).Return(suite.testJobConfig, nil),
		mockTaskStore.EXPECT().
			GetTasksForJobByRange(gomock.Any(), suite.testJobID, taskRanges[0]).Return(singleTaskInfo, nil),
		mockTaskStore.EXPECT().
			UpdateTask(gomock.Any(), gomock.Any()).Times(1).Return(nil),
		keeperMock.EXPECT().UpdateTaskGoalState(gomock.Any(), suite.taskInfos[1]).Times(1).Return(nil),
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
	keeperMock := mocks.NewMockKeeper(ctrl)
	suite.handler.goalstateKeeper = keeperMock

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
		mockTaskStore.EXPECT().UpdateTask(gomock.Any(), gomock.Any()).Times(1).Return(nil),
		keeperMock.EXPECT().UpdateTaskGoalState(gomock.Any(), gomock.Any()).Times(1).Return(nil),
		mockTaskStore.EXPECT().UpdateTask(gomock.Any(), gomock.Any()).Times(1).Return(nil),
		keeperMock.EXPECT().UpdateTaskGoalState(gomock.Any(), gomock.Any()).Times(1).Return(nil),
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
			To:   6,
		},
	}
	correctedRange := &task.InstanceRange{
		From: 5,
		To:   4,
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

func (suite *TaskHandlerTestSuite) TestStopAllTasksWithTaskUpdateFailure() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore

	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJobConfig(gomock.Any(), suite.testJobID).Return(suite.testJobConfig, nil),
		mockTaskStore.EXPECT().
			GetTasksForJob(gomock.Any(), suite.testJobID).Return(suite.taskInfos, nil),
		mockTaskStore.EXPECT().
			UpdateTask(gomock.Any(), gomock.Any()).AnyTimes().Return(fmt.Errorf("db update failure")),
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

func (suite *TaskHandlerTestSuite) TestStartAllTasks() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockResmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	suite.handler.resmgrClient = mockResmgrClient
	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore
	updater := jobmgr_job.NewJobRuntimeUpdater(mockJobStore, mockTaskStore, nil, tally.NoopScope)
	updater.Start()
	suite.handler.runtimeUpdater = updater
	defer updater.Stop()

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
	var expectedGangs []*resmgrsvc.Gang

	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJobConfig(gomock.Any(), suite.testJobID).Return(suite.testJobConfig, nil),
		mockTaskStore.EXPECT().
			GetTasksForJob(gomock.Any(), suite.testJobID).Return(taskInfos, nil),
		mockTaskStore.EXPECT().
			UpdateTask(gomock.Any(), gomock.Any()).Times(testInstanceCount).Return(nil),
		mockResmgrClient.EXPECT().
			EnqueueGangs(
				gomock.Any(),
				gomock.Any()).
			Do(func(_ context.Context, reqBody interface{}) {
				req := reqBody.(*resmgrsvc.EnqueueGangsRequest)
				for _, g := range req.Gangs {
					expectedGangs = append(expectedGangs, g)
				}
			}).
			Return(&resmgrsvc.EnqueueGangsResponse{}, nil),
		mockJobStore.EXPECT().
			GetJobConfig(gomock.Any(), suite.testJobID).
			Return(nil, errors.New("test error")).
			AnyTimes(),
	)

	var request = &task.StartRequest{
		JobId: suite.testJobID,
	}
	resp, err := suite.handler.Start(
		context.Background(),
		request,
	)
	suite.Len(expectedGangs, 4)
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
	updater := jobmgr_job.NewJobRuntimeUpdater(mockJobStore, mockTaskStore, nil, tally.NoopScope)
	updater.Start()
	suite.handler.runtimeUpdater = updater
	defer updater.Stop()

	expectedTaskIds := make(map[*mesos.TaskID]bool)
	for _, taskInfo := range suite.taskInfos {
		expectedTaskIds[taskInfo.GetRuntime().GetMesosTaskId()] = true
	}

	singleTaskInfo := make(map[uint32]*task.TaskInfo)
	singleTaskInfo[1] = suite.createTestTaskInfo(
		task.TaskState_FAILED, 1)
	mesosTaskID := singleTaskInfo[1].GetRuntime().GetMesosTaskId().GetValue()
	var expectedGangs []*resmgrsvc.Gang

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
		mockTaskStore.EXPECT().
			UpdateTask(gomock.Any(), gomock.Any()).Times(1).Return(nil),
		mockResmgrClient.EXPECT().
			EnqueueGangs(
				gomock.Any(),
				gomock.Any()).
			Do(func(_ context.Context, reqBody interface{}) {
				req := reqBody.(*resmgrsvc.EnqueueGangsRequest)
				for _, g := range req.Gangs {
					expectedGangs = append(expectedGangs, g)
				}
			}).
			Return(&resmgrsvc.EnqueueGangsResponse{}, nil),
		mockJobStore.EXPECT().
			GetJobConfig(gomock.Any(), suite.testJobID).
			Return(nil, errors.New("test error")).
			AnyTimes(),
	)

	var request = &task.StartRequest{
		JobId:  suite.testJobID,
		Ranges: taskRanges,
	}
	resp, err := suite.handler.Start(
		context.Background(),
		request,
	)
	suite.Len(expectedGangs, 1)
	suite.NotEqual(
		expectedGangs[0].GetTasks()[0].GetTaskId().GetValue(),
		mesosTaskID)
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
			To:   5,
		},
	}
	correctedTaskRange := &task.InstanceRange{
		From: 3,
		To:   4,
	}

	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJobConfig(gomock.Any(), suite.testJobID).Return(suite.testJobConfig, nil),
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

func (suite *TaskHandlerTestSuite) TestStartTasksWithRangesForLaunchingTask() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockResmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	suite.handler.resmgrClient = mockResmgrClient
	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore
	updater := jobmgr_job.NewJobRuntimeUpdater(mockJobStore, mockTaskStore, nil, tally.NoopScope)
	updater.Start()
	suite.handler.runtimeUpdater = updater
	defer updater.Stop()

	expectedTaskIds := make(map[*mesos.TaskID]bool)
	for _, taskInfo := range suite.taskInfos {
		expectedTaskIds[taskInfo.GetRuntime().GetMesosTaskId()] = true
	}

	singleTaskInfo := make(map[uint32]*task.TaskInfo)
	singleTaskInfo[1] = suite.createTestTaskInfo(
		task.TaskState_LAUNCHING, 1)
	mesosTaskID := singleTaskInfo[1].GetRuntime().GetMesosTaskId().GetValue()
	var expectedGangs []*resmgrsvc.Gang

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
		mockTaskStore.EXPECT().
			UpdateTask(gomock.Any(), gomock.Any()).Times(1).Return(nil),
		mockResmgrClient.EXPECT().
			EnqueueGangs(
				gomock.Any(),
				gomock.Any()).
			Do(func(_ context.Context, reqBody interface{}) {
				req := reqBody.(*resmgrsvc.EnqueueGangsRequest)
				for _, g := range req.Gangs {
					expectedGangs = append(expectedGangs, g)
				}
			}).
			Return(&resmgrsvc.EnqueueGangsResponse{}, nil),
		mockJobStore.EXPECT().
			GetJobConfig(gomock.Any(), suite.testJobID).
			Return(nil, errors.New("test error")).
			AnyTimes(),
	)

	var request = &task.StartRequest{
		JobId:  suite.testJobID,
		Ranges: taskRanges,
	}
	resp, err := suite.handler.Start(
		context.Background(),
		request,
	)
	suite.Len(expectedGangs, 1)
	suite.Equal(
		expectedGangs[0].GetTasks()[0].GetTaskId().GetValue(),
		mesosTaskID)
	suite.Equal(singleTaskInfo[1].GetRuntime().GetState(), task.TaskState_INITIALIZED)
	suite.NoError(err)
	suite.Nil(resp.GetError())
	suite.Equal(len(resp.GetInvalidInstanceIds()), 0)
	suite.Equal(resp.GetStartedInstanceIds(), []uint32{1})
}

func (suite *TaskHandlerTestSuite) TestStartStatefulTasksWithRanges() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore
	mockVolumeStore := store_mocks.NewMockPersistentVolumeStore(ctrl)
	suite.handler.volumeStore = mockVolumeStore
	updater := jobmgr_job.NewJobRuntimeUpdater(mockJobStore, mockTaskStore, nil, tally.NoopScope)
	updater.Start()
	suite.handler.runtimeUpdater = updater
	defer updater.Stop()

	mockTaskLauncher := launcher_mocks.NewMockLauncher(ctrl)
	suite.handler.taskLauncher = mockTaskLauncher

	expectedTaskIds := make(map[*mesos.TaskID]bool)
	for _, taskInfo := range suite.taskInfos {
		expectedTaskIds[taskInfo.GetRuntime().GetMesosTaskId()] = true
	}

	singleTaskInfo := make(map[uint32]*task.TaskInfo)
	singleTaskInfo[1] = suite.createTestTaskInfo(
		task.TaskState_FAILED, 1)
	singleTaskInfo[1].GetConfig().Volume = &task.PersistentVolumeConfig{
		ContainerPath: "testpath",
		SizeMB:        10,
	}
	singleTaskInfo[1].GetRuntime().VolumeID = &peloton.VolumeID{
		Value: "testvolumeid",
	}

	taskRanges := []*task.InstanceRange{
		{
			From: 1,
			To:   2,
		},
	}

	volumeInfo := &volume.PersistentVolumeInfo{
		State: volume.VolumeState_CREATED,
	}

	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJobConfig(gomock.Any(), suite.testJobID).Return(suite.testJobConfig, nil),
		mockTaskStore.EXPECT().
			GetTasksForJobByRange(gomock.Any(), suite.testJobID, taskRanges[0]).Return(singleTaskInfo, nil),
		mockTaskStore.EXPECT().
			UpdateTask(gomock.Any(), gomock.Any()).Times(1).Return(nil),

		mockVolumeStore.EXPECT().
			GetPersistentVolume(gomock.Any(), gomock.Any()).
			Return(volumeInfo, nil),
		mockTaskLauncher.EXPECT().
			LaunchTaskWithReservedResource(gomock.Any(), singleTaskInfo[1]).
			Return(nil),
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

func (suite *TaskHandlerTestSuite) TestStartTasksWithRangesNoVolumeCreated() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockResmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	suite.handler.resmgrClient = mockResmgrClient
	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore
	mockVolumeStore := store_mocks.NewMockPersistentVolumeStore(ctrl)
	suite.handler.volumeStore = mockVolumeStore
	updater := jobmgr_job.NewJobRuntimeUpdater(mockJobStore, mockTaskStore, nil, tally.NoopScope)
	updater.Start()
	suite.handler.runtimeUpdater = updater
	defer updater.Stop()

	expectedTaskIds := make(map[*mesos.TaskID]bool)
	for _, taskInfo := range suite.taskInfos {
		expectedTaskIds[taskInfo.GetRuntime().GetMesosTaskId()] = true
	}

	singleTaskInfo := make(map[uint32]*task.TaskInfo)
	singleTaskInfo[1] = suite.createTestTaskInfo(
		task.TaskState_FAILED, 1)
	singleTaskInfo[1].GetConfig().Volume = &task.PersistentVolumeConfig{
		ContainerPath: "testpath",
		SizeMB:        10,
	}
	singleTaskInfo[1].GetRuntime().VolumeID = &peloton.VolumeID{
		Value: "testvolumeid",
	}
	mesosTaskID := singleTaskInfo[1].GetRuntime().GetMesosTaskId().GetValue()
	var expectedGangs []*resmgrsvc.Gang

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
		mockTaskStore.EXPECT().
			UpdateTask(gomock.Any(), gomock.Any()).Times(1).Return(nil),

		mockVolumeStore.EXPECT().
			GetPersistentVolume(gomock.Any(), gomock.Any()).
			Return(nil, nil),
		mockResmgrClient.EXPECT().
			EnqueueGangs(
				gomock.Any(),
				gomock.Any()).
			Do(func(_ context.Context, reqBody interface{}) {
				req := reqBody.(*resmgrsvc.EnqueueGangsRequest)
				for _, g := range req.Gangs {
					expectedGangs = append(expectedGangs, g)
				}
			}).
			Return(&resmgrsvc.EnqueueGangsResponse{}, nil),
		mockJobStore.EXPECT().
			GetJobConfig(gomock.Any(), suite.testJobID).
			Return(nil, errors.New("test error")).
			AnyTimes(),
	)

	var request = &task.StartRequest{
		JobId:  suite.testJobID,
		Ranges: taskRanges,
	}
	resp, err := suite.handler.Start(
		context.Background(),
		request,
	)
	suite.Len(expectedGangs, 1)
	suite.NotEqual(
		expectedGangs[0].GetTasks()[0].GetTaskId().GetValue(),
		mesosTaskID)
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
