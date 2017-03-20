package task

import (
	"context"
	"fmt"
	"testing"

	"go.uber.org/yarpc"

	mesos "mesos/v1"
	"peloton/api/job"
	"peloton/api/task"
	"peloton/private/hostmgr/hostsvc"

	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
	yarpc_mocks "code.uber.internal/infra/peloton/vendor_mocks/go.uber.org/yarpc/encoding/json/mocks"
	log "github.com/Sirupsen/logrus"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

const (
	testInstanceCount = 2
)

type TaskHandlerTestSuite struct {
	suite.Suite
	handler       *serviceHandler
	testJobID     *job.JobID
	testJobConfig *job.JobConfig
	taskInfos     map[uint32]*task.TaskInfo
}

func (suite *TaskHandlerTestSuite) SetupTest() {
	mtx := NewMetrics(tally.NoopScope)
	suite.handler = &serviceHandler{
		metrics: mtx,
		rootCtx: context.Background(),
	}
	suite.testJobID = &job.JobID{
		Value: "test_job",
	}
	suite.testJobConfig = &job.JobConfig{
		Name:          suite.testJobID.Value,
		InstanceCount: testInstanceCount,
	}
	var taskInfos = make(map[uint32]*task.TaskInfo)
	for i := uint32(0); i < testInstanceCount; i++ {
		taskInfos[i] = suite.createTestTaskInfo(
			task.RuntimeInfo_RUNNING, i)
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
	state task.RuntimeInfo_TaskState,
	instanceID uint32) *task.TaskInfo {

	var taskID = fmt.Sprintf("%s-%d", suite.testJobID.Value, instanceID)
	return &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			TaskId:    &mesos.TaskID{Value: &taskID},
			State:     state,
			GoalState: task.RuntimeInfo_SUCCEEDED,
		},
		Config:     suite.testJobConfig.GetDefaultConfig(),
		InstanceId: instanceID,
		JobId:      suite.testJobID,
	}
}

func (suite *TaskHandlerTestSuite) TestStopAllTasks() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockHostmgrClient := yarpc_mocks.NewMockClient(ctrl)
	suite.handler.hostmgrClient = mockHostmgrClient
	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore

	expectedTaskIds := make(map[*mesos.TaskID]bool)
	for _, taskInfo := range suite.taskInfos {
		expectedTaskIds[taskInfo.GetRuntime().GetTaskId()] = true
	}

	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJob(suite.testJobID).Return(suite.testJobConfig, nil),
		mockTaskStore.EXPECT().
			GetTasksForJob(suite.testJobID).Return(suite.taskInfos, nil),
		mockTaskStore.EXPECT().
			UpdateTask(gomock.Any()).Times(testInstanceCount).Return(nil),
		mockHostmgrClient.EXPECT().
			Call(
				gomock.Any(),
				gomock.Eq(yarpc.NewReqMeta().Procedure("InternalHostService.KillTasks")),
				gomock.Any(),
				gomock.Any()).
			Do(func(_ context.Context, _ yarpc.CallReqMeta, reqBody interface{}, resBodyOut interface{}) {
				killTaskReq := reqBody.(*hostsvc.KillTasksRequest)
				for _, tid := range killTaskReq.TaskIds {
					_, ok := expectedTaskIds[tid]
					assert.Equal(suite.T(), ok, true)
				}
				o := resBodyOut.(*hostsvc.KillTasksResponse)
				*o = hostsvc.KillTasksResponse{}
			}).
			Return(nil, nil),
	)

	var request = &task.StopRequest{
		JobId: suite.testJobID,
	}
	resp, _, err := suite.handler.Stop(
		suite.handler.rootCtx,
		nil,
		request,
	)
	suite.NoError(err)
	suite.Equal(len(resp.GetInvalidInstanceIds()), 0)
	suite.Equal(len(resp.GetStoppedInstanceIds()), 2)
}

func (suite *TaskHandlerTestSuite) TestStopTasksWithRanges() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockHostmgrClient := yarpc_mocks.NewMockClient(ctrl)
	suite.handler.hostmgrClient = mockHostmgrClient
	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore

	singleTaskInfo := make(map[uint32]*task.TaskInfo)
	singleTaskInfo[1] = suite.taskInfos[1]

	expectedTaskIds := []*mesos.TaskID{suite.taskInfos[1].GetRuntime().GetTaskId()}

	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJob(suite.testJobID).Return(suite.testJobConfig, nil),
		mockTaskStore.EXPECT().
			GetTaskForJob(suite.testJobID, uint32(1)).Return(singleTaskInfo, nil),
		mockTaskStore.EXPECT().
			UpdateTask(gomock.Any()).Times(1).Return(nil),
		mockHostmgrClient.EXPECT().
			Call(
				gomock.Any(),
				gomock.Eq(yarpc.NewReqMeta().Procedure("InternalHostService.KillTasks")),
				gomock.Eq(&hostsvc.KillTasksRequest{
					TaskIds: expectedTaskIds,
				}),
				gomock.Any()).
			Do(func(_ context.Context, _ yarpc.CallReqMeta, _ interface{}, resBodyOut interface{}) {
				o := resBodyOut.(*hostsvc.KillTasksResponse)
				*o = hostsvc.KillTasksResponse{}
			}).
			Return(nil, nil),
	)

	taskRanges := []*task.InstanceRange{
		{
			From: 1,
			To:   2,
		},
	}
	var request = &task.StopRequest{
		JobId:  suite.testJobID,
		Ranges: taskRanges,
	}
	resp, _, err := suite.handler.Stop(
		suite.handler.rootCtx,
		nil,
		request,
	)
	suite.NoError(err)
	suite.Equal(len(resp.GetInvalidInstanceIds()), 0)
	suite.Equal(resp.GetStoppedInstanceIds(), []uint32{1})
}

func (suite *TaskHandlerTestSuite) TestStopTasksSkipKillNotRunningTask() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockHostmgrClient := yarpc_mocks.NewMockClient(ctrl)
	suite.handler.hostmgrClient = mockHostmgrClient
	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore

	singleTaskInfo := make(map[uint32]*task.TaskInfo)
	singleTaskInfo[1] = suite.taskInfos[1]
	failedTaskInfo := make(map[uint32]*task.TaskInfo)
	failedTaskInfo[2] = suite.createTestTaskInfo(task.RuntimeInfo_FAILED, uint32(2))

	expectedTaskIds := []*mesos.TaskID{singleTaskInfo[1].GetRuntime().GetTaskId()}

	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJob(suite.testJobID).Return(suite.testJobConfig, nil),
		mockTaskStore.EXPECT().
			GetTaskForJob(suite.testJobID, uint32(1)).Return(singleTaskInfo, nil),
		mockTaskStore.EXPECT().
			GetTaskForJob(suite.testJobID, uint32(2)).Return(failedTaskInfo, nil),
		mockTaskStore.EXPECT().
			UpdateTask(gomock.Any()).Times(2).Return(nil),
		mockHostmgrClient.EXPECT().
			Call(
				gomock.Any(),
				gomock.Eq(yarpc.NewReqMeta().Procedure("InternalHostService.KillTasks")),
				gomock.Eq(&hostsvc.KillTasksRequest{
					TaskIds: expectedTaskIds,
				}),
				gomock.Any()).
			Do(func(_ context.Context, _ yarpc.CallReqMeta, _ interface{}, resBodyOut interface{}) {
				o := resBodyOut.(*hostsvc.KillTasksResponse)
				*o = hostsvc.KillTasksResponse{}
			}).
			Times(1).
			Return(nil, nil),
	)

	taskRanges := []*task.InstanceRange{
		{
			From: 1,
			To:   3,
		},
	}
	var request = &task.StopRequest{
		JobId:  suite.testJobID,
		Ranges: taskRanges,
	}
	resp, _, err := suite.handler.Stop(
		suite.handler.rootCtx,
		nil,
		request,
	)
	suite.NoError(err)
	suite.Equal(len(resp.GetInvalidInstanceIds()), 0)
	suite.Equal(len(resp.GetStoppedInstanceIds()), 2)
}

func (suite *TaskHandlerTestSuite) TestStopTasksWithInvalidRanges() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockHostmgrClient := yarpc_mocks.NewMockClient(ctrl)
	suite.handler.hostmgrClient = mockHostmgrClient
	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore

	singleTaskInfo := make(map[uint32]*task.TaskInfo)
	singleTaskInfo[1] = suite.taskInfos[1]
	emptyTaskInfo := make(map[uint32]*task.TaskInfo)
	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJob(suite.testJobID).Return(suite.testJobConfig, nil),
		mockTaskStore.EXPECT().
			GetTaskForJob(suite.testJobID, uint32(1)).Return(singleTaskInfo, nil),
		mockTaskStore.EXPECT().
			GetTaskForJob(suite.testJobID, uint32(3)).Return(emptyTaskInfo, nil),
	)

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
	var request = &task.StopRequest{
		JobId:  suite.testJobID,
		Ranges: taskRanges,
	}
	resp, _, err := suite.handler.Stop(
		suite.handler.rootCtx,
		nil,
		request,
	)
	suite.NoError(err)
	suite.Equal(resp.GetInvalidInstanceIds(), []uint32{3})
	suite.Equal(len(resp.GetStoppedInstanceIds()), 0)
	suite.Equal(resp.GetError().GetOutOfRange().GetJobId().GetValue(), "test_job")
	suite.Equal(
		resp.GetError().GetOutOfRange().GetInstanceCount(),
		uint32(testInstanceCount))
}

func (suite *TaskHandlerTestSuite) TestStopTasksWithInvalidJobID() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockHostmgrClient := yarpc_mocks.NewMockClient(ctrl)
	suite.handler.hostmgrClient = mockHostmgrClient
	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore

	singleTaskInfo := make(map[uint32]*task.TaskInfo)
	singleTaskInfo[1] = suite.taskInfos[1]
	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJob(suite.testJobID).Return(nil, nil),
	)

	var request = &task.StopRequest{
		JobId: suite.testJobID,
	}
	resp, _, err := suite.handler.Stop(
		suite.handler.rootCtx,
		nil,
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

	mockHostmgrClient := yarpc_mocks.NewMockClient(ctrl)
	suite.handler.hostmgrClient = mockHostmgrClient
	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	suite.handler.taskStore = mockTaskStore

	gomock.InOrder(
		mockJobStore.EXPECT().
			GetJob(suite.testJobID).Return(suite.testJobConfig, nil),
		mockTaskStore.EXPECT().
			GetTasksForJob(suite.testJobID).Return(suite.taskInfos, nil),
		mockTaskStore.EXPECT().
			UpdateTask(gomock.Any()).AnyTimes().Return(fmt.Errorf("db update failure")),
	)

	var request = &task.StopRequest{
		JobId: suite.testJobID,
	}
	resp, _, err := suite.handler.Stop(
		suite.handler.rootCtx,
		nil,
		request,
	)
	suite.NoError(err)
	suite.Equal(len(resp.GetInvalidInstanceIds()), 0)
	suite.Equal(len(resp.GetStoppedInstanceIds()), 0)
	suite.NotNil(resp.GetError().GetUpdateError())
}
