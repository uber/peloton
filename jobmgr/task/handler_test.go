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
	"github.com/golang/mock/gomock"
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
		var taskID = fmt.Sprintf("%s-%d", suite.testJobID.Value, i)
		taskInfos[i] = &task.TaskInfo{
			Runtime: &task.RuntimeInfo{
				TaskId:    &mesos.TaskID{Value: &taskID},
				State:     task.RuntimeInfo_INITIALIZED,
				GoalState: task.RuntimeInfo_SUCCEEDED,
			},
			Config:     suite.testJobConfig.GetDefaultConfig(),
			InstanceId: i,
			JobId:      suite.testJobID,
		}
	}
	suite.taskInfos = taskInfos
}

func (suite *TaskHandlerTestSuite) TearDownTest() {
	fmt.Println("tearing down")
}

func TestPelotonTaskHanlder(t *testing.T) {
	suite.Run(t, new(TaskHandlerTestSuite))
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
				gomock.Eq(&hostsvc.KillTasksRequest{
					TaskIds: []string{"test_job-0", "test_job-1"},
				}),
				gomock.Any()).
			Do(func(_ context.Context, _ yarpc.CallReqMeta, _ interface{}, resBodyOut interface{}) {
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
					TaskIds: []string{"test_job-1"},
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
