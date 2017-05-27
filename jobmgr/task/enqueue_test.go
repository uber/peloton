package task

import (
	"context"
	"errors"
	"fmt"
	"testing"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"

	"go.uber.org/yarpc"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	yarpc_mocks "code.uber.internal/infra/peloton/vendor_mocks/go.uber.org/yarpc/encoding/json/mocks"
)

var (
	defaultResourceConfig = task.ResourceConfig{
		CpuLimit:    10,
		MemLimitMb:  10,
		DiskLimitMb: 10,
		FdLimit:     10,
	}
)

type TaskUtilTestSuite struct {
	suite.Suite

	testJobID     *peloton.JobID
	testJobConfig *job.JobConfig
	taskInfos     map[uint32]*task.TaskInfo
}

func (suite *TaskUtilTestSuite) SetupTest() {
	suite.testJobID = &peloton.JobID{
		Value: "test_job",
	}
	suite.testJobConfig = &job.JobConfig{
		Name:          suite.testJobID.Value,
		InstanceCount: testInstanceCount,
		Sla: &job.SlaConfig{
			Preemptible:             true,
			Priority:                22,
			MaximumRunningInstances: 2,
			MinimumRunningInstances: 1,
		},
	}
	var taskInfos = make(map[uint32]*task.TaskInfo)
	for i := uint32(0); i < testInstanceCount; i++ {
		taskInfos[i] = suite.createTestTaskInfo(
			task.TaskState_RUNNING, i)
	}
	suite.taskInfos = taskInfos
}

func (suite *TaskUtilTestSuite) TearDownTest() {
	log.Debug("tearing down")
}

func TestTaskUtilTestSuite(t *testing.T) {
	suite.Run(t, new(TaskUtilTestSuite))
}

func (suite *TaskUtilTestSuite) createTestTaskInfo(
	state task.TaskState,
	instanceID uint32) *task.TaskInfo {

	var taskID = fmt.Sprintf("%s-%d", suite.testJobID.Value, instanceID)
	return &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			TaskId:    &mesos.TaskID{Value: &taskID},
			State:     state,
			GoalState: task.TaskState_SUCCEEDED,
		},
		Config: &task.TaskConfig{
			Name:     suite.testJobConfig.Name,
			Resource: &defaultResourceConfig,
		},
		InstanceId: instanceID,
		JobId:      suite.testJobID,
	}
}

func (suite *TaskUtilTestSuite) TestEnqueueTasks() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockResmgrClient := yarpc_mocks.NewMockClient(ctrl)
	var tasksInfo []*task.TaskInfo
	for _, v := range suite.taskInfos {
		tasksInfo = append(tasksInfo, v)
	}
	tasks := ConvertToResMgrTask(tasksInfo, suite.testJobConfig)
	var expectedTasks []*resmgr.Task
	gomock.InOrder(
		mockResmgrClient.EXPECT().
			Call(
				gomock.Any(),
				gomock.Eq(yarpc.NewReqMeta().Procedure("ResourceManagerService.EnqueueTasks")),
				gomock.Eq(&resmgrsvc.EnqueueTasksRequest{
					Tasks: tasks,
				}),
				gomock.Any()).
			Do(func(_ context.Context, _ yarpc.CallReqMeta, reqBody interface{}, _ interface{}) {
				req := reqBody.(*resmgrsvc.EnqueueTasksRequest)
				for _, t := range req.Tasks {
					expectedTasks = append(expectedTasks, t)
				}
			}).
			Return(nil, nil),
	)

	EnqueueTasks(tasksInfo, suite.testJobConfig, mockResmgrClient)
	suite.Equal(tasks, expectedTasks)
}

func (suite *TaskUtilTestSuite) TestEnqueueTasksFailure() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockResmgrClient := yarpc_mocks.NewMockClient(ctrl)
	var tasksInfo []*task.TaskInfo
	for _, v := range suite.taskInfos {
		tasksInfo = append(tasksInfo, v)
	}
	tasks := ConvertToResMgrTask(tasksInfo, suite.testJobConfig)
	var expectedTasks []*resmgr.Task
	var err error
	gomock.InOrder(
		mockResmgrClient.EXPECT().
			Call(
				gomock.Any(),
				gomock.Eq(yarpc.NewReqMeta().Procedure("ResourceManagerService.EnqueueTasks")),
				gomock.Eq(&resmgrsvc.EnqueueTasksRequest{
					Tasks: tasks,
				}),
				gomock.Any()).
			Do(func(_ context.Context, _ yarpc.CallReqMeta, reqBody interface{}, _ interface{}) {
				req := reqBody.(*resmgrsvc.EnqueueTasksRequest)
				for _, t := range req.Tasks {
					expectedTasks = append(expectedTasks, t)
				}
				err = errors.New("Resmgr Error")
			}).
			Return(nil, err),
	)
	EnqueueTasks(tasksInfo, suite.testJobConfig, mockResmgrClient)
	suite.Error(err)
}
