package task

import (
	"context"
	"errors"
	"fmt"
	"testing"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	res_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"
	"code.uber.internal/infra/peloton/util"
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
			MesosTaskId: &mesos.TaskID{Value: &taskID},
			State:       state,
			GoalState:   task.TaskState_SUCCEEDED,
		},
		Config: &task.TaskConfig{
			Name:     suite.testJobConfig.Name,
			Resource: &defaultResourceConfig,
		},
		InstanceId: instanceID,
		JobId:      suite.testJobID,
	}
}

func (suite *TaskUtilTestSuite) TestEnqueueGangs() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockResmgrClient := res_mocks.NewMockResourceManagerServiceYarpcClient(ctrl)
	var tasksInfo []*task.TaskInfo
	for _, v := range suite.taskInfos {
		tasksInfo = append(tasksInfo, v)
	}
	gangs := util.ConvertToResMgrGangs(tasksInfo, suite.testJobConfig)
	var expectedGangs []*resmgrsvc.Gang
	gomock.InOrder(
		mockResmgrClient.EXPECT().EnqueueGangs(
			gomock.Any(),
			gomock.Eq(&resmgrsvc.EnqueueGangsRequest{
				Gangs: gangs,
			})).
			Do(func(_ context.Context, reqBody interface{}) {
				req := reqBody.(*resmgrsvc.EnqueueGangsRequest)
				for _, g := range req.Gangs {
					expectedGangs = append(expectedGangs, g)
				}
			}).
			Return(nil, nil),
	)

	EnqueueGangs(context.Background(), tasksInfo, suite.testJobConfig, mockResmgrClient)
	suite.Equal(gangs, expectedGangs)
}

func (suite *TaskUtilTestSuite) TestEnqueueGangsFailure() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockResmgrClient := res_mocks.NewMockResourceManagerServiceYarpcClient(ctrl)
	var tasksInfo []*task.TaskInfo
	for _, v := range suite.taskInfos {
		tasksInfo = append(tasksInfo, v)
	}
	gangs := util.ConvertToResMgrGangs(tasksInfo, suite.testJobConfig)
	var expectedGangs []*resmgrsvc.Gang
	var err error
	gomock.InOrder(
		mockResmgrClient.EXPECT().EnqueueGangs(
			gomock.Any(),
			gomock.Eq(&resmgrsvc.EnqueueGangsRequest{
				Gangs: gangs,
			})).
			Do(func(_ context.Context, reqBody interface{}) {
				req := reqBody.(*resmgrsvc.EnqueueGangsRequest)
				for _, g := range req.Gangs {
					expectedGangs = append(expectedGangs, g)
				}
			}).
			Return(nil, errors.New("Resmgr Error")),
	)
	err = EnqueueGangs(context.Background(), tasksInfo, suite.testJobConfig, mockResmgrClient)
	suite.Error(err)
}
