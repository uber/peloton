package task

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	res_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"
	"code.uber.internal/infra/peloton/util"
	"github.com/pborman/uuid"
)

const (
	testInstanceCount = 2
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
			Preemptible:               true,
			Priority:                  22,
			MaximumRunningInstances:   2,
			MinimumSchedulingUnitSize: 1,
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

	mtID := util.BuildMesosTaskID(util.BuildTaskID(suite.testJobID, instanceID), uuid.NewUUID().String())
	return &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			MesosTaskId: mtID,
			State:       state,
			GoalState:   task.TaskGoalState_SUCCEED,
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

	mockResmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	var tasksInfo []*task.TaskInfo
	for _, v := range suite.taskInfos {
		tasksInfo = append(tasksInfo, v)
	}
	gangs := util.ConvertToResMgrGangs(tasksInfo, suite.testJobConfig.GetSla())
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

	EnqueueGangs(context.Background(), tasksInfo, suite.testJobConfig.GetRespoolID(), suite.testJobConfig.GetSla(), mockResmgrClient)
	suite.Equal(gangs, expectedGangs)
}

func (suite *TaskUtilTestSuite) TestEnqueueGangsFailure() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockResmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	var tasksInfo []*task.TaskInfo
	for _, v := range suite.taskInfos {
		tasksInfo = append(tasksInfo, v)
	}
	gangs := util.ConvertToResMgrGangs(tasksInfo, suite.testJobConfig.GetSla())
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
	err = EnqueueGangs(context.Background(), tasksInfo, suite.testJobConfig.GetRespoolID(), suite.testJobConfig.GetSla(), mockResmgrClient)
	suite.Error(err)
}
