package task

import (
	"context"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	storemocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/yarpcerrors"
)

type TaskTestSuite struct {
	suite.Suite
	ctrl      *gomock.Controller
	taskStore *storemocks.MockTaskStore
	jobID     *peloton.JobID
}

func TestTask(t *testing.T) {
	suite.Run(t, new(TaskTestSuite))
}

func (suite *TaskTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.taskStore = storemocks.NewMockTaskStore(suite.ctrl)
	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
}

func (suite *TaskTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestTasksRunInParallel tests an action taken on list of instances
// in parallel
func (suite *TaskTestSuite) TestTasksRunInParallel() {
	instances := []uint32{0, 1, 2, 3, 4}
	taskConfig := &pbtask.TaskConfig{
		Name: "test-instance",
		Resource: &pbtask.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
		},
	}

	createSingleTaskConfig := func(id uint32) error {
		return suite.taskStore.CreateTaskConfig(
			context.Background(),
			suite.jobID,
			int64(id),
			taskConfig,
			nil,
			1,
		)
	}

	for _, i := range instances {
		suite.taskStore.EXPECT().
			CreateTaskConfig(
				gomock.Any(),
				suite.jobID,
				int64(i),
				taskConfig,
				nil,
				uint64(1)).
			Return(nil)
	}

	RunInParallel(suite.jobID.GetValue(), instances, createSingleTaskConfig)
}

// TestTaskRunInParallelFail tests failure scenario for running task action
// in parallel
func (suite *TaskTestSuite) TestTaskRunInParallelFail() {
	instances := []uint32{0, 1, 2, 3, 4}
	taskConfig := &pbtask.TaskConfig{
		Name: "test-instance",
		Resource: &pbtask.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
		},
	}

	createSingleTaskConfig := func(id uint32) error {
		return suite.taskStore.CreateTaskConfig(
			context.Background(),
			suite.jobID,
			int64(id),
			taskConfig,
			nil,
			1,
		)
	}

	suite.taskStore.EXPECT().
		CreateTaskConfig(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			taskConfig,
			nil,
			uint64(1)).
		Return(yarpcerrors.AbortedErrorf("db error")).
		AnyTimes()

	suite.Error(RunInParallel(suite.jobID.GetValue(), instances, createSingleTaskConfig))
}
