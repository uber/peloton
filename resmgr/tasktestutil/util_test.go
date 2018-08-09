package tasktestutil

import (
	"fmt"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	rmock "code.uber.internal/infra/peloton/resmgr/respool/mocks"
	"code.uber.internal/infra/peloton/resmgr/scalar"
	rm_task "code.uber.internal/infra/peloton/resmgr/task"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type TestUtilTestSuite struct {
	suite.Suite
}

// TestCreateConfig tests the create config
func (suite *TestUtilTestSuite) TestCreateConfig() {
	taskConfig := CreateTaskConfig()
	suite.Equal(taskConfig.PlacingTimeout, 1*time.Minute)
}

// TestValidateResources validates the resources
func (suite *TestUtilTestSuite) TestValidateResources() {
	res := &scalar.Resources{
		CPU:    33,
		GPU:    0,
		MEMORY: 333,
		DISK:   1000,
	}

	suite.True(ValidateResources(res,
		map[string]int64{"CPU": 33, "GPU": 0, "MEMORY": 333, "DISK": 1000}))

	res = &scalar.Resources{
		CPU:    33,
		GPU:    20,
		MEMORY: 333,
		DISK:   1000,
	}

	suite.False(ValidateResources(res,
		map[string]int64{"CPU": 33, "GPU": 0, "MEMORY": 333, "DISK": 1000}))
}

// TestValidateTransitions validates the transitions
func (suite *TestUtilTestSuite) TestValidateTransitions() {
	mockCtrl := gomock.NewController(suite.T())
	mockHostmgr := mocks.NewMockInternalHostServiceYARPCClient(
		mockCtrl)
	mockHostmgr.EXPECT().
		MarkHostDrained(
			gomock.Any(),
			gomock.Any()).
		Return(
			&hostsvc.MarkHostDrainedResponse{},
			nil,
		).
		AnyTimes()
	rm_task.InitTaskTracker(tally.NoopScope, CreateTaskConfig(), mockHostmgr)
	rmTaskTracker := rm_task.GetTracker()
	mockResPool := rmock.NewMockResPool(mockCtrl)
	mockResPool.EXPECT().GetPath().Return("/mock/path")

	rmTaskTracker.AddTask(
		suite.pendingGang0().Tasks[0],
		nil,
		mockResPool,
		CreateTaskConfig())
	rmtask := rmTaskTracker.GetTask(suite.pendingGang0().Tasks[0].Id)

	err := rmtask.TransitTo(task.TaskState_PENDING.String())
	suite.NoError(err)
	ValidateStateTransitions(rmtask, []task.TaskState{
		task.TaskState_READY,
		task.TaskState_PLACING,
		task.TaskState_PLACED,
		task.TaskState_LAUNCHING})
}

func (suite *TestUtilTestSuite) pendingGang0() *resmgrsvc.Gang {
	var gang resmgrsvc.Gang
	uuidStr := "uuidstr-1"
	jobID := "job1"
	instance := 1
	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID, instance, uuidStr)
	gang.Tasks = []*resmgr.Task{
		{
			Name:     "job1-1",
			Priority: 0,
			JobId:    &peloton.JobID{Value: "job1"},
			Id:       &peloton.TaskID{Value: fmt.Sprintf("%s-%d", jobID, instance)},
			Resource: &task.ResourceConfig{
				CpuLimit:    1,
				DiskLimitMb: 10,
				GpuLimit:    0,
				MemLimitMb:  100,
			},
			TaskId: &mesos_v1.TaskID{
				Value: &mesosTaskID,
			},
			Preemptible:             true,
			PlacementTimeoutSeconds: 60,
			PlacementRetryCount:     1,
		},
	}
	return &gang
}

func TestTaskTestUtil(t *testing.T) {
	suite.Run(t, new(TestUtilTestSuite))
}
