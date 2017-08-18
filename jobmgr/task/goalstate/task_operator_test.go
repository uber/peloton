package goalstate

import (
	"context"
	"testing"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	host_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	resmgr_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestTaskOperatorStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	hostMock := host_mocks.NewMockInternalHostServiceYARPCClient(ctrl)
	resMgrMock := resmgr_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)

	to := &taskOperator{
		hostmgrClient: hostMock,
		resmgrClient:  resMgrMock,
	}

	taskID := &mesos_v1.TaskID{
		Value: &[]string{"3c8a3c3e-71e3-49c5-9aed-2929823f595c-1-3c8a3c3e-71e3-49c5-9aed-2929823f5957"}[0],
	}

	pelotonTaskID := &peloton.TaskID{
		Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c-1",
	}

	hostMock.EXPECT().KillTasks(context.Background(), &hostsvc.KillTasksRequest{
		TaskIds: []*mesos_v1.TaskID{taskID},
	}).Return(nil, nil)

	resMgrMock.EXPECT().KillTasks(context.Background(), &resmgrsvc.KillTasksRequest{
		Tasks: []*peloton.TaskID{pelotonTaskID},
	}).Return(&resmgrsvc.KillTasksResponse{}, nil)

	assert.NoError(t, to.StopTask(context.Background(), &task.RuntimeInfo{
		MesosTaskId: taskID,
	}))
}
