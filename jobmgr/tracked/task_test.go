package tracked

import (
	"context"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	host_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestTaskRunAction(t *testing.T) {
	tt := &task{
		lastAction: StopAction,
	}

	before := time.Now()

	assert.NoError(t, tt.RunAction(context.Background(), NoAction))

	la, lt := tt.LastAction()
	assert.Equal(t, NoAction, la)
	assert.True(t, lt.After(before))
}

func TestTaskStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	hostMock := host_mocks.NewMockInternalHostServiceYARPCClient(ctrl)

	tt := &task{
		job: &job{
			m: &manager{
				hostmgrClient: hostMock,
			},
		},
	}

	assert.EqualError(t, tt.RunAction(context.Background(), StopAction), "tracked task has no runtime info assigned")

	taskID := &mesos_v1.TaskID{
		Value: &[]string{"3c8a3c3e-71e3-49c5-9aed-2929823f595c-1-3c8a3c3e-71e3-49c5-9aed-2929823f5957"}[0],
	}

	hostMock.EXPECT().KillTasks(context.Background(), &hostsvc.KillTasksRequest{
		TaskIds: []*mesos_v1.TaskID{taskID},
	}).Return(nil, nil)

	tt.runtime = &pb_task.RuntimeInfo{MesosTaskId: taskID}
	assert.NoError(t, tt.RunAction(context.Background(), StopAction))
}
