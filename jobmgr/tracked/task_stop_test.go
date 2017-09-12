package tracked

import (
	"context"
	"fmt"
	"testing"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	host_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	res_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"
	storage_mocks "code.uber.internal/infra/peloton/storage/mocks"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestTaskStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	hostMock := host_mocks.NewMockInternalHostServiceYARPCClient(ctrl)

	tt := &task{
		job: &job{
			m: &manager{
				hostmgrClient: hostMock,
				mtx:           newMetrics(tally.NoopScope),
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

func TestTaskStopIfInitializedCallsKillOnResmgr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResmgr := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	mockTaskStore := storage_mocks.NewMockTaskStore(ctrl)

	m := &manager{
		jobs:             map[string]*job{},
		taskQueue:        newDeadlineQueue(newMetrics(tally.NoopScope)),
		taskQueueChanged: make(chan struct{}, 1),
		taskStore:        mockTaskStore,
		resmgrClient:     mockResmgr,
		mtx:              newMetrics(tally.NoopScope),
		running:          true,
	}

	jobID := &peloton.JobID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c"}
	m.SetTask(jobID, 7, &pb_task.RuntimeInfo{
		State: pb_task.TaskState_INITIALIZED,
	})
	tt := m.GetJob(jobID).GetTask(7).(*task)
	taskID := &peloton.TaskID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c-7"}
	var killResponseErr []*resmgrsvc.KillTasksResponse_Error
	killResponseErr = append(killResponseErr,
		&resmgrsvc.KillTasksResponse_Error{
			NotFound: &resmgrsvc.TasksNotFound{
				Message: "Tasks Not Found",
				Task:    taskID,
			},
		})
	res := &resmgrsvc.KillTasksResponse{
		Error: killResponseErr,
	}
	mockResmgr.EXPECT().KillTasks(context.Background(), &resmgrsvc.KillTasksRequest{
		Tasks: []*peloton.TaskID{
			{
				Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c-7",
			},
		},
	}).Return(res, nil)

	taskInfo := &pb_task.TaskInfo{
		InstanceId: 7,
		Runtime: &pb_task.RuntimeInfo{
			State: pb_task.TaskState_INITIALIZED,
		},
	}
	mockTaskStore.EXPECT().
		GetTaskByID(gomock.Any(), fmt.Sprintf("%s-%d", tt.job.id.Value, tt.id)).Return(taskInfo, nil)
	mockTaskStore.EXPECT().
		UpdateTask(gomock.Any(), taskInfo).Return(nil)

	assert.NoError(t, tt.RunAction(context.Background(), StopAction))

	// Test that it's rescheduled immediatly as we updated the state.
	assert.Equal(t, tt, tt.job.m.WaitForScheduledTask(nil))
}
