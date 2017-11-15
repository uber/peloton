package tracked

import (
	"context"
	"testing"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	peloton_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestTaskInitialize(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	oldMesosTaskID := uuid.New()
	taskInfo := &peloton_task.TaskInfo{
		InstanceId: 1,
		JobId:      &peloton.JobID{Value: uuid.New()},
		Runtime: &peloton_task.RuntimeInfo{
			State: peloton_task.TaskState_KILLED,
			MesosTaskId: &mesos_v1.TaskID{
				Value: &oldMesosTaskID,
			},
		},
	}
	mockTaskStore := mocks.NewMockTaskStore(ctrl)
	mockTaskStore.EXPECT().GetTaskByID(gomock.Any(), gomock.Any()).Return(taskInfo, nil)
	mockTaskStore.EXPECT().UpdateTask(gomock.Any(), gomock.Any()).Do(
		func(_ context.Context, updatedTaskInfo *peloton_task.TaskInfo) {
			taskInfo = updatedTaskInfo
		}).Return(nil)

	jobConfig := &pb_job.JobConfig{
		Type: pb_job.JobType_BATCH,
	}
	mockJobStore := mocks.NewMockJobStore(ctrl)
	mockJobStore.EXPECT().GetJobConfig(gomock.Any(), gomock.Any()).Return(jobConfig, nil)

	tt := &task{
		job: &job{
			m: &manager{
				mtx:       newMetrics(tally.NoopScope),
				taskStore: mockTaskStore,
				jobStore:  mockJobStore,
			},
		},
	}

	reschedule, err := tt.RunAction(context.Background(), InitializeAction)
	assert.True(t, reschedule)
	assert.NoError(t, err)
	assert.NotEqual(t, oldMesosTaskID, taskInfo.Runtime.MesosTaskId)
	assert.Equal(t, peloton_task.TaskState_INITIALIZED, taskInfo.Runtime.State)
	assert.Equal(t, peloton_task.TaskState_SUCCEEDED, taskInfo.Runtime.GoalState)
}
