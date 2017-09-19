package tracked

import (
	"context"
	"testing"

	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	storage_mocks "code.uber.internal/infra/peloton/storage/mocks"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestTaskStartInitialized(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockJobStore := storage_mocks.NewMockJobStore(ctrl)
	mockTaskStore := storage_mocks.NewMockTaskStore(ctrl)

	tt := &task{
		id: 12345,
		job: &job{
			id: &peloton.JobID{
				Value: "my-job-id",
			},
			m: &manager{
				jobStore:  mockJobStore,
				taskStore: mockTaskStore,
				mtx:       newMetrics(tally.NoopScope),
			},
		},
		runtime: &pb_task.RuntimeInfo{},
	}

	jobConfig := &pb_job.JobConfig{
		RespoolID: &peloton.ResourcePoolID{
			Value: "my-respool-id",
		},
	}

	mockJobStore.EXPECT().
		GetJobConfig(gomock.Any(), tt.job.id, uint64(0)).Return(jobConfig, nil)
	mockTaskStore.EXPECT().
		UpdateTaskRuntime(gomock.Any(), tt.job.id, tt.id, gomock.Any()).
		Do(func(_, _, _ interface{}, runtime *pb_task.RuntimeInfo) {
			assert.Equal(t, pb_task.TaskGoalState_SUCCEED, runtime.GoalState)
			assert.Equal(t, pb_task.TaskState_INITIALIZED, runtime.State)
			assert.NotNil(t, runtime.MesosTaskId)
		}).
		Return(nil)

	assert.NoError(t, tt.RunAction(context.Background(), InitializeAction))
	assert.Nil(t, tt.runtime.MesosTaskId)
}
