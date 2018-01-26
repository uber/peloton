package tracked

import (
	"context"
	"testing"

	mesos_v1 "code.uber.internal/infra/peloton/.gen/mesos/v1"
	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	res_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"
	storage_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestTaskLaunchTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTaskStore := storage_mocks.NewMockTaskStore(ctrl)
	mockJobStore := storage_mocks.NewMockJobStore(ctrl)

	tt := &task{
		job: &job{
			m: &manager{
				mtx:           NewMetrics(tally.NoopScope),
				taskStore:     mockTaskStore,
				jobStore:      mockJobStore,
				jobs:          map[string]*job{},
				taskScheduler: newScheduler(NewQueueMetrics(tally.NoopScope)),
				jobScheduler:  newScheduler(NewQueueMetrics(tally.NoopScope)),
				running:       true,
			},
			id:    &peloton.JobID{Value: uuid.NewRandom().String()},
			tasks: map[uint32]*task{},
		},
		id: uint32(0),
	}
	tt.job.m.jobs[tt.job.id.Value] = tt.job
	tt.job.tasks[tt.id] = tt

	oldMesosTaskID := uuid.New()
	taskInfo := &pb_task.TaskInfo{
		InstanceId: 0,
		JobId:      tt.job.id,
		Runtime: &pb_task.RuntimeInfo{
			State: pb_task.TaskState_LAUNCHED,
			MesosTaskId: &mesos_v1.TaskID{
				Value: &oldMesosTaskID,
			},
			GoalState: pb_task.TaskState_SUCCEEDED,
		},
	}
	jobConfig := &pb_job.JobConfig{
		Type: pb_job.JobType_BATCH,
	}

	mockTaskStore.EXPECT().GetTaskByID(gomock.Any(), gomock.Any()).Return(taskInfo, nil)
	mockJobStore.EXPECT().GetJobConfig(gomock.Any(), gomock.Any()).Return(jobConfig, nil)
	mockTaskStore.EXPECT().UpdateTaskRuntimes(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(_ context.Context, jobID *peloton.JobID, runtimes map[uint32]*pb_task.RuntimeInfo) {
			for _, updatedRuntimeInfo := range runtimes {
				taskInfo.Runtime = updatedRuntimeInfo
			}
		}).Return(nil)

	reschedule, err := tt.RunAction(context.Background(), LaunchRetryAction)
	assert.True(t, reschedule)
	assert.NoError(t, err)
	assert.NotEqual(t, oldMesosTaskID, taskInfo.Runtime.MesosTaskId)
	assert.Equal(t, pb_task.TaskState_INITIALIZED, taskInfo.Runtime.State)
	assert.Equal(t, pb_task.TaskState_SUCCEEDED, taskInfo.Runtime.GoalState)
	assert.True(t, tt.IsScheduled())
}

func TestTaskSendLaunchInfoResMgr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	resmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)

	tt := &task{
		job: &job{
			m: &manager{
				mtx:           NewMetrics(tally.NoopScope),
				resmgrClient:  resmgrClient,
				jobs:          map[string]*job{},
				taskScheduler: newScheduler(NewQueueMetrics(tally.NoopScope)),
				jobScheduler:  newScheduler(NewQueueMetrics(tally.NoopScope)),
				running:       true,
			},
			id:    &peloton.JobID{Value: uuid.NewRandom().String()},
			tasks: map[uint32]*task{},
		},
		id: uint32(0),
	}
	tt.job.m.jobs[tt.job.id.Value] = tt.job
	tt.job.tasks[tt.id] = tt

	resmgrClient.EXPECT().
		MarkTasksLaunched(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.MarkTasksLaunchedResponse{}, nil)

	reschedule, err := tt.RunAction(context.Background(), NotifyLaunchedTasksAction)
	assert.True(t, reschedule)
	assert.NoError(t, err)
}
