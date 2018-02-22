package tracked

import (
	"context"
	"fmt"
	"testing"

	mesos_v1 "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	storage_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestTaskFailNoRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTaskStore := storage_mocks.NewMockTaskStore(ctrl)
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)
	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID.GetValue(), instanceID, uuid.NewUUID().String())

	runtime := &pb_task.RuntimeInfo{
		MesosTaskId:   &mesos_v1.TaskID{Value: &mesosTaskID},
		State:         pb_task.TaskState_FAILED,
		GoalState:     pb_task.TaskState_SUCCEEDED,
		ConfigVersion: 1,
		Message:       "testFailure",
		Reason:        mesos_v1.TaskStatus_REASON_COMMAND_EXECUTOR_FAILED.String(),
	}

	taskConfig := pb_task.TaskConfig{
		RestartPolicy: &pb_task.RestartPolicy{
			MaxFailures: 0,
		},
	}

	tt := &task{
		job: &job{
			m: &manager{
				jobs:          map[string]*job{},
				taskScheduler: newScheduler(NewQueueMetrics(tally.NoopScope)),
				jobScheduler:  newScheduler(NewQueueMetrics(tally.NoopScope)),
				taskStore:     mockTaskStore,
				mtx:           NewMetrics(tally.NoopScope),
				running:       true,
			},
			tasks: map[uint32]*task{},
			id:    jobID,
		},
		id:      uint32(instanceID),
		runtime: runtime,
	}

	tt.job.m.jobs[tt.job.id.Value] = tt.job
	tt.job.tasks[tt.id] = tt

	mockTaskStore.EXPECT().
		GetTaskConfig(gomock.Any(), jobID, instanceID, gomock.Any()).Return(&taskConfig, nil)

	reschedule, err := tt.RunAction(context.Background(), FailRetryAction)
	assert.False(t, reschedule)
	assert.NoError(t, err)
}

func TestTaskFailRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTaskStore := storage_mocks.NewMockTaskStore(ctrl)
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)
	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID.GetValue(), instanceID, uuid.NewUUID().String())

	runtime := &pb_task.RuntimeInfo{
		MesosTaskId:   &mesos_v1.TaskID{Value: &mesosTaskID},
		State:         pb_task.TaskState_FAILED,
		GoalState:     pb_task.TaskState_SUCCEEDED,
		ConfigVersion: 1,
		Message:       "testFailure",
		Reason:        mesos_v1.TaskStatus_REASON_COMMAND_EXECUTOR_FAILED.String(),
	}

	taskConfig := pb_task.TaskConfig{
		RestartPolicy: &pb_task.RestartPolicy{
			MaxFailures: 3,
		},
	}

	tt := &task{
		job: &job{
			m: &manager{
				jobs:          map[string]*job{},
				taskScheduler: newScheduler(NewQueueMetrics(tally.NoopScope)),
				jobScheduler:  newScheduler(NewQueueMetrics(tally.NoopScope)),
				taskStore:     mockTaskStore,
				mtx:           NewMetrics(tally.NoopScope),
				running:       true,
			},
			tasks: map[uint32]*task{},
			id:    jobID,
		},
		id:      uint32(instanceID),
		runtime: runtime,
	}

	tt.job.m.jobs[tt.job.id.Value] = tt.job
	tt.job.tasks[tt.id] = tt

	mockTaskStore.EXPECT().
		GetTaskConfig(gomock.Any(), jobID, instanceID, gomock.Any()).Return(&taskConfig, nil)
	mockTaskStore.EXPECT().
		UpdateTaskRuntimes(gomock.Any(), jobID, gomock.Any()).Return(nil)

	reschedule, err := tt.RunAction(context.Background(), FailRetryAction)
	assert.True(t, reschedule)
	assert.NoError(t, err)
	assert.True(t, tt.IsScheduled())
	assert.True(t, tt.runtime.GetMesosTaskId().GetValue() != mesosTaskID)
	assert.True(t, tt.runtime.GetPrevMesosTaskId().GetValue() == mesosTaskID)
	assert.True(t, tt.runtime.GetState() == pb_task.TaskState_INITIALIZED)
}

func TestTaskFailSystemFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTaskStore := storage_mocks.NewMockTaskStore(ctrl)
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)
	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID.GetValue(), instanceID, uuid.NewUUID().String())

	runtime := &pb_task.RuntimeInfo{
		MesosTaskId:   &mesos_v1.TaskID{Value: &mesosTaskID},
		State:         pb_task.TaskState_FAILED,
		GoalState:     pb_task.TaskState_SUCCEEDED,
		ConfigVersion: 1,
		Message:       "testFailure",
		Reason:        mesos_v1.TaskStatus_REASON_CONTAINER_LAUNCH_FAILED.String(),
	}

	taskConfig := pb_task.TaskConfig{
		RestartPolicy: &pb_task.RestartPolicy{
			MaxFailures: 0,
		},
	}

	tt := &task{
		job: &job{
			m: &manager{
				jobs:          map[string]*job{},
				taskScheduler: newScheduler(NewQueueMetrics(tally.NoopScope)),
				jobScheduler:  newScheduler(NewQueueMetrics(tally.NoopScope)),
				taskStore:     mockTaskStore,
				mtx:           NewMetrics(tally.NoopScope),
				running:       true,
			},
			tasks: map[uint32]*task{},
			id:    jobID,
		},
		id:      uint32(instanceID),
		runtime: runtime,
	}

	tt.job.m.jobs[tt.job.id.Value] = tt.job
	tt.job.tasks[tt.id] = tt

	mockTaskStore.EXPECT().
		GetTaskConfig(gomock.Any(), jobID, instanceID, gomock.Any()).Return(&taskConfig, nil)
	mockTaskStore.EXPECT().
		UpdateTaskRuntimes(gomock.Any(), jobID, gomock.Any()).Return(nil)

	reschedule, err := tt.RunAction(context.Background(), FailRetryAction)
	assert.True(t, reschedule)
	assert.NoError(t, err)
	assert.True(t, tt.IsScheduled())
	assert.True(t, tt.runtime.GetMesosTaskId().GetValue() != mesosTaskID)
	assert.True(t, tt.runtime.GetPrevMesosTaskId().GetValue() == mesosTaskID)
	assert.True(t, tt.runtime.GetState() == pb_task.TaskState_INITIALIZED)
}

func TestTaskFailDBError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTaskStore := storage_mocks.NewMockTaskStore(ctrl)
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)
	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID.GetValue(), instanceID, uuid.NewUUID().String())

	runtime := &pb_task.RuntimeInfo{
		MesosTaskId:   &mesos_v1.TaskID{Value: &mesosTaskID},
		State:         pb_task.TaskState_FAILED,
		GoalState:     pb_task.TaskState_SUCCEEDED,
		ConfigVersion: 1,
		Message:       "testFailure",
		Reason:        mesos_v1.TaskStatus_REASON_COMMAND_EXECUTOR_FAILED.String(),
	}

	tt := &task{
		job: &job{
			m: &manager{
				jobs:          map[string]*job{},
				taskScheduler: newScheduler(NewQueueMetrics(tally.NoopScope)),
				jobScheduler:  newScheduler(NewQueueMetrics(tally.NoopScope)),
				taskStore:     mockTaskStore,
				mtx:           NewMetrics(tally.NoopScope),
				running:       true,
			},
			tasks: map[uint32]*task{},
			id:    jobID,
		},
		id:      uint32(instanceID),
		runtime: runtime,
	}

	tt.job.m.jobs[tt.job.id.Value] = tt.job
	tt.job.tasks[tt.id] = tt

	mockTaskStore.EXPECT().
		GetTaskConfig(gomock.Any(), jobID, instanceID, gomock.Any()).Return(nil, fmt.Errorf("fake db error"))

	reschedule, err := tt.RunAction(context.Background(), FailRetryAction)
	assert.True(t, reschedule)
	assert.Error(t, err)
}
