package tracked

import (
	"context"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestTaskRunAction(t *testing.T) {
	tt := &task{
		lastAction: StopAction,
		job: &job{
			m: &manager{
				mtx: NewMetrics(tally.NoopScope),
			},
		},
	}

	assert.False(t, tt.IsScheduled())

	before := time.Now()

	reschedule, err := tt.RunAction(context.Background(), NoAction)
	assert.False(t, reschedule)
	assert.NoError(t, err)

	la, lt := tt.LastAction()
	assert.Equal(t, NoAction, la)
	assert.True(t, lt.After(before))
}

func TestTaskReloadRuntime(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskstoreMock := store_mocks.NewMockTaskStore(ctrl)

	tt := &task{
		lastAction: StopAction,
		job: &job{
			m: &manager{
				mtx:       NewMetrics(tally.NoopScope),
				taskStore: taskstoreMock,
			},
		},
	}

	taskstoreMock.EXPECT().
		GetTaskRuntime(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
	err := tt.reloadRuntime(context.Background())
	assert.Error(t, err)
}

func TestTaskFailAction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskstoreMock := store_mocks.NewMockTaskStore(ctrl)

	tt := &task{
		lastAction: StopAction,
		job: &job{
			m: &manager{
				mtx:           NewMetrics(tally.NoopScope),
				taskStore:     taskstoreMock,
				jobs:          map[string]*job{},
				running:       true,
				taskScheduler: newScheduler(NewQueueMetrics(tally.NoopScope)),
				jobScheduler:  newScheduler(NewQueueMetrics(tally.NoopScope)),
				stopChan:      make(chan struct{}),
			},
			id:    &peloton.JobID{Value: uuid.NewRandom().String()},
			tasks: map[uint32]*task{},
		},
		id: 0,
	}

	runtime := &pb_task.RuntimeInfo{
		State:     pb_task.TaskState_INITIALIZED,
		GoalState: pb_task.TaskState_FAILED,
	}
	newRuntimes := make(map[uint32]*pb_task.RuntimeInfo)

	newRuntimes[0] = &pb_task.RuntimeInfo{
		State:     pb_task.TaskState_FAILED,
		GoalState: pb_task.TaskState_FAILED,
	}

	taskstoreMock.EXPECT().
		GetTaskRuntime(gomock.Any(), tt.job.id, tt.id).Return(runtime, nil)
	taskstoreMock.EXPECT().
		UpdateTaskRuntimes(gomock.Any(), tt.job.id, newRuntimes).Return(nil)
	reschedule, err := tt.RunAction(context.Background(), FailAction)
	assert.True(t, reschedule)
	assert.NoError(t, err)
}
