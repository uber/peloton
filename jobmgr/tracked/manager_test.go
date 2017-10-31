package tracked

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestManagerAddAndGet(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}

	m := &manager{
		jobs: map[string]*job{},
	}

	assert.Nil(t, m.GetJob(jobID))

	j := m.addJob(jobID, nil)
	assert.NotNil(t, j)

	assert.Equal(t, j, m.GetJob(jobID))

	jj := m.addJob(jobID, nil)
	assert.Equal(t, j, jj)
}

func TestManagerClearTask(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}

	m := &manager{
		jobs:          map[string]*job{},
		taskScheduler: newScheduler(newMetrics(tally.NoopScope)),
		running:       true,
	}

	j := m.addJob(jobID, nil)
	m.SetTask(jobID, 0, &pb_task.RuntimeInfo{})
	m.SetTask(jobID, 1, &pb_task.RuntimeInfo{})
	t0 := j.GetTask(0)
	t1 := j.GetTask(1)
	assert.Equal(t, 1, len(m.jobs))
	assert.Equal(t, 2, len(j.tasks))

	m.clearTask(t0.(*task))
	assert.Equal(t, 1, len(m.jobs))
	assert.Equal(t, 2, len(j.tasks))

	m.WaitForScheduledTask(nil)
	m.WaitForScheduledTask(nil)
	m.clearTask(t0.(*task))
	assert.Equal(t, 1, len(m.jobs))
	assert.Equal(t, 1, len(j.tasks))

	m.clearTask(t1.(*task))
	assert.Equal(t, 0, len(m.jobs))
	assert.Equal(t, 0, len(j.tasks))

	m.clearTask(t1.(*task))
}

func TestManagerSyncFromDB(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobstoreMock := store_mocks.NewMockJobStore(ctrl)
	taskstoreMock := store_mocks.NewMockTaskStore(ctrl)

	m := &manager{
		jobs:          map[string]*job{},
		jobStore:      jobstoreMock,
		taskStore:     taskstoreMock,
		taskScheduler: newScheduler(newMetrics(tally.NoopScope)),
		running:       true,
	}

	jobstoreMock.EXPECT().GetAllJobs(gomock.Any()).Return(map[string]*pb_job.RuntimeInfo{
		"3c8a3c3e-71e3-49c5-9aed-2929823f595c": nil,
	}, nil)

	jobID := &peloton.JobID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c"}
	taskstoreMock.EXPECT().GetTasksForJob(gomock.Any(), jobID).
		Return(map[uint32]*pb_task.TaskInfo{
			1: {
				JobId:      jobID,
				InstanceId: 1,
				Runtime: &pb_task.RuntimeInfo{
					GoalState:            pb_task.TaskGoalState_RUN,
					DesiredConfigVersion: 42,
					ConfigVersion:        42,
				},
			},
		}, nil)

	m.syncFromDB(context.Background())

	task := m.GetJob(jobID).GetTask(1)
	assert.NotNil(t, task)
	assert.Equal(t, task, m.WaitForScheduledTask(nil))
}

func TestManagerStopClearsTasks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := &manager{
		jobs:          map[string]*job{},
		taskScheduler: newScheduler(newMetrics(tally.NoopScope)),
		running:       true,
		stopChan:      make(chan struct{}),
	}

	jobID := &peloton.JobID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c"}

	m.SetTask(jobID, 0, &pb_task.RuntimeInfo{})
	m.SetTask(jobID, 1, &pb_task.RuntimeInfo{})
	m.SetTask(jobID, 2, &pb_task.RuntimeInfo{})

	j := m.addJob(jobID, nil)
	assert.Len(t, j.tasks, 3)
	assert.Len(t, *m.taskScheduler.queue.pq, 3)

	m.Stop()
	assert.Nil(t, m.GetJob(jobID))
	assert.Len(t, *m.taskScheduler.queue.pq, 0)

	m.SetTask(jobID, 0, &pb_task.RuntimeInfo{})
	assert.Nil(t, m.GetJob(jobID))
	assert.Len(t, *m.taskScheduler.queue.pq, 0)
}

func TestManagerStartStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobstoreMock := store_mocks.NewMockJobStore(ctrl)

	m := &manager{
		jobs:          map[string]*job{},
		jobStore:      jobstoreMock,
		taskScheduler: newScheduler(newMetrics(tally.NoopScope)),
	}

	var wg sync.WaitGroup
	wg.Add(1)

	jobstoreMock.EXPECT().GetAllJobs(gomock.Any()).Return(nil, nil).Do(func(_ interface{}) error {
		wg.Done()
		return nil
	})

	m.Start()

	m.Stop()

	wg.Wait()
}

func TestPublishMetrics(t *testing.T) {
	m := &manager{
		jobs:          map[string]*job{},
		mtx:           newMetrics(tally.NoopScope),
		taskScheduler: newScheduler(newMetrics(tally.NoopScope)),
		running:       true,
	}

	jobID := &peloton.JobID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c"}
	m.SetTask(jobID, 0, &pb_task.RuntimeInfo{})

	m.publishMetrics()
}

func TestManagerRunTaskAction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)

	m := &manager{
		jobs:          map[string]*job{},
		mtx:           newMetrics(tally.NoopScope),
		taskScheduler: newScheduler(newMetrics(tally.NoopScope)),
		jobStore:      mockJobStore,
		taskStore:     mockTaskStore,
		running:       true,
	}

	jobID := &peloton.JobID{
		Value: "my-job-id",
	}
	j := m.addJob(jobID, nil)
	m.SetTask(jobID, 0, &pb_task.RuntimeInfo{})
	tt := j.tasks[0]
	jobConfig := &pb_job.JobConfig{
		RespoolID: &peloton.ResourcePoolID{
			Value: "my-respool-id",
		},
	}

	mockJobStore.EXPECT().GetJobConfig(context.Background(), jobID, uint64(0)).AnyTimes().Return(jobConfig, nil)

	before := time.Now()

	assert.NoError(t, m.RunTaskAction(context.Background(), jobID, uint32(0), NoAction))

	la, lt := tt.LastAction()
	assert.Equal(t, NoAction, la)
	assert.True(t, lt.After(before))

	mockTaskStore.EXPECT().GetTaskRuntime(context.Background(), gomock.Any(), gomock.Any()).Return(&pb_task.RuntimeInfo{
		ConfigVersion: 0,
	}, nil)
	assert.NoError(t, m.RunTaskAction(context.Background(), jobID, uint32(0), ReloadRuntime))
}

func TestUpdateTaskRuntimeWithError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskstoreMock := store_mocks.NewMockTaskStore(ctrl)

	m := &manager{
		jobs:          map[string]*job{},
		taskStore:     taskstoreMock,
		taskScheduler: newScheduler(newMetrics(tally.NoopScope)),
		running:       true,
	}

	jobID := &peloton.JobID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c"}

	m.SetTask(jobID, 0, &pb_task.RuntimeInfo{})
	assert.NotNil(t, m.jobs[jobID.Value].tasks[0].runtime)
	assert.NotNil(t, m.WaitForScheduledTask(nil))

	taskstoreMock.EXPECT().
		UpdateTaskRuntime(context.Background(), jobID, uint32(0), &pb_task.RuntimeInfo{}).
		Return(errors.New("some error"))

	err := m.UpdateTaskRuntime(context.Background(), jobID, 0, &pb_task.RuntimeInfo{})
	assert.EqualError(t, err, "some error")

	assert.Nil(t, m.jobs[jobID.Value].tasks[0].runtime)
	assert.NotNil(t, m.WaitForScheduledTask(nil))
}
