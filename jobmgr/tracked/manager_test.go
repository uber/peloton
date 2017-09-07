package tracked

import (
	"context"
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

	j := m.addJob(jobID)
	assert.NotNil(t, j)

	assert.Equal(t, j, m.GetJob(jobID))
	assert.Equal(t, j, m.addJob(jobID))
}

func TestManagerScheduleAndDequeueTasks(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	mtx := newMetrics(tally.NoopScope)
	m := &manager{
		jobs:             map[string]*job{},
		taskQueue:        newDeadlineQueue(mtx),
		taskQueueChanged: make(chan struct{}, 1),
		running:          true,
	}

	j := m.addJob(jobID)

	c := 100
	var wg sync.WaitGroup
	wg.Add(c)

	for i := 0; i < c; i++ {
		go func() {
			tt := m.WaitForScheduledTask(nil)
			assert.NotNil(t, tt)
			wg.Done()
		}()
	}

	go func() {
		for i := 0; i < c; i++ {
			m.SetTask(jobID, uint32(i), nil)
			m.ScheduleTask(j.GetTask(uint32(i)), time.Now())
		}
	}()

	wg.Wait()
}

func TestManagerClearTask(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}

	m := &manager{
		jobs:             map[string]*job{},
		taskQueue:        newDeadlineQueue(newMetrics(tally.NoopScope)),
		taskQueueChanged: make(chan struct{}, 1),
		running:          true,
	}

	j := m.addJob(jobID)
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
		jobs:             map[string]*job{},
		jobStore:         jobstoreMock,
		taskStore:        taskstoreMock,
		taskQueue:        newDeadlineQueue(newMetrics(tally.NoopScope)),
		taskQueueChanged: make(chan struct{}, 1),
		running:          true,
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
					GoalState:            pb_task.TaskState_RUNNING,
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
	m := &manager{
		jobs:             map[string]*job{},
		taskQueue:        newDeadlineQueue(newMetrics(tally.NoopScope)),
		taskQueueChanged: make(chan struct{}, 1),
		running:          true,
	}

	jobID := &peloton.JobID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c"}
	m.SetTask(jobID, 0, &pb_task.RuntimeInfo{})
	m.SetTask(jobID, 1, &pb_task.RuntimeInfo{})
	m.SetTask(jobID, 2, &pb_task.RuntimeInfo{})

	assert.Len(t, m.addJob(jobID).tasks, 3)
	assert.Len(t, *m.taskQueue.pq, 3)

	m.Stop()
	assert.Nil(t, m.GetJob(jobID))
	assert.Len(t, *m.taskQueue.pq, 0)

	m.SetTask(jobID, 0, &pb_task.RuntimeInfo{})
	assert.Nil(t, m.GetJob(jobID))
	assert.Len(t, *m.taskQueue.pq, 0)
}

func TestManagerStartStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobstoreMock := store_mocks.NewMockJobStore(ctrl)

	m := &manager{
		jobs:             map[string]*job{},
		jobStore:         jobstoreMock,
		taskQueue:        newDeadlineQueue(newMetrics(tally.NoopScope)),
		taskQueueChanged: make(chan struct{}, 1),
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
