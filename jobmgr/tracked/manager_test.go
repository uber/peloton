package tracked

import (
	"context"
	"sync"
	"testing"

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

func TestManagerClearTask(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}

	m := &manager{
		jobs:          map[string]*job{},
		taskScheduler: newScheduler(newMetrics(tally.NoopScope)),
		jobScheduler:  newScheduler(newMetrics(tally.NoopScope)),
		running:       true,
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

	assert.Equal(t, 1, len(m.jobs))
	m.WaitForScheduledTask(nil)
	m.clearTask(t0.(*task))
	assert.Equal(t, 1, len(m.jobs))
	assert.Equal(t, 1, len(j.tasks))

	m.WaitForScheduledTask(nil)
	m.clearTask(t1.(*task))
	assert.Equal(t, 0, len(m.jobs))
	assert.Equal(t, 0, len(j.tasks))
}

func TestManagerSetAndClearJob(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}

	jobRuntime := &pb_job.RuntimeInfo{
		State:     pb_job.JobState_RUNNING,
		GoalState: pb_job.JobState_SUCCEEDED,
	}
	jobInfo := &pb_job.JobInfo{
		Runtime: jobRuntime,
	}

	m := &manager{
		jobs:          map[string]*job{},
		taskScheduler: newScheduler(newMetrics(tally.NoopScope)),
		jobScheduler:  newScheduler(newMetrics(tally.NoopScope)),
		running:       true,
	}

	m.SetJob(jobID, jobInfo)
	assert.Equal(t, 1, len(m.jobs))
	m.WaitForScheduledJob(nil)

	m.SetTask(jobID, 0, &pb_task.RuntimeInfo{})
	m.WaitForScheduledTask(nil)
	j0 := m.GetJob(jobID)
	t0 := j0.GetTask(0)
	m.clearTask(t0.(*task))
	assert.Equal(t, 0, len(m.jobs))
}

func TestManagerSyncFromDB(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	id := "3c8a3c3e-71e3-49c5-9aed-2929823f595c"

	jobstoreMock := store_mocks.NewMockJobStore(ctrl)
	taskstoreMock := store_mocks.NewMockTaskStore(ctrl)

	m := &manager{
		jobs:          map[string]*job{},
		jobStore:      jobstoreMock,
		taskStore:     taskstoreMock,
		taskScheduler: newScheduler(newMetrics(tally.NoopScope)),
		jobScheduler:  newScheduler(newMetrics(tally.NoopScope)),
		running:       true,
	}

	jobstoreMock.EXPECT().GetAllJobs(gomock.Any()).Return(map[string]*pb_job.RuntimeInfo{
		id: &pb_job.RuntimeInfo{
			State:     pb_job.JobState_RUNNING,
			GoalState: pb_job.JobState_SUCCEEDED,
		},
	}, nil)

	jobID := &peloton.JobID{Value: id}
	jobstoreMock.EXPECT().GetJobConfig(gomock.Any(), jobID).Return(nil, nil)
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

	job0 := m.GetJob(jobID)
	assert.NotNil(t, job0)
	task := job0.GetTask(1)
	assert.NotNil(t, task)
	assert.Equal(t, job0, m.WaitForScheduledJob(nil))
	assert.Equal(t, task, m.WaitForScheduledTask(nil))
}

func TestManagerStopClearsTasks(t *testing.T) {
	m := &manager{
		jobs:          map[string]*job{},
		taskScheduler: newScheduler(newMetrics(tally.NoopScope)),
		jobScheduler:  newScheduler(newMetrics(tally.NoopScope)),
		running:       true,
		stopChan:      make(chan struct{}),
	}

	jobID := &peloton.JobID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c"}
	m.SetTask(jobID, 0, &pb_task.RuntimeInfo{})
	m.SetTask(jobID, 1, &pb_task.RuntimeInfo{})
	m.SetTask(jobID, 2, &pb_task.RuntimeInfo{})

	assert.Len(t, m.addJob(jobID).tasks, 3)
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
		jobScheduler:  newScheduler(newMetrics(tally.NoopScope)),
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
		jobScheduler:  newScheduler(newMetrics(tally.NoopScope)),
		running:       true,
	}

	jobID := &peloton.JobID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c"}
	m.SetTask(jobID, 0, &pb_task.RuntimeInfo{})

	m.publishMetrics()
}
