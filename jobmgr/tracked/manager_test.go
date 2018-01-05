package tracked

import (
	"context"
	"fmt"
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
		taskScheduler: newScheduler(NewQueueMetrics(tally.NoopScope)),
		jobScheduler:  newScheduler(NewQueueMetrics(tally.NoopScope)),
		running:       true,
	}

	j := m.addJob(jobID)
	runtimes := make(map[uint32]*pb_task.RuntimeInfo)
	runtimes[0] = &pb_task.RuntimeInfo{}
	runtimes[1] = &pb_task.RuntimeInfo{}
	m.SetTasks(jobID, runtimes, UpdateAndSchedule)
	t0 := j.GetTask(0)
	t1 := j.GetTask(1)
	assert.Equal(t, 1, len(m.jobs))
	assert.Equal(t, 2, len(j.tasks))
	assert.True(t, t0.IsScheduled())
	assert.True(t, t1.IsScheduled())

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
		taskScheduler: newScheduler(NewQueueMetrics(tally.NoopScope)),
		jobScheduler:  newScheduler(NewQueueMetrics(tally.NoopScope)),
		running:       true,
	}

	m.SetJob(jobID, jobInfo)
	assert.Equal(t, 1, len(m.jobs))
	m.WaitForScheduledJob(nil)

	runtimes := make(map[uint32]*pb_task.RuntimeInfo)
	runtimes[0] = &pb_task.RuntimeInfo{}
	m.SetTasks(jobID, runtimes, UpdateAndSchedule)
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
		taskScheduler: newScheduler(NewQueueMetrics(tally.NoopScope)),
		jobScheduler:  newScheduler(NewQueueMetrics(tally.NoopScope)),
		running:       true,
		mtx:           NewMetrics(tally.NoopScope),
	}

	jobID := &peloton.JobID{Value: id}
	var jobIDList []peloton.JobID
	jobIDList = append(jobIDList, peloton.JobID{Value: id})

	jobConfig := &pb_job.JobConfig{
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		InstanceCount: 1,
	}

	jobstoreMock.EXPECT().GetJobsByStates(gomock.Any(), gomock.Any()).Return(jobIDList, nil)

	jobstoreMock.EXPECT().GetJobRuntime(gomock.Any(), jobID).Return(&pb_job.RuntimeInfo{
		State:     pb_job.JobState_RUNNING,
		GoalState: pb_job.JobState_SUCCEEDED,
	}, nil)

	jobstoreMock.EXPECT().GetJobConfig(gomock.Any(), jobID).Return(jobConfig, nil)

	taskstoreMock.EXPECT().GetTaskRuntimesForJobByRange(gomock.Any(), jobID, gomock.Any()).
		Return(map[uint32]*pb_task.RuntimeInfo{
			0: {
				GoalState:            pb_task.TaskState_RUNNING,
				DesiredConfigVersion: 42,
				ConfigVersion:        42,
			},
		}, nil)

	m.syncFromDB(context.Background())

	job0 := m.GetJob(jobID)
	assert.NotNil(t, job0)
	task := job0.GetTask(0)
	assert.NotNil(t, task)
	assert.Equal(t, job0, m.WaitForScheduledJob(nil))
	assert.Equal(t, task, m.WaitForScheduledTask(nil))
}

func TestManagerStopClearsTasks(t *testing.T) {
	m := &manager{
		jobs:          map[string]*job{},
		taskScheduler: newScheduler(NewQueueMetrics(tally.NoopScope)),
		jobScheduler:  newScheduler(NewQueueMetrics(tally.NoopScope)),
		running:       true,
		stopChan:      make(chan struct{}),
		mtx:           NewMetrics(tally.NoopScope),
	}

	jobID := &peloton.JobID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c"}
	runtimes := make(map[uint32]*pb_task.RuntimeInfo)
	runtimes[0] = &pb_task.RuntimeInfo{}
	runtimes[1] = &pb_task.RuntimeInfo{}
	runtimes[2] = &pb_task.RuntimeInfo{}
	m.SetTasks(jobID, runtimes, UpdateAndSchedule)

	assert.Len(t, m.addJob(jobID).tasks, 3)
	assert.Len(t, *m.taskScheduler.queue.pq, 3)

	m.Stop()
	assert.Nil(t, m.GetJob(jobID))
	assert.Len(t, *m.taskScheduler.queue.pq, 0)

	runtimes = make(map[uint32]*pb_task.RuntimeInfo)
	runtimes[0] = &pb_task.RuntimeInfo{}
	m.SetTasks(jobID, runtimes, UpdateAndSchedule)
	assert.Nil(t, m.GetJob(jobID))
	assert.Len(t, *m.taskScheduler.queue.pq, 0)
}

func TestManagerStartStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var jobIDList []peloton.JobID

	jobstoreMock := store_mocks.NewMockJobStore(ctrl)

	m := &manager{
		jobs:          map[string]*job{},
		jobStore:      jobstoreMock,
		taskScheduler: newScheduler(NewQueueMetrics(tally.NoopScope)),
		jobScheduler:  newScheduler(NewQueueMetrics(tally.NoopScope)),
		mtx:           NewMetrics(tally.NoopScope),
	}

	var wg sync.WaitGroup
	wg.Add(1)

	jobstoreMock.EXPECT().GetJobsByStates(gomock.Any(), gomock.Any()).Return(jobIDList, nil).Do(func(_ interface{}, _ interface{}) ([]peloton.JobID, error) {
		wg.Done()
		return jobIDList, nil
	})

	m.Start()

	m.Stop()

	wg.Wait()
}

func TestPublishMetrics(t *testing.T) {
	m := &manager{
		jobs:          map[string]*job{},
		mtx:           NewMetrics(tally.NoopScope),
		taskScheduler: newScheduler(NewQueueMetrics(tally.NoopScope)),
		jobScheduler:  newScheduler(NewQueueMetrics(tally.NoopScope)),
		running:       true,
	}

	jobID := &peloton.JobID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c"}
	runtimes := make(map[uint32]*pb_task.RuntimeInfo)
	runtimes[0] = &pb_task.RuntimeInfo{}
	m.SetTasks(jobID, runtimes, UpdateAndSchedule)

	m.publishMetrics()
}

func TestManagerUpdateTaskRuntimeDBError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobstoreMock := store_mocks.NewMockJobStore(ctrl)
	taskstoreMock := store_mocks.NewMockTaskStore(ctrl)

	m := &manager{
		jobs:          map[string]*job{},
		mtx:           NewMetrics(tally.NoopScope),
		jobStore:      jobstoreMock,
		taskStore:     taskstoreMock,
		taskScheduler: newScheduler(NewQueueMetrics(tally.NoopScope)),
		jobScheduler:  newScheduler(NewQueueMetrics(tally.NoopScope)),
		running:       true,
	}

	jobID := &peloton.JobID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c"}
	runtimes := make(map[uint32]*pb_task.RuntimeInfo)
	runtimes[0] = &pb_task.RuntimeInfo{
		State: pb_task.TaskState_RUNNING,
	}

	taskstoreMock.EXPECT().
		UpdateTaskRuntimes(gomock.Any(), gomock.Any(), gomock.Any()).Return(fmt.Errorf("fake db error"))

	err := m.UpdateTaskRuntimes(context.Background(), jobID, runtimes, UpdateAndSchedule)
	assert.Error(t, err)
	assert.Nil(t, m.jobs[jobID.Value].tasks[0].runtime)
}
