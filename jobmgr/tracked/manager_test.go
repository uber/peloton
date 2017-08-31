package tracked

import (
	"sync"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
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
