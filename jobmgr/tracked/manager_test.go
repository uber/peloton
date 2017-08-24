package tracked

import (
	"sync"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
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

	m := &manager{
		jobs:             map[string]*job{},
		taskQueue:        newDeadlineQueue(),
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
