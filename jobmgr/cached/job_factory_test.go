package cached

import (
	"context"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestAddAndGetAndClearJob(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}

	m := &jobFactory{
		jobs:    map[string]*job{},
		running: true,
	}

	assert.Nil(t, m.GetJob(jobID))

	j := m.AddJob(jobID)
	assert.NotNil(t, j)

	assert.Equal(t, j, m.GetJob(jobID))
	assert.Equal(t, j, m.AddJob(jobID))
	assert.Equal(t, 1, len(m.GetAllJobs()))

	m.ClearJob(jobID)
	assert.Equal(t, 0, len(m.GetAllJobs()))
	assert.Nil(t, m.GetJob(jobID))
}

func TestStartStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := &jobFactory{
		jobs: map[string]*job{},
		mtx:  NewMetrics(tally.NoopScope),
	}

	m.Start()

	assert.True(t, m.running)
	jobID := &peloton.JobID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c"}
	runtimes := make(map[uint32]*pb_task.RuntimeInfo)
	runtimes[0] = &pb_task.RuntimeInfo{}
	runtimes[1] = &pb_task.RuntimeInfo{}
	runtimes[2] = &pb_task.RuntimeInfo{}

	j := m.AddJob(jobID)
	j.UpdateTasks(context.Background(), runtimes, UpdateCacheOnly)
	assert.Equal(t, 3, len(j.GetAllTasks()))

	m.Stop()
	assert.Nil(t, m.GetJob(jobID))
}

func TestPublishMetrics(t *testing.T) {
	m := &jobFactory{
		jobs:    map[string]*job{},
		mtx:     NewMetrics(tally.NoopScope),
		running: true,
	}

	jobID := &peloton.JobID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c"}
	runtimes := make(map[uint32]*pb_task.RuntimeInfo)
	runtimes[0] = &pb_task.RuntimeInfo{}
	j := m.AddJob(jobID)
	j.UpdateTasks(context.Background(), runtimes, UpdateCacheOnly)

	m.publishMetrics()
}
