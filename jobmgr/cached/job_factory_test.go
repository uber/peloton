package cached

import (
	"context"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestInitJobFactory(t *testing.T) {
	InitJobFactory(nil, nil, nil, tally.NoopScope)
	f := GetJobFactory()
	assert.NotNil(t, f)
}

func TestAddAndGetAndClearJob(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}

	f := &jobFactory{
		jobs:    map[string]*job{},
		running: true,
	}

	assert.Nil(t, f.GetJob(jobID))

	j := f.AddJob(jobID)
	assert.NotNil(t, j)

	assert.Equal(t, j, f.GetJob(jobID))
	assert.Equal(t, j, f.AddJob(jobID))
	assert.Equal(t, 1, len(f.GetAllJobs()))

	f.ClearJob(jobID)
	assert.Equal(t, 0, len(f.GetAllJobs()))
	assert.Nil(t, f.GetJob(jobID))
}

func TestStartStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	f := &jobFactory{
		jobs: map[string]*job{},
		mtx:  NewMetrics(tally.NoopScope),
	}

	f.Start()

	assert.True(t, f.running)
	jobID := &peloton.JobID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c"}
	runtimes := make(map[uint32]*pbtask.RuntimeInfo)
	runtimes[0] = &pbtask.RuntimeInfo{}
	runtimes[1] = &pbtask.RuntimeInfo{}
	runtimes[2] = &pbtask.RuntimeInfo{}

	j := f.AddJob(jobID)
	j.UpdateTasks(context.Background(), runtimes, UpdateCacheOnly)
	assert.Equal(t, 3, len(j.GetAllTasks()))

	f.Stop()
	assert.Nil(t, f.GetJob(jobID))
}

func TestPublishMetrics(t *testing.T) {
	f := &jobFactory{
		jobs:    map[string]*job{},
		mtx:     NewMetrics(tally.NoopScope),
		running: true,
	}

	jobID := &peloton.JobID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c"}
	runtimes := make(map[uint32]*pbtask.RuntimeInfo)
	runtimes[0] = &pbtask.RuntimeInfo{}
	j := f.AddJob(jobID)
	j.UpdateTasks(context.Background(), runtimes, UpdateCacheOnly)

	f.publishMetrics()
}
