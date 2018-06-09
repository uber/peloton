package goalstate

import (
	"context"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"

	goalstatemocks "code.uber.internal/infra/peloton/common/goalstate/mocks"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestJobEnqueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)

	goalStateDriver := &driver{
		jobEngine: jobGoalStateEngine,
		mtx:       NewMetrics(tally.NoopScope),
		cfg:       &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}

	jobEnt := &jobEntity{
		id:     jobID,
		driver: goalStateDriver,
	}

	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := JobEnqueue(context.Background(), jobEnt)
	assert.NoError(t, err)
}

func TestUntrackJob(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)

	goalStateDriver := &driver{
		jobEngine:  jobGoalStateEngine,
		taskEngine: taskGoalStateEngine,
		jobFactory: jobFactory,
		mtx:        NewMetrics(tally.NoopScope),
		cfg:        &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}

	jobEnt := &jobEntity{
		id:     jobID,
		driver: goalStateDriver,
	}

	taskMap := make(map[uint32]cached.Task)
	taskMap[0] = cachedTask

	jobFactory.EXPECT().
		GetJob(jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		GetAllTasks().
		Return(taskMap)

	taskGoalStateEngine.EXPECT().
		Delete(gomock.Any()).
		Return()

	jobGoalStateEngine.EXPECT().
		Delete(gomock.Any()).
		Return()

	jobFactory.EXPECT().
		ClearJob(jobID).Return()

	err := JobUntrack(context.Background(), jobEnt)
	assert.NoError(t, err)
}

// Test JobStateInvalid workflow is as expected
func TestJobStateInvalidAction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)

	goalStateDriver := &driver{
		jobFactory: jobFactory,
		mtx:        NewMetrics(tally.NoopScope),
		cfg:        &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}

	jobEnt := &jobEntity{
		id:     jobID,
		driver: goalStateDriver,
	}

	jobFactory.EXPECT().
		GetJob(jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		GetRuntime(context.Background()).
		Return(&job.RuntimeInfo{
			State:     job.JobState_KILLING,
			GoalState: job.JobState_RUNNING,
		}, nil)

	err := JobStateInvalid(context.Background(), jobEnt)
	assert.NoError(t, err)
}
