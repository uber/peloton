package goalstate

import (
	"context"
	"fmt"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"

	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestJobStateAndGoalState(t *testing.T) {
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

	runtime := &job.RuntimeInfo{
		State:     job.JobState_RUNNING,
		GoalState: job.JobState_SUCCEEDED,
	}

	// Test fetching the entity ID
	assert.Equal(t, jobID.GetValue(), jobEnt.GetID())

	// Test fetching the entity state
	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		GetRuntime(context.Background()).Return(runtime, nil)

	actState := jobEnt.GetState()
	assert.Equal(t, runtime.State, actState.(job.JobState))

	// Test fetching the entity goal state
	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		GetRuntime(context.Background()).Return(runtime, nil)

	actGoalState := jobEnt.GetGoalState()
	assert.Equal(t, runtime.State, actState.(job.JobState))

	// Fetching job runtime gives an error
	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		GetRuntime(context.Background()).Return(nil, fmt.Errorf("fake error"))

	actState = jobEnt.GetState()
	assert.Equal(t, job.JobState_UNKNOWN, actState.(job.JobState))

	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		GetRuntime(context.Background()).Return(nil, fmt.Errorf("fake error"))

	actGoalState = jobEnt.GetGoalState()
	assert.Equal(t, job.JobState_UNKNOWN, actGoalState.(job.JobState))
}

func TestJobGetActionList(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}

	jobEnt := &jobEntity{
		id: jobID,
	}
	_, _, actions := jobEnt.GetActionList(job.JobState_UNKNOWN, job.JobState_UNKNOWN)
	assert.Equal(t, 1, len(actions))

	_, _, actions = jobEnt.GetActionList(job.JobState_SUCCEEDED, job.JobState_SUCCEEDED)
	assert.Equal(t, 1, len(actions))

	_, _, actions = jobEnt.GetActionList(job.JobState_RUNNING, job.JobState_SUCCEEDED)
	assert.Equal(t, 2, len(actions))

	_, _, actions = jobEnt.GetActionList(job.JobState_RUNNING, job.JobState_KILLED)
	assert.Equal(t, 3, len(actions))
}

func TestEngineJobSuggestAction(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	jobEnt := &jobEntity{
		id: jobID,
	}

	a := jobEnt.suggestJobAction(job.JobState_INITIALIZED, job.JobState_SUCCEEDED)
	assert.Equal(t, CreateTasksAction, a)

	a = jobEnt.suggestJobAction(job.JobState_SUCCEEDED, job.JobState_SUCCEEDED)
	assert.Equal(t, UntrackAction, a)

	a = jobEnt.suggestJobAction(job.JobState_RUNNING, job.JobState_KILLED)
	assert.Equal(t, KillAction, a)

	a = jobEnt.suggestJobAction(job.JobState_RUNNING, job.JobState_SUCCEEDED)
	assert.Equal(t, NoJobAction, a)

	a = jobEnt.suggestJobAction(job.JobState_KILLING, job.JobState_SUCCEEDED)
	assert.Equal(t, JobStateInvalidAction, a)

	a = jobEnt.suggestJobAction(job.JobState_KILLING, job.JobState_FAILED)
	assert.Equal(t, JobStateInvalidAction, a)
}
