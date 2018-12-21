package goalstate

import (
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/jobmgr/cached"

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
		CurrentState().
		Return(cached.JobStateVector{State: runtime.State})
	actState := jobEnt.GetState()
	assert.Equal(t, runtime.State, actState.(cached.JobStateVector).State)

	// Test fetching the entity goal state
	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		GoalState().
		Return(cached.JobStateVector{State: runtime.GoalState})
	actGoalState := jobEnt.GetGoalState()
	assert.Equal(t, runtime.GoalState, actGoalState.(cached.JobStateVector).State)

}

func TestJobGetActionList(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}

	jobEnt := &jobEntity{
		id: jobID,
	}
	_, _, actions := jobEnt.GetActionList(
		cached.JobStateVector{State: job.JobState_UNKNOWN},
		cached.JobStateVector{State: job.JobState_UNKNOWN},
	)
	assert.Equal(t, 1, len(actions))

	_, _, actions = jobEnt.GetActionList(
		cached.JobStateVector{State: job.JobState_SUCCEEDED},
		cached.JobStateVector{State: job.JobState_SUCCEEDED},
	)
	assert.Equal(t, 1, len(actions))

	_, _, actions = jobEnt.GetActionList(
		cached.JobStateVector{State: job.JobState_UNINITIALIZED},
		cached.JobStateVector{State: job.JobState_SUCCEEDED},
	)
	assert.Equal(t, 1, len(actions))

	_, _, actions = jobEnt.GetActionList(
		cached.JobStateVector{State: job.JobState_RUNNING},
		cached.JobStateVector{State: job.JobState_SUCCEEDED},
	)
	assert.Equal(t, 3, len(actions))

	_, _, actions = jobEnt.GetActionList(
		cached.JobStateVector{State: job.JobState_RUNNING},
		cached.JobStateVector{State: job.JobState_KILLED},
	)
	assert.Equal(t, 4, len(actions))

	_, _, actions = jobEnt.GetActionList(
		cached.JobStateVector{State: job.JobState_RUNNING, StateVersion: 0},
		cached.JobStateVector{State: job.JobState_KILLED, StateVersion: 1},
	)
	assert.Equal(t, 4, len(actions))
}

func TestEngineJobSuggestAction(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	jobEnt := &jobEntity{
		id: jobID,
	}

	a := jobEnt.suggestJobAction(
		cached.JobStateVector{State: job.JobState_INITIALIZED},
		cached.JobStateVector{State: job.JobState_SUCCEEDED},
	)
	assert.Equal(t, CreateTasksAction, a)

	a = jobEnt.suggestJobAction(
		cached.JobStateVector{State: job.JobState_SUCCEEDED},
		cached.JobStateVector{State: job.JobState_SUCCEEDED},
	)
	assert.Equal(t, UntrackAction, a)

	a = jobEnt.suggestJobAction(
		cached.JobStateVector{State: job.JobState_RUNNING},
		cached.JobStateVector{State: job.JobState_KILLED},
	)
	assert.Equal(t, KillAction, a)

	a = jobEnt.suggestJobAction(
		cached.JobStateVector{State: job.JobState_RUNNING, StateVersion: 0},
		cached.JobStateVector{State: job.JobState_KILLED, StateVersion: 1},
	)
	assert.Equal(t, KillAction, a)

	a = jobEnt.suggestJobAction(
		cached.JobStateVector{State: job.JobState_RUNNING},
		cached.JobStateVector{State: job.JobState_SUCCEEDED},
	)
	assert.Equal(t, NoJobAction, a)

	a = jobEnt.suggestJobAction(
		cached.JobStateVector{State: job.JobState_KILLING},
		cached.JobStateVector{State: job.JobState_SUCCEEDED},
	)
	assert.Equal(t, JobStateInvalidAction, a)

	a = jobEnt.suggestJobAction(
		cached.JobStateVector{State: job.JobState_KILLING},
		cached.JobStateVector{State: job.JobState_FAILED},
	)
	assert.Equal(t, JobStateInvalidAction, a)

	a = jobEnt.suggestJobAction(
		cached.JobStateVector{State: job.JobState_UNINITIALIZED},
		cached.JobStateVector{State: job.JobState_SUCCEEDED},
	)
	assert.Equal(t, RecoverAction, a)

	a = jobEnt.suggestJobAction(
		cached.JobStateVector{State: job.JobState_KILLED, StateVersion: 0},
		cached.JobStateVector{State: job.JobState_KILLED, StateVersion: 0},
	)
	assert.Equal(t, UntrackAction, a)
}
