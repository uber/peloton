package goalstate

import (
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"

	"code.uber.internal/infra/peloton/jobmgr/tracked"
	"code.uber.internal/infra/peloton/jobmgr/tracked/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestEngineSuggestActionJobGoalState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	e := NewEngine(Config{}, nil, tally.NoopScope).(*engine)

	jobMock := mocks.NewMockJob(ctrl)

	jobRuntime := &job.RuntimeInfo{
		State:     job.JobState_INITIALIZED,
		GoalState: job.JobState_SUCCEEDED,
	}
	jobMock.EXPECT().GetJobRuntime(gomock.Any()).Return(jobRuntime, nil)
	action, err := e.suggestJobAction(jobMock)
	assert.NoError(t, err)
	assert.Equal(t, tracked.JobCreateTasks, action)

	jobRuntime.State = job.JobState_RUNNING
	jobMock.EXPECT().GetJobRuntime(gomock.Any()).Return(jobRuntime, nil)
	action, err = e.suggestJobAction(jobMock)
	assert.NoError(t, err)
	assert.Equal(t, tracked.JobNoAction, action)
}

func TestEngineProcessJob(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobMock := mocks.NewMockJob(ctrl)
	managerMock := mocks.NewMockManager(ctrl)

	e := &engine{
		trackedManager: managerMock,
	}
	e.cfg.normalize()

	jobRuntime := &job.RuntimeInfo{
		State:     job.JobState_INITIALIZED,
		GoalState: job.JobState_SUCCEEDED,
	}

	jobMock.EXPECT().GetJobRuntime(gomock.Any()).Return(jobRuntime, nil)
	jobMock.EXPECT().RunAction(gomock.Any(), tracked.JobCreateTasks).Return(false, nil)
	jobMock.EXPECT().ClearJobRuntime()
	jobMock.EXPECT().SetLastDelay(gomock.Any()).Return()
	managerMock.EXPECT().ScheduleJob(jobMock, gomock.Any())
	e.processJob(jobMock)
}
