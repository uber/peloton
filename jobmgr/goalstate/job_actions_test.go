package goalstate

import (
	"context"
	"fmt"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"

	"code.uber.internal/infra/peloton/jobmgr/cached"
	jobmgrcommon "code.uber.internal/infra/peloton/jobmgr/common"

	goalstatemocks "code.uber.internal/infra/peloton/common/goalstate/mocks"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	storemocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

type JobActionsTestSuite struct {
	suite.Suite

	ctrl                *gomock.Controller
	jobGoalStateEngine  *goalstatemocks.MockEngine
	taskGoalStateEngine *goalstatemocks.MockEngine
	jobFactory          *cachedmocks.MockJobFactory
	goalStateDriver     *driver
	jobID               *peloton.JobID
	jobEnt              *jobEntity
}

func (suite *JobActionsTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())

	suite.jobGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.taskGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)

	suite.goalStateDriver = &driver{
		jobEngine:  suite.jobGoalStateEngine,
		taskEngine: suite.taskGoalStateEngine,
		jobFactory: suite.jobFactory,
		mtx:        NewMetrics(tally.NoopScope),
		cfg:        &Config{},
	}
	suite.goalStateDriver.cfg.normalize()

	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.jobEnt = &jobEntity{
		id:     suite.jobID,
		driver: suite.goalStateDriver,
	}
}

func (suite *JobActionsTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func (suite *JobActionsTestSuite) TestJobEnqueue() {
	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := JobEnqueue(context.Background(), suite.jobEnt)
	suite.NoError(err)
}

func (suite *JobActionsTestSuite) TestUntrackJobBatch() {
	cachedJob := cachedmocks.NewMockJob(suite.ctrl)
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
	taskMap := make(map[uint32]cached.Task)
	taskMap[0] = cachedTask

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&job.JobConfig{
			Type: job.JobType_BATCH,
		}, nil)

	cachedJob.EXPECT().
		GetAllTasks().
		Return(taskMap)

	suite.taskGoalStateEngine.EXPECT().
		Delete(gomock.Any()).
		Return()

	suite.jobGoalStateEngine.EXPECT().
		Delete(gomock.Any()).
		Return()

	suite.jobFactory.EXPECT().
		ClearJob(suite.jobID).Return()

	err := JobUntrack(context.Background(), suite.jobEnt)
	suite.NoError(err)
}

func (suite *JobActionsTestSuite) TestUntrackJobStateless() {
	cachedJob := cachedmocks.NewMockJob(suite.ctrl)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&job.JobConfig{
			Type: job.JobType_SERVICE,
		}, nil)

	err := JobUntrack(context.Background(), suite.jobEnt)
	suite.NoError(err)
}

// Test JobStateInvalid workflow is as expected
func (suite *JobActionsTestSuite) TestJobStateInvalidAction() {
	cachedJob := cachedmocks.NewMockJob(suite.ctrl)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		GetRuntime(context.Background()).
		Return(&job.RuntimeInfo{
			State:     job.JobState_KILLING,
			GoalState: job.JobState_RUNNING,
		}, nil)

	err := JobStateInvalid(context.Background(), suite.jobEnt)
	suite.NoError(err)
}

func (suite *JobActionsTestSuite) TestJobRecoverActionSuccess() {
	cachedJob := cachedmocks.NewMockJob(suite.ctrl)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&job.JobConfig{}, nil)

	cachedJob.EXPECT().
		Update(gomock.Any(), &job.JobInfo{
			Runtime: &job.RuntimeInfo{State: job.JobState_INITIALIZED},
		}, nil, cached.UpdateCacheAndDB).
		Return(nil)

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any())

	err := JobRecover(context.Background(), suite.jobEnt)
	suite.NoError(err)
}

func (suite *JobActionsTestSuite) TestJobRecoverActionFailToRecover() {
	cachedJob := cachedmocks.NewMockJob(suite.ctrl)
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)

	taskMap := make(map[uint32]cached.Task)
	taskMap[0] = cachedTask

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&job.JobConfig{}, yarpcerrors.NotFoundErrorf("config not found")).
		Times(2)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		GetAllTasks().
		Return(taskMap)

	suite.taskGoalStateEngine.EXPECT().
		Delete(gomock.Any()).
		Return()

	suite.jobGoalStateEngine.EXPECT().
		Delete(gomock.Any()).
		Return()

	suite.jobFactory.EXPECT().
		ClearJob(suite.jobID).Return()

	err := JobRecover(context.Background(), suite.jobEnt)
	suite.NoError(err)
}

// TestDeleteJobFromActiveJobs tests DeleteJobFromActiveJobs goalstate
// action results
func (suite *JobActionsTestSuite) TestDeleteJobFromActiveJobs() {
	jobStore := storemocks.NewMockJobStore(suite.ctrl)
	cachedJob := cachedmocks.NewMockJob(suite.ctrl)

	suite.goalStateDriver.jobStore = jobStore

	tt := []struct {
		typ          job.JobType
		state        job.JobState
		shouldDelete bool
	}{
		{
			typ:          job.JobType_BATCH,
			state:        job.JobState_RUNNING,
			shouldDelete: false,
		},

		{
			typ:          job.JobType_SERVICE,
			state:        job.JobState_RUNNING,
			shouldDelete: false,
		},

		{
			typ:          job.JobType_SERVICE,
			state:        job.JobState_FAILED,
			shouldDelete: false,
		},
		{
			typ:          job.JobType_BATCH,
			state:        job.JobState_SUCCEEDED,
			shouldDelete: true,
		},
		{
			typ:          job.JobType_BATCH,
			state:        job.JobState_FAILED,
			shouldDelete: true,
		},
	}
	for _, test := range tt {
		suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(cachedJob)
		cachedJob.EXPECT().GetRuntime(context.Background()).
			Return(&job.RuntimeInfo{
				State: test.state,
			}, nil)
		cachedJob.EXPECT().
			GetConfig(gomock.Any()).
			Return(&job.JobConfig{
				Type: test.typ,
			}, nil)

		// cachedJob.EXPECT().GetJobType().Return(test.typ)
		if test.shouldDelete {
			jobStore.EXPECT().DeleteActiveJob(gomock.Any(), suite.jobID).Return(nil)
		}
		err := DeleteJobFromActiveJobs(context.Background(), suite.jobEnt)
		suite.NoError(err)
	}

}

// TestDeleteJobFromActiveJobsFailures tests failure scenarios for
// DeleteJobFromActiveJobs goalstate action
func (suite *JobActionsTestSuite) TestDeleteJobFromActiveJobsFailures() {
	jobStore := storemocks.NewMockJobStore(suite.ctrl)
	cachedJob := cachedmocks.NewMockJob(suite.ctrl)

	suite.goalStateDriver.jobStore = jobStore

	// set cached job to nil. this should not return error
	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(nil)
	err := DeleteJobFromActiveJobs(context.Background(), suite.jobEnt)
	suite.NoError(err)

	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(cachedJob).AnyTimes()

	// simulate GetRuntime error
	cachedJob.EXPECT().
		GetRuntime(context.Background()).
		Return(nil, fmt.Errorf("runtime error"))
	err = DeleteJobFromActiveJobs(context.Background(), suite.jobEnt)
	suite.Error(err)

	// Simulate GetConfig error
	cachedJob.EXPECT().GetRuntime(context.Background()).
		Return(&job.RuntimeInfo{
			State: job.JobState_FAILED,
		}, nil)
	cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(nil, fmt.Errorf("config error"))
	err = DeleteJobFromActiveJobs(context.Background(), suite.jobEnt)
	suite.Error(err)

	// Simulate storage error
	cachedJob.EXPECT().GetRuntime(context.Background()).
		Return(&job.RuntimeInfo{
			State: job.JobState_FAILED,
		}, nil)
	cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&job.JobConfig{
			Type: job.JobType_BATCH,
		}, nil)
	jobStore.EXPECT().DeleteActiveJob(gomock.Any(), suite.jobID).
		Return(fmt.Errorf("DB error"))
	err = DeleteJobFromActiveJobs(context.Background(), suite.jobEnt)
	suite.Error(err)
}

// TestStartJobSuccess tests the success case of start job action
func (suite *JobActionsTestSuite) TestStartJobSuccess() {
	cachedJob := cachedmocks.NewMockJob(suite.ctrl)
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
	taskMap := make(map[uint32]cached.Task)
	taskMap[0] = cachedTask
	jobRuntime := &job.RuntimeInfo{
		StateVersion: 1,
		State:        job.JobState_RUNNING,
	}

	cachedJob.EXPECT().ID().Return(suite.jobID).AnyTimes()
	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(cachedJob)
	cachedJob.EXPECT().GetAllTasks().Return(taskMap)
	cachedJob.EXPECT().PatchTasks(gomock.Any(), gomock.Any()).Return(nil)
	suite.taskGoalStateEngine.EXPECT().Enqueue(gomock.Any(), gomock.Any())
	cachedJob.EXPECT().GetRuntime(gomock.Any()).Return(jobRuntime, nil)
	cachedJob.EXPECT().
		CompareAndSetRuntime(gomock.Any(), jobRuntime).
		Return(nil, nil)
	suite.jobGoalStateEngine.EXPECT().Enqueue(gomock.Any(), gomock.Any())

	suite.NoError(JobStart(context.Background(), suite.jobEnt))
}

// TestStartJobGetJobFailure tests the failure case of start job action
// due to failure to get job from the job factory.
func (suite *JobActionsTestSuite) TestStartJobGetJobFailure() {
	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(nil)

	suite.NoError(JobStart(context.Background(), suite.jobEnt))
}

// TestStartJobPatchTasksFailure tests the failure case
// of start job action due to error while patching tasks
func (suite *JobActionsTestSuite) TestStartJobPatchTasksFailure() {
	cachedJob := cachedmocks.NewMockJob(suite.ctrl)
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
	taskMap := make(map[uint32]cached.Task)
	taskMap[0] = cachedTask

	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(cachedJob)
	cachedJob.EXPECT().GetAllTasks().Return(taskMap)
	cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any()).
		Return(yarpcerrors.InternalErrorf("test error"))

	suite.Error(JobStart(context.Background(), suite.jobEnt))
}

// TestStartJobGetRuntimeFailure tests the failure case
// of start job action due to error getting job runtime
func (suite *JobActionsTestSuite) TestStartJobGetRuntimeFailure() {
	cachedJob := cachedmocks.NewMockJob(suite.ctrl)
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
	taskMap := make(map[uint32]cached.Task)
	taskMap[0] = cachedTask

	cachedJob.EXPECT().ID().Return(suite.jobID).AnyTimes()
	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(cachedJob)
	cachedJob.EXPECT().GetAllTasks().Return(taskMap)
	cachedJob.EXPECT().PatchTasks(gomock.Any(), gomock.Any()).Return(nil)
	suite.taskGoalStateEngine.EXPECT().Enqueue(gomock.Any(), gomock.Any())
	cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(JobStart(context.Background(), suite.jobEnt))
}

// TestStartJobRuntimeUpdateFailure tests the failure case
// of start job action due to error updating job runtime
func (suite *JobActionsTestSuite) TestStartJobRuntimeUpdateFailure() {
	cachedJob := cachedmocks.NewMockJob(suite.ctrl)
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
	taskMap := make(map[uint32]cached.Task)
	taskMap[0] = cachedTask
	jobRuntime := &job.RuntimeInfo{
		StateVersion: 1,
		State:        job.JobState_RUNNING,
	}

	cachedJob.EXPECT().ID().Return(suite.jobID).AnyTimes()
	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(cachedJob)
	cachedJob.EXPECT().GetAllTasks().Return(taskMap)
	cachedJob.EXPECT().PatchTasks(gomock.Any(), gomock.Any()).Return(nil)
	suite.taskGoalStateEngine.EXPECT().Enqueue(gomock.Any(), gomock.Any())
	cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil).
		Times(jobmgrcommon.MaxConcurrencyErrorRetry)
	cachedJob.EXPECT().
		CompareAndSetRuntime(gomock.Any(), jobRuntime).
		Return(nil, jobmgrcommon.UnexpectedVersionError).
		Times(jobmgrcommon.MaxConcurrencyErrorRetry)

	suite.Error(JobStart(context.Background(), suite.jobEnt))
}

func TestJobActions(t *testing.T) {
	suite.Run(t, new(JobActionsTestSuite))
}
