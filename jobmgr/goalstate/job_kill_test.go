package goalstate

import (
	"context"
	"testing"

	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	goalstatemocks "code.uber.internal/infra/peloton/common/goalstate/mocks"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	storemocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type JobKillTestSuite struct {
	suite.Suite

	ctrl                *gomock.Controller
	jobStore            *storemocks.MockJobStore
	jobGoalStateEngine  *goalstatemocks.MockEngine
	taskGoalStateEngine *goalstatemocks.MockEngine
	jobFactory          *cachedmocks.MockJobFactory
	cachedJob           *cachedmocks.MockJob
	cachedConfig        *cachedmocks.MockJobConfig
	goalStateDriver     *driver
	jobID               *peloton.JobID
	jobEnt              *jobEntity
}

func TestJobKill(t *testing.T) {
	suite.Run(t, new(JobKillTestSuite))
}

func (suite *JobKillTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.jobStore = storemocks.NewMockJobStore(suite.ctrl)
	suite.jobGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.taskGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.cachedConfig = cachedmocks.NewMockJobConfig(suite.ctrl)

	suite.goalStateDriver = &driver{
		jobEngine:  suite.jobGoalStateEngine,
		taskEngine: suite.taskGoalStateEngine,
		jobStore:   suite.jobStore,
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

func (suite *JobKillTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestJobKill tests killing a fully created job
func (suite JobKillTestSuite) TestJobKill() {
	instanceCount := uint32(4)
	cachedTasks := make(map[uint32]cached.Task)
	mockTasks := make(map[uint32]*cachedmocks.MockTask)
	for i := uint32(0); i < instanceCount; i++ {
		cachedTask := cachedmocks.NewMockTask(suite.ctrl)
		mockTasks[i] = cachedTask
		cachedTasks[i] = cachedTask
	}

	runtimes := make(map[uint32]*pbtask.RuntimeInfo)
	runtimes[0] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_RUNNING,
		GoalState: pbtask.TaskState_SUCCEEDED,
	}
	runtimes[1] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_RUNNING,
		GoalState: pbtask.TaskState_SUCCEEDED,
	}
	runtimes[2] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_INITIALIZED,
		GoalState: pbtask.TaskState_SUCCEEDED,
	}
	runtimes[3] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_INITIALIZED,
		GoalState: pbtask.TaskState_SUCCEEDED,
	}

	runtimeDiffs := make(map[uint32]map[string]interface{})
	runtimeDiffs[0] = map[string]interface{}{
		cached.GoalStateField: pbtask.TaskState_KILLED,
		cached.MessageField:   "Task stop API request",
		cached.ReasonField:    "",
	}
	runtimeDiffs[1] = map[string]interface{}{
		cached.GoalStateField: pbtask.TaskState_KILLED,
		cached.MessageField:   "Task stop API request",
		cached.ReasonField:    "",
	}
	runtimeDiffs[2] = map[string]interface{}{
		cached.GoalStateField: pbtask.TaskState_KILLED,
		cached.MessageField:   "Task stop API request",
		cached.ReasonField:    "",
	}
	runtimeDiffs[3] = map[string]interface{}{
		cached.GoalStateField: pbtask.TaskState_KILLED,
		cached.MessageField:   "Task stop API request",
		cached.ReasonField:    "",
	}

	jobRuntime := &pbjob.RuntimeInfo{
		State:     pbjob.JobState_RUNNING,
		GoalState: pbjob.JobState_SUCCEEDED,
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedJob.EXPECT().
		GetAllTasks().
		Return(cachedTasks)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	suite.cachedJob.EXPECT().
		Update(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Do(func(_ context.Context, jobInfo *pbjob.JobInfo, _ cached.UpdateRequest) {
			suite.Equal(jobInfo.Runtime.State, pbjob.JobState_KILLING)
		}).
		Return(nil)

	for i := uint32(0); i < instanceCount; i++ {
		mockTasks[i].EXPECT().
			GetRunTime(gomock.Any()).
			Return(runtimes[i], nil)
	}

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), runtimeDiffs).
		Return(nil)

	suite.cachedJob.EXPECT().
		GetJobType().
		Return(pbjob.JobType_BATCH)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(suite.cachedConfig, nil)

	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return().
		Times(int(instanceCount))

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := JobKill(context.Background(), suite.jobEnt)
	suite.NoError(err)
}

// TestJobKillPartiallyCreatedJob tests killing partially created jobs
func (suite JobKillTestSuite) TestJobKillPartiallyCreatedJob() {
	cachedTasks := make(map[uint32]cached.Task)
	mockTasks := make(map[uint32]*cachedmocks.MockTask)
	for i := uint32(2); i < 4; i++ {
		cachedTask := cachedmocks.NewMockTask(suite.ctrl)
		mockTasks[i] = cachedTask
		cachedTasks[i] = cachedTask
	}

	runtimes := make(map[uint32]*pbtask.RuntimeInfo)
	runtimes[2] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_SUCCEEDED,
		GoalState: pbtask.TaskState_KILLED,
	}
	runtimes[3] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_KILLED,
		GoalState: pbtask.TaskState_KILLED,
	}
	jobRuntime := &pbjob.RuntimeInfo{
		State:     pbjob.JobState_INITIALIZED,
		GoalState: pbjob.JobState_KILLED,
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).AnyTimes()

	suite.cachedJob.EXPECT().
		GetAllTasks().
		Return(cachedTasks)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	suite.cachedJob.EXPECT().
		Update(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Do(func(_ context.Context, jobInfo *pbjob.JobInfo, _ cached.UpdateRequest) {
			suite.Equal(jobInfo.Runtime.State, pbjob.JobState_KILLED)
		}).
		Return(nil)

	for i := uint32(2); i < 4; i++ {
		mockTasks[i].EXPECT().
			GetRunTime(gomock.Any()).
			Return(runtimes[i], nil).Times(2)
	}

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any()).
		Return(nil)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(suite.cachedConfig, nil)

	suite.cachedJob.EXPECT().
		IsPartiallyCreated(gomock.Any()).
		Return(true)

	err := JobKill(context.Background(), suite.jobEnt)
	suite.NoError(err)

	runtimes[2] = &pbtask.RuntimeInfo{
		State:     pbtask.TaskState_RUNNING,
		GoalState: pbtask.TaskState_KILLED,
	}
	jobRuntime.State = pbjob.JobState_INITIALIZED

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).AnyTimes()

	suite.cachedJob.EXPECT().
		GetAllTasks().
		Return(cachedTasks)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(suite.cachedConfig, nil)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	suite.cachedJob.EXPECT().
		Update(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Do(func(_ context.Context, jobInfo *pbjob.JobInfo, _ cached.UpdateRequest) {
			suite.Equal(jobInfo.Runtime.State, pbjob.JobState_KILLING)
		}).
		Return(nil)

	for i := uint32(2); i < 4; i++ {
		mockTasks[i].EXPECT().
			GetRunTime(gomock.Any()).
			Return(runtimes[i], nil).AnyTimes()
	}

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any()).
		Return(nil)

	suite.cachedJob.EXPECT().
		IsPartiallyCreated(gomock.Any()).
		Return(true)

	err = JobKill(context.Background(), suite.jobEnt)
	suite.NoError(err)
}
