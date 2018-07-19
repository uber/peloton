package deadline

import (
	"context"
	"fmt"
	"testing"
	"time"

	peloton_job "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	peloton_task "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	"code.uber.internal/infra/peloton/common/lifecycle"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	goalstatemocks "code.uber.internal/infra/peloton/jobmgr/goalstate/mocks"
	storage_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type DeadlineTrackerTestSuite struct {
	suite.Suite
	mockCtrl        *gomock.Controller
	tracker         *tracker
	mockTaskStore   *storage_mocks.MockTaskStore
	mockJobStore    *storage_mocks.MockJobStore
	jobFactory      *cachedmocks.MockJobFactory
	goalStateDriver *goalstatemocks.MockDriver
}

func (suite *DeadlineTrackerTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockTaskStore = storage_mocks.NewMockTaskStore(suite.mockCtrl)
	suite.mockJobStore = storage_mocks.NewMockJobStore(suite.mockCtrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.mockCtrl)
	suite.goalStateDriver = goalstatemocks.NewMockDriver(suite.mockCtrl)
	suite.tracker = &tracker{
		jobStore:        suite.mockJobStore,
		taskStore:       suite.mockTaskStore,
		jobFactory:      suite.jobFactory,
		goalStateDriver: suite.goalStateDriver,
		config: &Config{
			DeadlineTrackingPeriod: 10 * time.Second,
		},
		metrics:   NewMetrics(tally.NoopScope),
		lifeCycle: lifecycle.NewLifeCycle(),
	}
}

func (suite *DeadlineTrackerTestSuite) TearDownSuite() {
	suite.mockCtrl.Finish()
}

func (suite *DeadlineTrackerTestSuite) TestDeadlineTrackingCycle() {
	jobID := &peloton.JobID{Value: "bca875f5-322a-4439-b0c9-63e3cf9f982e"}

	taskInfo := &peloton_task.TaskInfo{
		InstanceId: 1,
		Runtime: &peloton_task.RuntimeInfo{
			State:     peloton_task.TaskState_RUNNING,
			StartTime: time.Now().AddDate(0, 0, -1).UTC().Format(time.RFC3339Nano),
		},
	}

	runtimes := make(map[uint32]*peloton_task.RuntimeInfo)
	runtimes[1] = taskInfo.Runtime

	job := cachedmocks.NewMockJob(suite.mockCtrl)
	task := cachedmocks.NewMockTask(suite.mockCtrl)
	jobConfig := cachedmocks.NewMockJobConfigCache(suite.mockCtrl)
	jobs := make(map[string]cached.Job)
	jobs[jobID.Value] = job
	tasks := make(map[uint32]cached.Task)
	tasks[1] = task

	jobConfig.EXPECT().GetSLA().
		Return(&peloton_job.SlaConfig{
			MaxRunningTime: 5,
		}).Times(3)
	suite.jobFactory.EXPECT().GetAllJobs().Return(jobs)
	job.EXPECT().GetConfig(gomock.Any()).Return(jobConfig, nil)
	job.EXPECT().GetAllTasks().Return(tasks)
	task.EXPECT().GetRunTime(gomock.Any()).Return(taskInfo.Runtime, nil)
	suite.jobFactory.EXPECT().AddJob(gomock.Any()).Return(job)
	job.EXPECT().PatchTasks(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, runtimeDiffs map[uint32]cached.RuntimeDiff) {
			suite.Equal(peloton_task.TaskState_KILLED, runtimeDiffs[1][cached.GoalStateField])
		}).
		Return(nil)
	suite.goalStateDriver.EXPECT().EnqueueTask(gomock.Any(), gomock.Any(), gomock.Any()).Return()
	job.EXPECT().
		GetJobType().Return(peloton_job.JobType_BATCH)
	suite.goalStateDriver.EXPECT().
		JobRuntimeDuration(peloton_job.JobType_BATCH).
		Return(1 * time.Second)
	suite.goalStateDriver.EXPECT().EnqueueJob(gomock.Any(), gomock.Any())

	suite.tracker.trackDeadline()

	// Test GetConfig error
	suite.jobFactory.EXPECT().GetAllJobs().Return(jobs)
	job.EXPECT().GetConfig(gomock.Any()).Return(nil, fmt.Errorf("Fake GetConfig error"))
	suite.tracker.trackDeadline()

	// Test GetSLA error
	suite.jobFactory.EXPECT().GetAllJobs().Return(jobs)
	job.EXPECT().GetConfig(gomock.Any()).Return(jobConfig, nil)
	jobConfig.EXPECT().GetSLA().Return(nil)
	suite.tracker.trackDeadline()

	// Test GetRunTime error
	jobConfig.EXPECT().GetSLA().
		Return(&peloton_job.SlaConfig{
			MaxRunningTime: 5,
		})
	suite.jobFactory.EXPECT().GetAllJobs().Return(jobs)
	job.EXPECT().GetConfig(gomock.Any()).Return(jobConfig, nil)
	job.EXPECT().GetAllTasks().Return(tasks)
	task.EXPECT().GetRunTime(gomock.Any()).Return(nil, fmt.Errorf("Fake RunTime error"))
	suite.tracker.trackDeadline()

	// Test PatchTasks error
	jobConfig.EXPECT().GetSLA().
		Return(&peloton_job.SlaConfig{
			MaxRunningTime: 5,
		}).Times(3)
	suite.jobFactory.EXPECT().GetAllJobs().Return(jobs)
	job.EXPECT().GetConfig(gomock.Any()).Return(jobConfig, nil)
	job.EXPECT().GetAllTasks().Return(tasks)
	task.EXPECT().GetRunTime(gomock.Any()).Return(taskInfo.Runtime, nil)
	suite.jobFactory.EXPECT().AddJob(gomock.Any()).Return(job)
	job.EXPECT().PatchTasks(gomock.Any(), gomock.Any()).Return(fmt.Errorf("Fake PatchTasks error"))
	suite.tracker.trackDeadline()
}

func (suite *DeadlineTrackerTestSuite) TestTracker_StartStop() {
	defer func() {
		suite.tracker.Stop()
		_, ok := <-suite.tracker.lifeCycle.StopCh()
		suite.False(ok)

		// Stopping tracker again should be no-op
		err := suite.tracker.Stop()
		suite.NoError(err)
	}()
	err := suite.tracker.Start()
	suite.NoError(err)
	suite.NotNil(suite.tracker.lifeCycle.StopCh())

	// Starting tracker again should be no-op
	err = suite.tracker.Start()
	suite.NoError(err)
}

func TestDeadlineTracker(t *testing.T) {
	suite.Run(t, new(DeadlineTrackerTestSuite))
}
