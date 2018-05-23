package deadline

import (
	"testing"
	"time"

	peloton_job "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	peloton_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"

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
		stopChan: make(chan struct{}),
		metrics:  NewMetrics(tally.NoopScope),
	}
}

func (suite *DeadlineTrackerTestSuite) TearDownSuite() {
	suite.mockCtrl.Finish()
}

func (suite *DeadlineTrackerTestSuite) TestDeadlineTrackingCycle() {
	jobID := &peloton.JobID{Value: "test-deadline"}
	jobConfig := &peloton_job.JobConfig{
		Sla: &peloton_job.SlaConfig{
			MaxRunningTime: 5,
		},
	}

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
	jobs := make(map[string]cached.Job)
	jobs[jobID.Value] = job
	tasks := make(map[uint32]cached.Task)
	tasks[1] = task

	suite.jobFactory.EXPECT().GetAllJobs().Return(jobs)
	suite.mockJobStore.EXPECT().GetJobConfig(gomock.Any(), gomock.Any()).Return(jobConfig, nil)
	job.EXPECT().GetAllTasks().Return(tasks)
	task.EXPECT().GetRunTime().Return(taskInfo.Runtime).Times(2)
	suite.mockTaskStore.EXPECT().GetTaskByID(gomock.Any(), gomock.Any()).Return(taskInfo, nil)
	suite.jobFactory.EXPECT().AddJob(gomock.Any()).Return(job)
	job.EXPECT().UpdateTasks(gomock.Any(), runtimes, cached.UpdateCacheAndDB).Return(nil)
	suite.goalStateDriver.EXPECT().EnqueueTask(gomock.Any(), gomock.Any(), gomock.Any()).Return()
	job.EXPECT().
		GetJobType().Return(peloton_job.JobType_BATCH)
	suite.goalStateDriver.EXPECT().
		GetJobRuntimeDuration(peloton_job.JobType_BATCH).
		Return(1 * time.Second)
	suite.goalStateDriver.EXPECT().EnqueueJob(gomock.Any(), gomock.Any())

	err := suite.tracker.trackDeadline()

	suite.NoError(err)
	suite.Equal(peloton_task.TaskState_KILLED, taskInfo.GetRuntime().GoalState)
}

func (suite *DeadlineTrackerTestSuite) TestTracker_StartStop() {
	defer func() {
		suite.tracker.Stop()
		suite.False(suite.tracker.isRunning())
	}()
	err := suite.tracker.Start()
	suite.NoError(err)
	suite.True(suite.tracker.isRunning())
}

func TestDeadlineTracker(t *testing.T) {
	suite.Run(t, new(DeadlineTrackerTestSuite))
}
