package deadline

import (
	"testing"
	"time"

	peloton_job "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	peloton_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"code.uber.internal/infra/peloton/jobmgr/tracked"
	tracked_mocks "code.uber.internal/infra/peloton/jobmgr/tracked/mocks"
	storage_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type DeadlineTrackerTestSuite struct {
	suite.Suite
	mockCtrl           *gomock.Controller
	tracker            *tracker
	mockTaskStore      *storage_mocks.MockTaskStore
	mockJobStore       *storage_mocks.MockJobStore
	mockTrackedManager *tracked_mocks.MockManager
}

func (suite *DeadlineTrackerTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockTaskStore = storage_mocks.NewMockTaskStore(suite.mockCtrl)
	suite.mockJobStore = storage_mocks.NewMockJobStore(suite.mockCtrl)
	suite.mockTrackedManager = tracked_mocks.NewMockManager(suite.mockCtrl)
	suite.tracker = &tracker{
		jobStore:       suite.mockJobStore,
		taskStore:      suite.mockTaskStore,
		trackedManager: suite.mockTrackedManager,
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
		Runtime: &peloton_task.RuntimeInfo{
			State:     peloton_task.TaskState_RUNNING,
			StartTime: time.Now().AddDate(0, 0, -1).UTC().Format(time.RFC3339Nano),
		},
	}

	job := tracked_mocks.NewMockJob(suite.mockCtrl)

	tasks := make(map[uint32]tracked.Task)
	task := tracked_mocks.NewMockTask(suite.mockCtrl)
	tasks[1] = task
	task.EXPECT().GetRunTime().Return(taskInfo.Runtime).Times(2)
	job.EXPECT().GetAllTasks().Return(tasks)
	jobs := make(map[string]tracked.Job)
	jobs[jobID.Value] = job

	suite.mockJobStore.EXPECT().GetJobConfig(gomock.Any(), gomock.Any()).Return(jobConfig, nil)
	suite.mockTrackedManager.EXPECT().GetAllJobs().Return(jobs)
	suite.mockTaskStore.EXPECT().GetTaskByID(gomock.Any(), gomock.Any()).Return(taskInfo, nil)
	suite.mockTrackedManager.EXPECT().UpdateTaskRuntime(gomock.Any(), gomock.Any(), gomock.Any(), taskInfo.Runtime).Return(nil)

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
