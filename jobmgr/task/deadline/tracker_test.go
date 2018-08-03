package deadline

import (
	"context"
	"errors"
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

	mockJob       *cachedmocks.MockJob
	mockTask      *cachedmocks.MockTask
	mockJobConfig *cachedmocks.MockJobConfigCache
	mockJobs      map[string]cached.Job
	mockTasks     map[uint32]cached.Task
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

	jobID := &peloton.JobID{Value: "bca875f5-322a-4439-b0c9-63e3cf9f982e"}
	suite.mockJob = cachedmocks.NewMockJob(suite.mockCtrl)
	suite.mockTask = cachedmocks.NewMockTask(suite.mockCtrl)
	suite.mockJobConfig = cachedmocks.NewMockJobConfigCache(suite.mockCtrl)
	suite.mockJobs = make(map[string]cached.Job)
	suite.mockJobs[jobID.Value] = suite.mockJob
	suite.mockTasks = make(map[uint32]cached.Task)
	suite.mockTasks[1] = suite.mockTask

}

func (suite *DeadlineTrackerTestSuite) TearDownSuite() {
	suite.mockCtrl.Finish()
}

// TestDeadlineTrackingCycle tests the happy case of trackDeadline
func (suite *DeadlineTrackerTestSuite) TestDeadlineTrackingCycle() {
	taskInfo := &peloton_task.TaskInfo{
		InstanceId: 1,
		Runtime: &peloton_task.RuntimeInfo{
			State:     peloton_task.TaskState_RUNNING,
			StartTime: time.Now().AddDate(0, 0, -1).UTC().Format(time.RFC3339Nano),
		},
	}

	suite.mockJobConfig.EXPECT().GetSLA().
		Return(&peloton_job.SlaConfig{
			MaxRunningTime: 5,
		}).Times(3)
	suite.jobFactory.EXPECT().GetAllJobs().Return(suite.mockJobs)
	suite.mockJob.EXPECT().GetConfig(gomock.Any()).Return(suite.mockJobConfig, nil)
	suite.mockJob.EXPECT().GetAllTasks().Return(suite.mockTasks)
	suite.mockTask.EXPECT().GetRunTime(gomock.Any()).Return(taskInfo.Runtime, nil)
	suite.jobFactory.EXPECT().AddJob(gomock.Any()).Return(suite.mockJob)
	suite.mockJob.EXPECT().PatchTasks(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, runtimeDiffs map[uint32]cached.RuntimeDiff) {
			suite.Equal(peloton_task.TaskState_KILLED, runtimeDiffs[1][cached.GoalStateField])
		}).
		Return(nil)
	suite.goalStateDriver.EXPECT().EnqueueTask(gomock.Any(), gomock.Any(), gomock.Any()).Return()
	suite.mockJob.EXPECT().
		GetJobType().Return(peloton_job.JobType_BATCH)
	suite.goalStateDriver.EXPECT().
		JobRuntimeDuration(peloton_job.JobType_BATCH).
		Return(1 * time.Second)
	suite.goalStateDriver.EXPECT().EnqueueJob(gomock.Any(), gomock.Any())

	suite.tracker.trackDeadline()
}

// TestDeadlineTrackingCycle_GetConfigErr tests trackDeadline when GetConfig has error
func (suite *DeadlineTrackerTestSuite) TestDeadlineTrackingCycle_GetConfigErr() {
	suite.jobFactory.EXPECT().GetAllJobs().Return(suite.mockJobs)
	suite.mockJob.EXPECT().GetConfig(gomock.Any()).Return(nil, fmt.Errorf("Fake GetConfig error"))
	suite.tracker.trackDeadline()
}

// TestDeadlineTrackingCycle_GetConfigErr tests trackDeadline when GetSLA has error
func (suite *DeadlineTrackerTestSuite) TestDeadlineTrackingCycle_GetSLAErr() {
	suite.jobFactory.EXPECT().GetAllJobs().Return(suite.mockJobs)
	suite.mockJob.EXPECT().GetConfig(gomock.Any()).Return(suite.mockJobConfig, nil)
	suite.mockJobConfig.EXPECT().GetSLA().Return(nil)
	suite.tracker.trackDeadline()
}

// TestDeadlineTrackingCycle_GetRunTime tests trackDeadline when GetRunTime has error
func (suite *DeadlineTrackerTestSuite) TestDeadlineTrackingCycle_GetRunTimeErr() {
	suite.mockJobConfig.EXPECT().GetSLA().
		Return(&peloton_job.SlaConfig{
			MaxRunningTime: 5,
		})
	suite.jobFactory.EXPECT().GetAllJobs().Return(suite.mockJobs)
	suite.mockJob.EXPECT().GetConfig(gomock.Any()).Return(suite.mockJobConfig, nil)
	suite.mockJob.EXPECT().GetAllTasks().Return(suite.mockTasks)
	suite.mockTask.EXPECT().GetRunTime(gomock.Any()).Return(nil, fmt.Errorf("Fake RunTime error"))
	suite.tracker.trackDeadline()
}

// TestDeadlineTrackingCycle_GetRunTime tests trackDeadline when PatchTasks has error
func (suite *DeadlineTrackerTestSuite) TestDeadlineTrackingCycle_PatchTasksErr() {
	taskInfo := &peloton_task.TaskInfo{
		InstanceId: 1,
		Runtime: &peloton_task.RuntimeInfo{
			State:     peloton_task.TaskState_RUNNING,
			StartTime: time.Now().AddDate(0, 0, -1).UTC().Format(time.RFC3339Nano),
		},
	}
	suite.mockJobConfig.EXPECT().GetSLA().
		Return(&peloton_job.SlaConfig{
			MaxRunningTime: 5,
		}).Times(3)
	suite.jobFactory.EXPECT().GetAllJobs().Return(suite.mockJobs)
	suite.mockJob.EXPECT().GetConfig(gomock.Any()).Return(suite.mockJobConfig, nil)
	suite.mockJob.EXPECT().GetAllTasks().Return(suite.mockTasks)
	suite.mockTask.EXPECT().GetRunTime(gomock.Any()).Return(taskInfo.Runtime, nil)
	suite.jobFactory.EXPECT().AddJob(gomock.Any()).Return(suite.mockJob)
	suite.mockJob.EXPECT().PatchTasks(gomock.Any(), gomock.Any()).Return(fmt.Errorf("Fake PatchTasks error"))
	suite.tracker.trackDeadline()
}

// TestTracker_StartStop tests tracker start
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

// Test trackDeadline when GetConfig failed
func (suite *DeadlineTrackerTestSuite) TestDeadlineTrackGetConfigException() {
	suite.jobFactory.EXPECT().GetAllJobs().Return(suite.mockJobs)
	suite.mockJob.EXPECT().GetConfig(gomock.Any()).Return(nil, errors.New("err"))
	suite.tracker.trackDeadline()
}

// Test trackDeadline when trackDeadline is 0
func (suite *DeadlineTrackerTestSuite) TestDeadlineTrackNoMaxRunningTime() {
	suite.jobFactory.EXPECT().GetAllJobs().Return(suite.mockJobs)
	suite.mockJob.EXPECT().GetConfig(gomock.Any()).Return(suite.mockJobConfig, nil)
	suite.mockJobConfig.EXPECT().GetSLA().
		Return(&peloton_job.SlaConfig{
			MaxRunningTime: 0,
		})
	suite.tracker.trackDeadline()
}

// Test trackDeadline when GetRuntime failed
func (suite *DeadlineTrackerTestSuite) TestDeadlineTrackGetRuntimeException() {
	suite.jobFactory.EXPECT().GetAllJobs().Return(suite.mockJobs)
	suite.mockJob.EXPECT().GetConfig(gomock.Any()).Return(suite.mockJobConfig, nil)
	suite.mockJob.EXPECT().GetAllTasks().Return(suite.mockTasks)
	suite.mockJobConfig.EXPECT().GetSLA().
		Return(&peloton_job.SlaConfig{
			MaxRunningTime: 5,
		})
	suite.mockTask.EXPECT().GetRunTime(gomock.Any()).Return(nil, errors.New(""))
	suite.tracker.trackDeadline()

}

// Test trackDeadline when task not running
func (suite *DeadlineTrackerTestSuite) TestDeadlineTrackNotRunning() {

	taskInfo := &peloton_task.TaskInfo{
		InstanceId: 1,
		Runtime: &peloton_task.RuntimeInfo{
			State: peloton_task.TaskState_PENDING,
		},
	}
	suite.jobFactory.EXPECT().GetAllJobs().Return(suite.mockJobs)
	suite.mockJob.EXPECT().GetConfig(gomock.Any()).Return(suite.mockJobConfig, nil)
	suite.mockJob.EXPECT().GetAllTasks().Return(suite.mockTasks)
	suite.mockJobConfig.EXPECT().GetSLA().
		Return(&peloton_job.SlaConfig{
			MaxRunningTime: 5,
		})
	suite.mockTask.EXPECT().GetRunTime(gomock.Any()).Return(taskInfo.Runtime, nil)

	suite.tracker.trackDeadline()
}

// Test trackDeadline when task not running
func (suite *DeadlineTrackerTestSuite) TestDeadlineTrackStopFailed() {

	taskInfo := &peloton_task.TaskInfo{
		InstanceId: 1,
		Runtime: &peloton_task.RuntimeInfo{
			State:     peloton_task.TaskState_RUNNING,
			StartTime: time.Now().AddDate(0, 0, -1).UTC().Format(time.RFC3339Nano),
		},
	}
	suite.jobFactory.EXPECT().GetAllJobs().Return(suite.mockJobs)
	suite.mockJob.EXPECT().GetConfig(gomock.Any()).Return(suite.mockJobConfig, nil)
	suite.mockJob.EXPECT().GetAllTasks().Return(suite.mockTasks)
	suite.mockJobConfig.EXPECT().GetSLA().
		Return(&peloton_job.SlaConfig{
			MaxRunningTime: 5,
		}).Times(3)
	suite.mockTask.EXPECT().GetRunTime(gomock.Any()).Return(taskInfo.Runtime, nil)
	suite.jobFactory.EXPECT().AddJob(gomock.Any()).Return(suite.mockJob)
	suite.mockJob.EXPECT().PatchTasks(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, runtimeDiffs map[uint32]cached.RuntimeDiff) {
			suite.Equal(peloton_task.TaskState_KILLED, runtimeDiffs[1][cached.GoalStateField])
		}).
		Return(errors.New(""))
	suite.tracker.trackDeadline()
}

func TestDeadlineTracker(t *testing.T) {
	suite.Run(t, new(DeadlineTrackerTestSuite))
}
