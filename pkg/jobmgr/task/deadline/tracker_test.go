// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package deadline

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	peloton_job "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	peloton_task "github.com/uber/peloton/.gen/peloton/api/v0/task"

	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	goalstatemocks "github.com/uber/peloton/pkg/jobmgr/goalstate/mocks"
	storage_mocks "github.com/uber/peloton/pkg/storage/mocks"

	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"

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
	suite.mockTask.EXPECT().GetRuntime(gomock.Any()).Return(taskInfo.Runtime, nil)
	suite.jobFactory.EXPECT().AddJob(gomock.Any()).Return(suite.mockJob)
	suite.mockJob.EXPECT().PatchTasks(gomock.Any(), gomock.Any(), false).
		Do(func(ctx context.Context,
			runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff,
			_ bool) {
			suite.Equal(peloton_task.TaskState_KILLED, runtimeDiffs[1][jobmgrcommon.GoalStateField])
			suite.Equal(
				peloton_task.TerminationStatus_TERMINATION_STATUS_REASON_DEADLINE_TIMEOUT_EXCEEDED,
				runtimeDiffs[1][jobmgrcommon.TerminationStatusField].(*peloton_task.TerminationStatus).GetReason())
		}).Return(nil, nil, nil)
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

// TestDeadlineTrackingCycle_GetRunTime tests trackDeadline when GetRuntime has error
func (suite *DeadlineTrackerTestSuite) TestDeadlineTrackingCycle_GetRunTimeErr() {
	suite.mockJobConfig.EXPECT().GetSLA().
		Return(&peloton_job.SlaConfig{
			MaxRunningTime: 5,
		})
	suite.jobFactory.EXPECT().GetAllJobs().Return(suite.mockJobs)
	suite.mockJob.EXPECT().GetConfig(gomock.Any()).Return(suite.mockJobConfig, nil)
	suite.mockJob.EXPECT().GetAllTasks().Return(suite.mockTasks)
	suite.mockTask.EXPECT().GetRuntime(gomock.Any()).Return(nil, fmt.Errorf("Fake Runtime error"))
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
	suite.mockTask.EXPECT().GetRuntime(gomock.Any()).Return(taskInfo.Runtime, nil)
	suite.jobFactory.EXPECT().AddJob(gomock.Any()).Return(suite.mockJob)
	suite.mockJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, nil, fmt.Errorf("Fake PatchTasks error"))
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
	suite.mockTask.EXPECT().GetRuntime(gomock.Any()).Return(nil, errors.New(""))
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
	suite.mockTask.EXPECT().GetRuntime(gomock.Any()).Return(taskInfo.Runtime, nil)

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
	suite.mockTask.EXPECT().GetRuntime(gomock.Any()).Return(taskInfo.Runtime, nil)
	suite.jobFactory.EXPECT().AddJob(gomock.Any()).Return(suite.mockJob)
	suite.mockJob.EXPECT().PatchTasks(gomock.Any(), gomock.Any(), false).
		Do(func(ctx context.Context,
			runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff,
			_ bool) {
			suite.Equal(peloton_task.TaskState_KILLED, runtimeDiffs[1][jobmgrcommon.GoalStateField])
		}).Return(nil, nil, errors.New(""))
	suite.tracker.trackDeadline()
}

func TestDeadlineTracker(t *testing.T) {
	suite.Run(t, new(DeadlineTrackerTestSuite))
}
