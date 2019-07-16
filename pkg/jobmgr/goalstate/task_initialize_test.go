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

package goalstate

import (
	"context"
	"errors"
	"testing"

	mesos_v1 "github.com/uber/peloton/.gen/mesos/v1"
	pb_job "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/models"

	goalstatemocks "github.com/uber/peloton/pkg/common/goalstate/mocks"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	store_mocks "github.com/uber/peloton/pkg/storage/mocks"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type TestTaskInitializeSuite struct {
	suite.Suite
	mockCtrl            *gomock.Controller
	jobStore            *store_mocks.MockJobStore
	taskStore           *store_mocks.MockTaskStore
	jobGoalStateEngine  *goalstatemocks.MockEngine
	taskGoalStateEngine *goalstatemocks.MockEngine
	jobFactory          *cachedmocks.MockJobFactory
	cachedJob           *cachedmocks.MockJob
	cachedTask          *cachedmocks.MockTask
	jobConfig           *cachedmocks.MockJobConfigCache
	cachedConfig        *cachedmocks.MockJobConfigCache
	taskConfigV2Ops     *objectmocks.MockTaskConfigV2Ops
	goalStateDriver     *driver
	jobID               *peloton.JobID
	instanceID          uint32
	newConfigVersion    uint64
	oldMesosTaskID      string
	taskEnt             *taskEntity
	runtime             *pbtask.RuntimeInfo
}

func TestTaskInitializeRun(t *testing.T) {
	suite.Run(t, new(TestTaskInitializeSuite))
}

func (suite *TestTaskInitializeSuite) SetupTest() {
	suite.mockCtrl = gomock.NewController(suite.T())
	defer suite.mockCtrl.Finish()

	suite.jobStore = store_mocks.NewMockJobStore(suite.mockCtrl)
	suite.taskStore = store_mocks.NewMockTaskStore(suite.mockCtrl)
	suite.jobGoalStateEngine = goalstatemocks.NewMockEngine(suite.mockCtrl)
	suite.taskGoalStateEngine = goalstatemocks.NewMockEngine(suite.mockCtrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.mockCtrl)
	suite.cachedJob = cachedmocks.NewMockJob(suite.mockCtrl)
	suite.cachedTask = cachedmocks.NewMockTask(suite.mockCtrl)
	suite.jobConfig = cachedmocks.NewMockJobConfigCache(suite.mockCtrl)
	suite.cachedConfig = cachedmocks.NewMockJobConfigCache(suite.mockCtrl)
	suite.taskConfigV2Ops = objectmocks.NewMockTaskConfigV2Ops(suite.mockCtrl)

	suite.goalStateDriver = &driver{
		jobEngine:       suite.jobGoalStateEngine,
		taskEngine:      suite.taskGoalStateEngine,
		jobStore:        suite.jobStore,
		taskStore:       suite.taskStore,
		jobFactory:      suite.jobFactory,
		taskConfigV2Ops: suite.taskConfigV2Ops,
		mtx:             NewMetrics(tally.NoopScope),
		cfg:             &Config{},
	}
	suite.goalStateDriver.cfg.normalize()
	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.instanceID = uint32(0)

	suite.taskEnt = &taskEntity{
		jobID:      suite.jobID,
		instanceID: suite.instanceID,
		driver:     suite.goalStateDriver,
	}

	suite.newConfigVersion = uint64(2)
	suite.oldMesosTaskID = uuid.New()

	suite.runtime = &pbtask.RuntimeInfo{
		State: pbtask.TaskState_KILLED,
		MesosTaskId: &mesos_v1.TaskID{
			Value: &suite.oldMesosTaskID,
		},
		ConfigVersion:        suite.newConfigVersion - 1,
		DesiredConfigVersion: suite.newConfigVersion,
	}
}

// Test TaskInitialize in  happy case
func (suite *TestTaskInitializeSuite) TestTaskInitialize() {
	testTable := []struct {
		taskConfig  *pbtask.TaskConfig
		healthState pbtask.HealthState
	}{
		{
			taskConfig:  &pbtask.TaskConfig{},
			healthState: pbtask.HealthState_DISABLED,
		},
		{
			taskConfig: &pbtask.TaskConfig{
				HealthCheck: &pbtask.HealthCheckConfig{
					Enabled:                true,
					InitialIntervalSecs:    10,
					IntervalSecs:           10,
					MaxConsecutiveFailures: 10,
					TimeoutSecs:            10,
				},
			},
			healthState: pbtask.HealthState_HEALTH_UNKNOWN,
		},
		{
			taskConfig: &pbtask.TaskConfig{
				HealthCheck: &pbtask.HealthCheckConfig{
					Enabled:                false,
					InitialIntervalSecs:    10,
					IntervalSecs:           10,
					MaxConsecutiveFailures: 10,
					TimeoutSecs:            10,
				},
			},
			healthState: pbtask.HealthState_DISABLED,
		},
	}

	for _, tt := range testTable {
		newRuntime := suite.runtime

		suite.jobFactory.EXPECT().
			GetJob(suite.jobID).Return(suite.cachedJob)

		suite.cachedJob.EXPECT().
			GetTask(suite.instanceID).Return(suite.cachedTask)

		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).Return(suite.runtime, nil)

		suite.taskConfigV2Ops.EXPECT().GetTaskConfig(
			gomock.Any(), suite.jobID, suite.instanceID, suite.newConfigVersion).
			Return(tt.taskConfig, &models.ConfigAddOn{}, nil)

		suite.cachedJob.EXPECT().PatchTasks(gomock.Any(), gomock.Any(), false).Do(
			func(_ context.Context,
				runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff,
				_ bool) {
				for _, runtimeDiff := range runtimeDiffs {
					suite.Equal(
						pbtask.TaskState_INITIALIZED,
						runtimeDiff[jobmgrcommon.StateField],
					)
					suite.Equal(
						suite.newConfigVersion,
						runtimeDiff[jobmgrcommon.ConfigVersionField],
					)
					suite.Equal(
						tt.healthState,
						runtimeDiff[jobmgrcommon.HealthyField],
					)

				}
			}).Return(nil, nil, nil)

		suite.cachedConfig.EXPECT().
			GetType().Return(pb_job.JobType_BATCH)

		suite.cachedJob.EXPECT().
			GetJobType().Return(pb_job.JobType_BATCH)

		suite.taskGoalStateEngine.EXPECT().
			Enqueue(gomock.Any(), gomock.Any()).
			Return()

		suite.jobGoalStateEngine.EXPECT().
			Enqueue(gomock.Any(), gomock.Any()).
			Return()

		err := TaskInitialize(context.Background(), suite.taskEnt)
		suite.NoError(err)
		suite.NotEqual(suite.oldMesosTaskID, newRuntime.MesosTaskId)
	}
}

// Test TaskInitialize with no cached job
func (suite *TestTaskInitializeSuite) TestTaskInitializeNoJob() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).Return(nil)

	err := TaskInitialize(context.Background(), suite.taskEnt)
	suite.NoError(err)
}

// Test TaskInitialize with no cached task
func (suite *TestTaskInitializeSuite) TestTaskInitializeNoTask() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetTask(suite.instanceID).Return(nil)

	err := TaskInitialize(context.Background(), suite.taskEnt)
	suite.NoError(err)
}

// Test TaskInitialize when no task runtime exist
func (suite *TestTaskInitializeSuite) TestTaskInitializeNoTaskRuntime() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetTask(suite.instanceID).Return(suite.cachedTask)

	suite.cachedTask.EXPECT().
		GetRuntime(gomock.Any()).Return(nil, errors.New(""))

	err := TaskInitialize(context.Background(), suite.taskEnt)
	suite.Error(err)
}

// Test TaskInitialize when task config exist
func (suite *TestTaskInitializeSuite) TestTaskInitializeNoTaskConfig() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetTask(suite.instanceID).Return(suite.cachedTask)

	suite.cachedTask.EXPECT().
		GetRuntime(gomock.Any()).Return(suite.runtime, nil)

	suite.taskConfigV2Ops.EXPECT().GetTaskConfig(
		gomock.Any(), suite.jobID, suite.instanceID, suite.newConfigVersion).Return(nil, nil, errors.New(""))

	err := TaskInitialize(context.Background(), suite.taskEnt)
	suite.Error(err)
}
