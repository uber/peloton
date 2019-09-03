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
	"fmt"
	"testing"
	"time"

	mesosv1 "github.com/uber/peloton/.gen/mesos/v1"
	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	pbupdate "github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/private/models"
	resmocks "github.com/uber/peloton/.gen/peloton/private/resmgrsvc/mocks"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/uber/peloton/pkg/common/goalstate"
	goalstatemocks "github.com/uber/peloton/pkg/common/goalstate/mocks"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	storemocks "github.com/uber/peloton/pkg/storage/mocks"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

type UpdateStartTestSuite struct {
	suite.Suite
	ctrl *gomock.Controller

	taskStore       *storemocks.MockTaskStore
	jobFactory      *cachedmocks.MockJobFactory
	jobConfigOps    *objectmocks.MockJobConfigOps
	taskConfigV2Ops *objectmocks.MockTaskConfigV2Ops
	resmgrClient    *resmocks.MockResourceManagerServiceYARPCClient

	updateGoalStateEngine *goalstatemocks.MockEngine
	jobGoalStateEngine    *goalstatemocks.MockEngine
	taskGoalStateEngine   *goalstatemocks.MockEngine
	goalStateDriver       *driver

	jobID    *peloton.JobID
	updateID *peloton.UpdateID

	updateEnt    *updateEntity
	cachedJob    *cachedmocks.MockJob
	cachedUpdate *cachedmocks.MockUpdate
	cachedTask   *cachedmocks.MockTask

	jobConfig     *pbjob.JobConfig
	prevJobConfig *pbjob.JobConfig
	jobRuntime    *pbjob.RuntimeInfo
}

func TestUpdateStart(t *testing.T) {
	suite.Run(t, new(UpdateStartTestSuite))
}

func (suite *UpdateStartTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.taskStore = storemocks.NewMockTaskStore(suite.ctrl)
	suite.jobConfigOps = objectmocks.NewMockJobConfigOps(suite.ctrl)
	suite.taskConfigV2Ops = objectmocks.NewMockTaskConfigV2Ops(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.updateGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.jobGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.taskGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.resmgrClient =
		resmocks.NewMockResourceManagerServiceYARPCClient(suite.ctrl)

	suite.goalStateDriver = &driver{
		taskStore:       suite.taskStore,
		jobConfigOps:    suite.jobConfigOps,
		taskConfigV2Ops: suite.taskConfigV2Ops,
		jobFactory:      suite.jobFactory,
		resmgrClient:    suite.resmgrClient,
		updateEngine:    suite.updateGoalStateEngine,
		taskEngine:      suite.taskGoalStateEngine,
		jobEngine:       suite.jobGoalStateEngine,
		mtx:             NewMetrics(tally.NoopScope),
		cfg:             &Config{},
	}
	suite.goalStateDriver.cfg.normalize()

	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.updateID = &peloton.UpdateID{Value: uuid.NewRandom().String()}
	suite.updateEnt = &updateEntity{
		id:     suite.updateID,
		jobID:  suite.jobID,
		driver: suite.goalStateDriver,
	}

	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.cachedUpdate = cachedmocks.NewMockUpdate(suite.ctrl)
	suite.cachedTask = cachedmocks.NewMockTask(suite.ctrl)

	commandValueNew := "new.sh"
	taskConfigNew := &pbtask.TaskConfig{
		Command: &mesosv1.CommandInfo{
			Value: &commandValueNew,
		},
	}
	suite.jobConfig = &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 4,
		},
		InstanceCount: 10,
		DefaultConfig: taskConfigNew,
	}

	commandValue := "entrypoint.sh"
	taskConfig := &pbtask.TaskConfig{
		Command: &mesosv1.CommandInfo{
			Value: &commandValue,
		},
	}
	suite.prevJobConfig = &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 3,
		},
		InstanceCount: 5,
		DefaultConfig: taskConfig,
	}

	suite.jobRuntime = &pbjob.RuntimeInfo{
		State:     pbjob.JobState_RUNNING,
		GoalState: pbjob.JobState_RUNNING,
	}
}

func (suite *UpdateStartTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func (suite *UpdateStartTestSuite) TestUpdateStartCacheJobGetFail() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		Cancel(gomock.Any(), gomock.Any()).
		Return(nil)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Do(func(updateEntity goalstate.Entity, deadline time.Time) {
			suite.Equal(suite.jobID.GetValue(), updateEntity.GetID())
		})

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any())

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

func (suite *UpdateStartTestSuite) TestUpdateStartCacheJobGetError() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		Cancel(gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any())

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

func (suite *UpdateStartTestSuite) TestUpdateStartJobConfigGetFail() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_INITIALIZED,
		})

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			JobVersion: suite.jobConfig.ChangeLog.Version,
		}).AnyTimes()

	suite.jobConfigOps.EXPECT().GetResult(
		gomock.Any(), suite.jobID, suite.jobConfig.ChangeLog.Version).
		Return(nil, fmt.Errorf("fake db error"))

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

func (suite *UpdateStartTestSuite) TestUpdateStartTaskConfigCreateFail() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_INITIALIZED,
		})

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			JobVersion: suite.jobConfig.ChangeLog.Version,
		}).AnyTimes()

	suite.jobConfigOps.EXPECT().GetResult(
		gomock.Any(), suite.jobID, suite.jobConfig.ChangeLog.Version).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig:   suite.jobConfig,
			ConfigAddOn: &models.ConfigAddOn{},
		}, nil)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			nil).
		Return(fmt.Errorf("fake db error"))

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

func (suite *UpdateStartTestSuite) TestUpdateStartWriteProgressFail() {
	instancesTotal := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_INITIALIZED,
		})

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID).
		AnyTimes()

	suite.jobConfigOps.EXPECT().GetResult(
		gomock.Any(), suite.jobID, suite.jobConfig.ChangeLog.Version).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig:   suite.jobConfig,
			ConfigAddOn: &models.ConfigAddOn{},
		}, nil)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			nil).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_RESTART)

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  instancesTotal,
			JobVersion: suite.jobConfig.ChangeLog.Version,
		}).AnyTimes()

	suite.cachedJob.EXPECT().
		WriteWorkflowProgress(
			gomock.Any(),
			suite.updateID,
			pbupdate.State_ROLLING_FORWARD,
			[]uint32{},
			[]uint32{},
			gomock.Any(),
		).Return(fmt.Errorf("fake db error"))

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

// TestUpdateContainsUnchangedInstance test the situation update
// contains unchanged instance
func (suite *UpdateStartTestSuite) TestUpdateContainsUnchangedInstance() {
	instancesTotal := []uint32{0}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedJob.EXPECT().
		ID().
		Return(&peloton.JobID{Value: uuid.New()})

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_INITIALIZED,
		})

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID).
		AnyTimes()

	suite.jobConfigOps.EXPECT().GetResult(
		gomock.Any(), suite.jobID, suite.jobConfig.ChangeLog.Version).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig:   suite.jobConfig,
			ConfigAddOn: &models.ConfigAddOn{},
		}, nil)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			nil).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_RESTART).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  instancesTotal,
			JobVersion: suite.jobConfig.ChangeLog.Version,
		}).AnyTimes()

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Do(func(ctx context.Context, runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff, _ bool) {
			for i := uint32(len(instancesTotal)); i < suite.jobConfig.GetInstanceCount(); i++ {
				runtimeDiff := runtimeDiffs[i]
				suite.NotNil(runtimeDiff)
				suite.Equal(runtimeDiff[jobmgrcommon.DesiredConfigVersionField],
					suite.jobConfig.GetChangeLog().Version)
				suite.Equal(runtimeDiff[jobmgrcommon.ConfigVersionField],
					suite.jobConfig.GetChangeLog().Version)
			}
		}).Return(nil, nil, nil)

	suite.cachedJob.EXPECT().
		WriteWorkflowProgress(
			gomock.Any(),
			suite.updateID,
			pbupdate.State_ROLLING_FORWARD,
			[]uint32{},
			[]uint32{},
			gomock.Any(),
		).Return(nil)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateStart_ContainsUnchangedInstance_PatchTasksFail test the situation update
// contains unchanged instance
func (suite *UpdateStartTestSuite) TestUpdateStart_ContainsUnchangedInstance_PatchTasksFail() {
	instancesTotal := []uint32{0}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_INITIALIZED,
		})

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID).
		AnyTimes()

	suite.jobConfigOps.EXPECT().GetResult(
		gomock.Any(), suite.jobID, suite.jobConfig.ChangeLog.Version).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig:   suite.jobConfig,
			ConfigAddOn: &models.ConfigAddOn{},
		}, nil)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			nil).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_RESTART)

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  instancesTotal,
			JobVersion: suite.jobConfig.ChangeLog.Version,
		}).AnyTimes()

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, nil, yarpcerrors.UnavailableErrorf("test error"))

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.Error(err)
}

// TestUpdateStart_NoUnchangedInstance test the situation update
// contains no unchanged instance
func (suite *UpdateStartTestSuite) TestUpdateStart_NoUnchangedInstance() {
	var instancesTotal []uint32
	for i := uint32(0); i < suite.jobConfig.GetInstanceCount(); i++ {
		instancesTotal = append(instancesTotal, i)
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		ID().
		Return(&peloton.JobID{Value: uuid.New()})

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_INITIALIZED,
		})

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID).
		AnyTimes()

	suite.jobConfigOps.EXPECT().GetResult(
		gomock.Any(), suite.jobID, suite.jobConfig.ChangeLog.Version).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig:   suite.jobConfig,
			ConfigAddOn: &models.ConfigAddOn{},
		}, nil)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			nil).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_RESTART).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  instancesTotal,
			JobVersion: suite.jobConfig.ChangeLog.Version,
		}).AnyTimes()

	suite.cachedJob.EXPECT().
		WriteWorkflowProgress(
			gomock.Any(),
			suite.updateID,
			pbupdate.State_ROLLING_FORWARD,
			[]uint32{},
			[]uint32{},
			gomock.Any(),
		).Return(nil)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateWorkflowUpdate tests starting a workflow update with no unchanged instances
func (suite *UpdateStartTestSuite) TestUpdateWorkflowUpdate() {
	instancesTotal := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	taskRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < suite.prevJobConfig.InstanceCount; i++ {
		runtime := &pbtask.RuntimeInfo{
			State:                pbtask.TaskState_RUNNING,
			ConfigVersion:        suite.prevJobConfig.ChangeLog.Version,
			DesiredConfigVersion: suite.prevJobConfig.ChangeLog.Version,
		}
		taskRuntimes[i] = runtime
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_INITIALIZED,
		})

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			JobVersion: suite.jobConfig.ChangeLog.Version,
			Instances:  instancesTotal,
		}).AnyTimes()

	suite.jobConfigOps.EXPECT().GetResult(
		gomock.Any(), suite.jobID, suite.jobConfig.ChangeLog.Version).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig:   suite.jobConfig,
			ConfigAddOn: &models.ConfigAddOn{},
		}, nil)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			nil).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			JobVersion: suite.prevJobConfig.ChangeLog.Version,
		})

	suite.jobConfigOps.EXPECT().Get(
		gomock.Any(), suite.jobID, suite.prevJobConfig.ChangeLog.Version).
		Return(suite.prevJobConfig, &models.ConfigAddOn{}, nil)

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, nil).
		Return(taskRuntimes, nil)

	suite.taskConfigV2Ops.EXPECT().
		GetTaskConfig(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(suite.prevJobConfig.DefaultConfig, nil, nil).
		Times(int(suite.prevJobConfig.InstanceCount))

	suite.cachedUpdate.EXPECT().
		Modify(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(
			_ context.Context,
			instancesAdded []uint32,
			instancesUpdated []uint32,
			instancesRemoved []uint32,
		) {
			suite.Len(instancesAdded,
				int(suite.jobConfig.InstanceCount-suite.prevJobConfig.InstanceCount))
			suite.Len(instancesUpdated, int(suite.prevJobConfig.InstanceCount))
			suite.Empty(instancesRemoved)
		}).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  instancesTotal,
			JobVersion: suite.jobConfig.ChangeLog.Version,
		}).AnyTimes()

	suite.cachedJob.EXPECT().
		WriteWorkflowProgress(
			gomock.Any(),
			suite.updateID,
			pbupdate.State_ROLLING_FORWARD,
			[]uint32{},
			[]uint32{},
			gomock.Any(),
		).Return(nil)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateWorkflowUpdatePrevJobConfigGetError tests getting an error when
// previous job config is fetched
func (suite *UpdateStartTestSuite) TestUpdateWorkflowUpdatePrevJobConfigGetError() {
	instancesTotal := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_INITIALIZED,
		})

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			JobVersion: suite.jobConfig.ChangeLog.Version,
			Instances:  instancesTotal,
		}).AnyTimes()

	suite.jobConfigOps.EXPECT().GetResult(
		gomock.Any(), suite.jobID, suite.jobConfig.ChangeLog.Version).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig:   suite.jobConfig,
			ConfigAddOn: &models.ConfigAddOn{},
		}, nil)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			nil).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			JobVersion: suite.prevJobConfig.ChangeLog.Version,
		})

	suite.jobConfigOps.EXPECT().Get(
		gomock.Any(), suite.jobID, suite.prevJobConfig.ChangeLog.Version).
		Return(nil, nil, fmt.Errorf("fake db error"))

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.Error(err)
}

// TestUpdateWorkflowUpdate tests getting an error when task runtimes are fetched
func (suite *UpdateStartTestSuite) TestUpdateWorkflowUpdateGetRuntimeError() {
	instancesTotal := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_INITIALIZED,
		})

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			JobVersion: suite.jobConfig.ChangeLog.Version,
			Instances:  instancesTotal,
		}).AnyTimes()

	suite.jobConfigOps.EXPECT().GetResult(
		gomock.Any(), suite.jobID, suite.jobConfig.ChangeLog.Version).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig:   suite.jobConfig,
			ConfigAddOn: &models.ConfigAddOn{},
		}, nil)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			nil).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			JobVersion: suite.prevJobConfig.ChangeLog.Version,
		})

	suite.jobConfigOps.EXPECT().Get(
		gomock.Any(), suite.jobID, suite.prevJobConfig.ChangeLog.Version).
		Return(suite.prevJobConfig, &models.ConfigAddOn{}, nil)

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID)

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, nil).
		Return(nil, fmt.Errorf("fake db error"))

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.Error(err)
}

// TestUpdateWorkflowUpdateModifyError tests getting an error when modifying the update
func (suite *UpdateStartTestSuite) TestUpdateWorkflowUpdateModifyError() {
	instancesTotal := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	taskRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < suite.prevJobConfig.InstanceCount; i++ {
		runtime := &pbtask.RuntimeInfo{
			State:         pbtask.TaskState_RUNNING,
			ConfigVersion: suite.prevJobConfig.ChangeLog.Version,
		}
		taskRuntimes[i] = runtime
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_INITIALIZED,
		})

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			JobVersion: suite.jobConfig.ChangeLog.Version,
			Instances:  instancesTotal,
		}).AnyTimes()

	suite.jobConfigOps.EXPECT().GetResult(
		gomock.Any(), suite.jobID, suite.jobConfig.ChangeLog.Version).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig:   suite.jobConfig,
			ConfigAddOn: &models.ConfigAddOn{},
		}, nil)

	suite.cachedJob.EXPECT().
		CreateTaskConfigs(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			nil).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			JobVersion: suite.prevJobConfig.ChangeLog.Version,
		})

	suite.jobConfigOps.EXPECT().Get(
		gomock.Any(), suite.jobID, suite.prevJobConfig.ChangeLog.Version).
		Return(suite.prevJobConfig, &models.ConfigAddOn{}, nil)

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, nil).
		Return(taskRuntimes, nil)

	suite.taskConfigV2Ops.EXPECT().
		GetTaskConfig(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(suite.prevJobConfig.DefaultConfig, nil, nil).
		Times(int(suite.prevJobConfig.InstanceCount))

	suite.cachedUpdate.EXPECT().
		Modify(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err := UpdateStart(context.Background(), suite.updateEnt)
	suite.Error(err)
}
