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

package updatesvc

import (
	"context"
	"fmt"
	"testing"

	"github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/api/v0/update/svc"
	"github.com/uber/peloton/.gen/peloton/private/models"

	versionutil "github.com/uber/peloton/pkg/common/util/entityversion"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	goalstatemocks "github.com/uber/peloton/pkg/jobmgr/goalstate/mocks"
	storemocks "github.com/uber/peloton/pkg/storage/mocks"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	"go.uber.org/yarpc/yarpcerrors"
)

type UpdateSvcTestSuite struct {
	suite.Suite

	ctrl            *gomock.Controller
	jobConfigOps    *objectmocks.MockJobConfigOps
	jobRuntimeOps   *objectmocks.MockJobRuntimeOps
	updateStore     *storemocks.MockUpdateStore
	jobFactory      *cachedmocks.MockJobFactory
	goalStateDriver *goalstatemocks.MockDriver
	h               *serviceHandler

	cachedJobConfig *cachedmocks.MockJobConfigCache
	cachedJob       *cachedmocks.MockJob
	cachedUpdate    *cachedmocks.MockUpdate

	cachedTask *cachedmocks.MockTask

	jobRuntime *job.RuntimeInfo
	jobConfig  *job.JobConfig

	updateConfig *update.UpdateConfig
	newJobConfig *job.JobConfig

	jobID     *peloton.JobID
	respoolID *peloton.ResourcePoolID
	updateID  *peloton.UpdateID
}

func TestUpdateSvc(t *testing.T) {
	suite.Run(t, new(UpdateSvcTestSuite))
}

func (suite *UpdateSvcTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())

	suite.jobConfigOps = objectmocks.NewMockJobConfigOps(suite.ctrl)
	suite.jobRuntimeOps = objectmocks.NewMockJobRuntimeOps(suite.ctrl)
	suite.updateStore = storemocks.NewMockUpdateStore(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.goalStateDriver = goalstatemocks.NewMockDriver(suite.ctrl)

	suite.cachedJobConfig = cachedmocks.NewMockJobConfigCache(suite.ctrl)
	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.cachedTask = cachedmocks.NewMockTask(suite.ctrl)
	suite.cachedUpdate = cachedmocks.NewMockUpdate(suite.ctrl)
	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.respoolID = &peloton.ResourcePoolID{Value: uuid.NewRandom().String()}
	suite.updateID = &peloton.UpdateID{Value: uuid.NewRandom().String()}

	suite.jobRuntime = &job.RuntimeInfo{
		State:                job.JobState_RUNNING,
		GoalState:            job.JobState_RUNNING,
		ConfigurationVersion: 2,
		WorkflowVersion:      1,
	}

	labels := []*peloton.Label{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
	}
	cmd1 := "hello world 1"
	suite.jobConfig = &job.JobConfig{
		Type:          job.JobType_SERVICE,
		InstanceCount: uint32(10),
		RespoolID:     suite.respoolID,
		ChangeLog: &peloton.ChangeLog{
			Version: uint64(2),
		},
		DefaultConfig: &task.TaskConfig{
			Command: &mesos_v1.CommandInfo{Value: &cmd1},
		},
		Labels: labels,
	}

	cmd2 := "hello world 2"
	suite.newJobConfig = &job.JobConfig{
		Type:          job.JobType_SERVICE,
		InstanceCount: uint32(10),
		RespoolID:     suite.respoolID,
		ChangeLog: &peloton.ChangeLog{
			Version: uint64(3),
		},
		DefaultConfig: &task.TaskConfig{
			Command: &mesos_v1.CommandInfo{Value: &cmd2},
		},
		Labels: labels,
	}

	suite.updateConfig = &update.UpdateConfig{
		BatchSize: uint32(2),
	}

	suite.h = &serviceHandler{
		jobConfigOps:    suite.jobConfigOps,
		jobRuntimeOps:   suite.jobRuntimeOps,
		updateStore:     suite.updateStore,
		goalStateDriver: suite.goalStateDriver,
		jobFactory:      suite.jobFactory,
		metrics:         NewMetrics(tally.NoopScope),
	}
}

func (suite *UpdateSvcTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestCreateSuccess tests successfully creating a job update
func (suite *UpdateSvcTestSuite) TestCreateSuccess() {
	opaque := "test"
	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.jobRuntimeOps.EXPECT().
		Get(gomock.Any(), suite.jobID).Return(suite.jobRuntime, nil)

	configAddOn := &models.ConfigAddOn{}
	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), suite.jobID, gomock.Any()).
		Return(suite.jobConfig, configAddOn, nil)

	suite.cachedJob.EXPECT().
		CreateWorkflow(
			gomock.Any(),
			models.WorkflowType_UPDATE,
			suite.updateConfig,
			gomock.Any(),
			gomock.Any(),
		).
		Return(
			suite.updateID,
			versionutil.GetJobEntityVersion(
				suite.jobRuntime.GetConfigurationVersion()+1,
				suite.jobRuntime.GetDesiredStateVersion(),
				suite.jobRuntime.GetWorkflowVersion()),
			nil)

	suite.goalStateDriver.EXPECT().
		EnqueueUpdate(gomock.Any(), gomock.Any(), gomock.Any())

	_, err := suite.h.CreateUpdate(
		context.Background(),
		&svc.CreateUpdateRequest{
			JobId:        suite.jobID,
			JobConfig:    suite.newJobConfig,
			UpdateConfig: suite.updateConfig,
			OpaqueData:   &peloton.OpaqueData{Data: opaque},
		},
	)
	suite.NoError(err)
}

// TestAddInstancesSuccess tests successfully add instances
func (suite *UpdateSvcTestSuite) TestAddInstancesSuccess() {
	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.jobRuntimeOps.EXPECT().
		Get(gomock.Any(), suite.jobID).Return(suite.jobRuntime, nil)

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), suite.jobID, gomock.Any()).
		Return(suite.jobConfig, &models.ConfigAddOn{}, nil)

	suite.cachedJob.EXPECT().
		CreateWorkflow(
			gomock.Any(),
			models.WorkflowType_UPDATE,
			suite.updateConfig,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).
		Return(suite.updateID,
			versionutil.GetJobEntityVersion(
				suite.jobRuntime.GetConfigurationVersion()+1,
				suite.jobRuntime.GetDesiredStateVersion(),
				suite.jobRuntime.GetWorkflowVersion(),
			),
			nil)

	suite.goalStateDriver.EXPECT().
		EnqueueUpdate(gomock.Any(), gomock.Any(), gomock.Any())

	suite.newJobConfig.InstanceCount = 12
	_, err := suite.h.CreateUpdate(
		context.Background(),
		&svc.CreateUpdateRequest{
			JobId:        suite.jobID,
			JobConfig:    suite.newJobConfig,
			UpdateConfig: suite.updateConfig,
		},
	)
	suite.NoError(err)
}

// TestUpdateLabelsSuccess tests label update
func (suite *UpdateSvcTestSuite) TestUpdateLabelsSuccess() {
	newConfig := *suite.jobConfig
	newConfig.Labels = append(newConfig.Labels, &peloton.Label{Key: "newKey", Value: "newLabel"})

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.jobRuntimeOps.EXPECT().
		Get(gomock.Any(), suite.jobID).Return(suite.jobRuntime, nil)

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), suite.jobID, gomock.Any()).
		Return(suite.jobConfig, &models.ConfigAddOn{}, nil)

	suite.cachedJob.EXPECT().
		CreateWorkflow(
			gomock.Any(),
			models.WorkflowType_UPDATE,
			suite.updateConfig,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).
		Return(suite.updateID,
			versionutil.GetJobEntityVersion(
				suite.jobRuntime.GetConfigurationVersion(),
				suite.jobRuntime.GetDesiredStateVersion(),
				suite.jobRuntime.GetWorkflowVersion()+1),
			nil)

	suite.goalStateDriver.EXPECT().
		EnqueueUpdate(gomock.Any(), gomock.Any(), gomock.Any())

	_, err := suite.h.CreateUpdate(
		context.Background(),
		&svc.CreateUpdateRequest{
			JobId:        suite.jobID,
			JobConfig:    &newConfig,
			UpdateConfig: suite.updateConfig,
		},
	)
	suite.NoError(err)
}

// TestCreateBadJobUUID tests creating a job update with a non-UUID jobID
func (suite *UpdateSvcTestSuite) TestCreateBadJobUUID() {
	badJobID := &peloton.JobID{Value: "bad-value"}
	_, err := suite.h.CreateUpdate(
		context.Background(),
		&svc.CreateUpdateRequest{
			JobId:        badJobID,
			JobConfig:    suite.newJobConfig,
			UpdateConfig: suite.updateConfig,
		},
	)
	suite.True(yarpcerrors.IsInvalidArgument(err))
	suite.EqualError(err,
		"code:invalid-argument message:JobID must be of UUID format")
}

// TestCreateFailJobNotFound tests failing to find the job provided
// in the create update request
func (suite *UpdateSvcTestSuite) TestCreateFailJobNotFound() {
	suite.jobRuntimeOps.EXPECT().
		Get(gomock.Any(), suite.jobID).Return(nil, fmt.Errorf("fake db error"))
	_, err := suite.h.CreateUpdate(
		context.Background(),
		&svc.CreateUpdateRequest{
			JobId:        suite.jobID,
			JobConfig:    suite.newJobConfig,
			UpdateConfig: suite.updateConfig,
		},
	)

	suite.True(yarpcerrors.IsNotFound(err))
	suite.EqualError(err, "code:not-found message:job not found")
}

// TestCreateInitializedJob tests trying to update a paritally-created job
func (suite *UpdateSvcTestSuite) TestCreateInitializedJob() {
	suite.jobRuntime.State = job.JobState_INITIALIZED
	suite.jobRuntimeOps.EXPECT().
		Get(gomock.Any(), suite.jobID).Return(suite.jobRuntime, nil)

	_, err := suite.h.CreateUpdate(
		context.Background(),
		&svc.CreateUpdateRequest{
			JobId:        suite.jobID,
			JobConfig:    suite.newJobConfig,
			UpdateConfig: suite.updateConfig,
		},
	)

	suite.True(yarpcerrors.IsUnavailable(err))
	suite.EqualError(err,
		"code:unavailable message:cannot update partially created job")
}

// TestCreateGetJobConfigFail tests failing to get job config
// from DB during update create request
func (suite *UpdateSvcTestSuite) TestCreateGetJobConfigFail() {
	suite.jobRuntimeOps.EXPECT().
		Get(gomock.Any(), suite.jobID).Return(suite.jobRuntime, nil)

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), suite.jobID, gomock.Any()).
		Return(nil, nil, fmt.Errorf("fake db error"))

	_, err := suite.h.CreateUpdate(
		context.Background(),
		&svc.CreateUpdateRequest{
			JobId:        suite.jobID,
			JobConfig:    suite.newJobConfig,
			UpdateConfig: suite.updateConfig,
		},
	)
	suite.EqualError(err, "fake db error")
}

// TestCreateBatchJob tests creating a job update for batch jobs
func (suite *UpdateSvcTestSuite) TestCreateBatchJob() {
	suite.jobConfig.Type = job.JobType_BATCH

	suite.jobRuntimeOps.EXPECT().
		Get(gomock.Any(), suite.jobID).Return(suite.jobRuntime, nil)

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), suite.jobID, gomock.Any()).
		Return(suite.jobConfig, &models.ConfigAddOn{}, nil)

	_, err := suite.h.CreateUpdate(
		context.Background(),
		&svc.CreateUpdateRequest{
			JobId:        suite.jobID,
			JobConfig:    suite.newJobConfig,
			UpdateConfig: suite.updateConfig,
		},
	)

	suite.True(yarpcerrors.IsInvalidArgument(err))
	suite.EqualError(err,
		"code:invalid-argument message:job must be of type service")
}

// TestCreateMissingChangeLog tests creating a job update with no changelog
// version in the new job configuration
func (suite *UpdateSvcTestSuite) TestCreateMissingChangeLog() {
	suite.newJobConfig.ChangeLog = nil

	suite.jobRuntimeOps.EXPECT().
		Get(gomock.Any(), suite.jobID).Return(suite.jobRuntime, nil)

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), suite.jobID, gomock.Any()).
		Return(suite.jobConfig, &models.ConfigAddOn{}, nil)

	_, err := suite.h.CreateUpdate(
		context.Background(),
		&svc.CreateUpdateRequest{
			JobId:        suite.jobID,
			JobConfig:    suite.newJobConfig,
			UpdateConfig: suite.updateConfig,
		},
	)

	suite.True(yarpcerrors.IsInvalidArgument(err))
	suite.EqualError(err,
		"code:invalid-argument message:missing changelog in job configuration")
}

// TestCreateChangeJobType tests creating a job update with job type set to
// BATCH in the new job configuration
func (suite *UpdateSvcTestSuite) TestCreateChangeJobType() {
	suite.newJobConfig.Type = job.JobType_BATCH

	suite.jobRuntimeOps.EXPECT().
		Get(gomock.Any(), suite.jobID).Return(suite.jobRuntime, nil)

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), suite.jobID, gomock.Any()).
		Return(suite.jobConfig, &models.ConfigAddOn{}, nil)

	_, err := suite.h.CreateUpdate(
		context.Background(),
		&svc.CreateUpdateRequest{
			JobId:        suite.jobID,
			JobConfig:    suite.newJobConfig,
			UpdateConfig: suite.updateConfig,
		},
	)

	suite.True(yarpcerrors.IsInvalidArgument(err))
	suite.EqualError(err,
		"code:invalid-argument message:job type is immutable")
}

// TestCreateReduceInstanceCount tests creating a job update with
// reduced instance count in the new job configuration
func (suite *UpdateSvcTestSuite) TestCreateReduceInstanceCount() {
	suite.newJobConfig.InstanceCount = suite.jobConfig.InstanceCount - 1

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.jobRuntimeOps.EXPECT().
		Get(gomock.Any(), suite.jobID).Return(suite.jobRuntime, nil)

	configAddOn := &models.ConfigAddOn{}
	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), suite.jobID, gomock.Any()).
		Return(suite.jobConfig, configAddOn, nil)

	suite.cachedJob.EXPECT().
		CreateWorkflow(
			gomock.Any(),
			models.WorkflowType_UPDATE,
			suite.updateConfig,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).
		Return(
			suite.updateID,
			versionutil.GetJobEntityVersion(
				suite.jobRuntime.GetConfigurationVersion()+1,
				suite.jobRuntime.GetDesiredStateVersion(),
				suite.jobRuntime.GetWorkflowVersion()),
			nil)

	suite.goalStateDriver.EXPECT().
		EnqueueUpdate(gomock.Any(), gomock.Any(), gomock.Any())

	_, err := suite.h.CreateUpdate(
		context.Background(),
		&svc.CreateUpdateRequest{
			JobId:        suite.jobID,
			JobConfig:    suite.newJobConfig,
			UpdateConfig: suite.updateConfig,
		},
	)
	suite.NoError(err)
}

// TestCreateChangeRespoolID tests creating a job update with a different
// resource pool identifier in the new job configuration
func (suite *UpdateSvcTestSuite) TestCreateChangeRespoolID() {
	suite.newJobConfig.RespoolID = &peloton.ResourcePoolID{
		Value: uuid.NewRandom().String(),
	}

	suite.jobRuntimeOps.EXPECT().
		Get(gomock.Any(), suite.jobID).Return(suite.jobRuntime, nil)

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), suite.jobID, gomock.Any()).
		Return(suite.jobConfig, &models.ConfigAddOn{}, nil)

	_, err := suite.h.CreateUpdate(
		context.Background(),
		&svc.CreateUpdateRequest{
			JobId:        suite.jobID,
			JobConfig:    suite.newJobConfig,
			UpdateConfig: suite.updateConfig,
		},
	)

	suite.True(yarpcerrors.IsInvalidArgument(err))
	suite.EqualError(err,
		"code:invalid-argument message:resource pool identifier is immutable")
}

// TestCreateAddUpdateFail tests failing to create the new update
// in the DB during the create update request
func (suite *UpdateSvcTestSuite) TestCreateAddUpdateFail() {
	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.jobRuntimeOps.EXPECT().
		Get(gomock.Any(), suite.jobID).Return(suite.jobRuntime, nil)

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), suite.jobID, gomock.Any()).
		Return(suite.jobConfig, &models.ConfigAddOn{}, nil)

	suite.cachedJob.EXPECT().
		CreateWorkflow(
			gomock.Any(),
			models.WorkflowType_UPDATE,
			suite.updateConfig,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).
		Return(suite.updateID, nil, fmt.Errorf("fake db error"))

	suite.goalStateDriver.EXPECT().
		EnqueueUpdate(gomock.Any(), gomock.Any(), gomock.Any())

	_, err := suite.h.CreateUpdate(
		context.Background(),
		&svc.CreateUpdateRequest{
			JobId:        suite.jobID,
			JobConfig:    suite.newJobConfig,
			UpdateConfig: suite.updateConfig,
		},
	)
	suite.EqualError(err, "fake db error")
}

// TestGetUpdateNoID tests getting an update info
// with no update-id passed as input
func (suite *UpdateSvcTestSuite) TestGetUpdateNoID() {
	_, err := suite.h.GetUpdate(
		context.Background(),
		&svc.GetUpdateRequest{
			StatusOnly: true,
		})
	suite.True(yarpcerrors.IsInvalidArgument(err))
	suite.EqualError(err,
		"code:invalid-argument message:no update ID provided")
}

// TestGetUpdateSummaryFail tests failing to fetch the update summary from DB
func (suite *UpdateSvcTestSuite) TestGetUpdateSummaryFail() {
	updateModel := &models.UpdateModel{
		State:          update.State_ROLLING_FORWARD,
		InstancesTotal: suite.newJobConfig.InstanceCount,
		InstancesDone:  uint32(5),
	}

	suite.updateStore.EXPECT().
		GetUpdateProgress(gomock.Any(), suite.updateID).
		Return(updateModel, fmt.Errorf("fake db error"))

	_, err := suite.h.GetUpdate(
		context.Background(),
		&svc.GetUpdateRequest{
			UpdateId:   suite.updateID,
			StatusOnly: true,
		})
	suite.EqualError(err, "fake db error")
}

// TestGetUpdateSummary tests fetching the update summary
func (suite *UpdateSvcTestSuite) TestGetUpdateSummary() {
	updateModel := &models.UpdateModel{
		State:          update.State_ROLLING_FORWARD,
		InstancesTotal: suite.newJobConfig.InstanceCount,
		InstancesDone:  uint32(5),
	}

	suite.updateStore.EXPECT().
		GetUpdateProgress(gomock.Any(), suite.updateID).
		Return(updateModel, nil)

	resp, err := suite.h.GetUpdate(
		context.Background(),
		&svc.GetUpdateRequest{
			UpdateId:   suite.updateID,
			StatusOnly: true,
		})

	suite.NoError(err)
	suite.Equal(suite.updateID, resp.GetUpdateInfo().GetUpdateId())
	suite.Equal(updateModel.State, resp.GetUpdateInfo().GetStatus().GetState())
	suite.Equal(updateModel.InstancesDone,
		resp.GetUpdateInfo().GetStatus().GetNumTasksDone())
	suite.Equal(updateModel.InstancesTotal-updateModel.InstancesDone,
		resp.GetUpdateInfo().GetStatus().GetNumTasksRemaining())
}

// TestGetUpdateFail tests failing to fetch the update information from the DB
func (suite *UpdateSvcTestSuite) TestGetUpdateFail() {
	updateModel := &models.UpdateModel{
		JobID:                suite.jobID,
		UpdateConfig:         suite.updateConfig,
		JobConfigVersion:     suite.newJobConfig.ChangeLog.Version,
		PrevJobConfigVersion: suite.jobConfig.ChangeLog.Version,
		State:                update.State_ROLLING_FORWARD,
		InstancesTotal:       suite.newJobConfig.InstanceCount,
		InstancesDone:        uint32(5),
	}

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(updateModel, fmt.Errorf("fake db error"))

	_, err := suite.h.GetUpdate(
		context.Background(),
		&svc.GetUpdateRequest{
			UpdateId:   suite.updateID,
			StatusOnly: false,
		})
	suite.EqualError(err, "fake db error")
}

// TestGetUpdate tests fetching the update information from the DB
func (suite *UpdateSvcTestSuite) TestGetUpdate() {
	opaque := "test"

	updateModel := &models.UpdateModel{
		JobID:                suite.jobID,
		UpdateConfig:         suite.updateConfig,
		JobConfigVersion:     suite.newJobConfig.ChangeLog.Version,
		PrevJobConfigVersion: suite.jobConfig.ChangeLog.Version,
		State:                update.State_ROLLING_FORWARD,
		InstancesTotal:       suite.newJobConfig.InstanceCount,
		InstancesDone:        uint32(5),
		OpaqueData:           &peloton.OpaqueData{Data: opaque},
	}

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(updateModel, nil)

	resp, err := suite.h.GetUpdate(
		context.Background(),
		&svc.GetUpdateRequest{
			UpdateId:   suite.updateID,
			StatusOnly: false,
		})

	suite.NoError(err)
	suite.Equal(suite.updateID, resp.GetUpdateInfo().GetUpdateId())
	suite.Equal(suite.updateConfig, resp.GetUpdateInfo().GetConfig())
	suite.Equal(updateModel.State, resp.GetUpdateInfo().GetStatus().GetState())
	suite.Equal(updateModel.InstancesDone,
		resp.GetUpdateInfo().GetStatus().GetNumTasksDone())
	suite.Equal(updateModel.InstancesTotal-updateModel.InstancesDone,
		resp.GetUpdateInfo().GetStatus().GetNumTasksRemaining())
	suite.Equal(suite.jobID, resp.GetUpdateInfo().GetJobId())
	suite.Equal(suite.newJobConfig.ChangeLog.Version,
		resp.GetUpdateInfo().GetConfigVersion())
	suite.Equal(suite.jobConfig.ChangeLog.Version,
		resp.GetUpdateInfo().GetPrevConfigVersion())
	suite.Equal(opaque, resp.GetUpdateInfo().GetOpaqueData().GetData())
}

// TestGetCacheUpdateNoID tests fetching an update from cache without
// providing an update ID as input
func (suite *UpdateSvcTestSuite) TestGetCacheUpdateNoID() {
	_, err := suite.h.GetUpdateCache(
		context.Background(),
		&svc.GetUpdateCacheRequest{},
	)
	suite.True(yarpcerrors.IsInvalidArgument(err))
	suite.EqualError(err,
		"code:invalid-argument message:no update ID provided")
}

// TestGetCacheUpdateNotFound tests fetching a non-existent update from cache
func (suite *UpdateSvcTestSuite) TestGetCacheUpdateNotFound() {
	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(&models.UpdateModel{
			JobID: suite.jobID,
		}, nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetWorkflow(suite.updateID).
		Return(nil)

	_, err := suite.h.GetUpdateCache(
		context.Background(),
		&svc.GetUpdateCacheRequest{
			UpdateId: suite.updateID,
		},
	)
	suite.True(yarpcerrors.IsNotFound(err))
	suite.EqualError(err,
		"code:not-found message:update not found")
}

// TestGetCacheUpdate tests fetching the update information in the cache
func (suite *UpdateSvcTestSuite) TestGetCacheUpdate() {
	state := update.State_ROLLING_FORWARD
	instancesTotal := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	instancesDone := []uint32{0, 1, 2, 3}
	instancesCurrent := []uint32{4, 5}
	instancesAdded := []uint32{8, 9}
	instancesUpdated := []uint32{0, 1, 2, 3, 4, 5, 6, 7}

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(&models.UpdateModel{
			JobID: suite.jobID,
		}, nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		JobID().
		Return(suite.jobID)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: state,
		})

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances: instancesTotal,
		})

	suite.cachedUpdate.EXPECT().
		GetInstancesDone().
		Return(instancesDone)

	suite.cachedUpdate.EXPECT().
		GetInstancesFailed().
		Return([]uint32{})

	suite.cachedUpdate.EXPECT().
		GetInstancesCurrent().
		Return(instancesCurrent)

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(instancesAdded)

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(instancesUpdated)

	resp, err := suite.h.GetUpdateCache(
		context.Background(),
		&svc.GetUpdateCacheRequest{
			UpdateId: suite.updateID,
		},
	)

	suite.NoError(err)
	suite.Equal(suite.jobID, resp.GetJobId())
	suite.Equal(state, resp.GetState())
	suite.Equal(instancesTotal, resp.GetInstancesTotal())
	suite.Equal(instancesDone, resp.GetInstancesDone())
	suite.Equal(instancesCurrent, resp.GetInstancesCurrent())
	suite.Equal(instancesAdded, resp.GetInstancesAdded())
	suite.Equal(instancesUpdated, resp.GetInstancesUpdated())
	suite.Equal([]uint32{}, resp.GetInstancesFailed())
}

// TestListNoJobID tests fetching all updates for a job
// without providing a job ID as input
func (suite *UpdateSvcTestSuite) TestListNoJobID() {
	_, err := suite.h.ListUpdates(
		context.Background(),
		&svc.ListUpdatesRequest{},
	)

	suite.True(yarpcerrors.IsInvalidArgument(err))
	suite.EqualError(err,
		"code:invalid-argument message:no job ID provided")
}

// TestListGetUpdatesForJobFail tests failing to fetch updates for a job from
// DB while fetching all updates for a job
func (suite *UpdateSvcTestSuite) TestListGetUpdatesForJobFail() {
	suite.updateStore.EXPECT().
		GetUpdatesForJob(gomock.Any(), suite.jobID.GetValue()).
		Return([]*peloton.UpdateID{}, fmt.Errorf("fake db error"))

	_, err := suite.h.ListUpdates(
		context.Background(),
		&svc.ListUpdatesRequest{
			JobID: suite.jobID,
		},
	)
	suite.EqualError(err, "fake db error")
}

// TestListGetUpdateFail tests failing to fetch update information from the
// DB while fetching all updates for a job
func (suite *UpdateSvcTestSuite) TestListGetUpdateFail() {
	updateModel := &models.UpdateModel{
		JobID:                suite.jobID,
		UpdateConfig:         suite.updateConfig,
		JobConfigVersion:     suite.newJobConfig.ChangeLog.Version,
		PrevJobConfigVersion: suite.jobConfig.ChangeLog.Version,
		State:                update.State_ROLLING_FORWARD,
		InstancesTotal:       suite.newJobConfig.InstanceCount,
		InstancesDone:        uint32(5),
	}

	updates := []*peloton.UpdateID{
		{Value: uuid.NewRandom().String()},
		{Value: uuid.NewRandom().String()},
		{Value: uuid.NewRandom().String()},
	}

	suite.updateStore.EXPECT().
		GetUpdatesForJob(gomock.Any(), suite.jobID.GetValue()).
		Return(updates, nil)

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), gomock.Any()).
		Return(updateModel, fmt.Errorf("fake db error"))

	_, err := suite.h.ListUpdates(
		context.Background(),
		&svc.ListUpdatesRequest{
			JobID: suite.jobID,
		},
	)
	suite.EqualError(err, "fake db error")
}

// TestList tests fetching all updates for a job
func (suite *UpdateSvcTestSuite) TestList() {
	updateModel := &models.UpdateModel{
		JobID:                suite.jobID,
		UpdateConfig:         suite.updateConfig,
		JobConfigVersion:     suite.newJobConfig.ChangeLog.Version,
		PrevJobConfigVersion: suite.jobConfig.ChangeLog.Version,
		State:                update.State_ROLLING_FORWARD,
		InstancesTotal:       suite.newJobConfig.InstanceCount,
		InstancesDone:        uint32(5),
	}

	updates := []*peloton.UpdateID{
		{Value: uuid.NewRandom().String()},
		{Value: uuid.NewRandom().String()},
		{Value: uuid.NewRandom().String()},
	}

	suite.updateStore.EXPECT().
		GetUpdatesForJob(gomock.Any(), suite.jobID.GetValue()).
		Return(updates, nil)

	for _, updateID := range updates {
		suite.updateStore.EXPECT().
			GetUpdate(gomock.Any(), updateID).
			Return(updateModel, nil)
	}

	resp, err := suite.h.ListUpdates(
		context.Background(),
		&svc.ListUpdatesRequest{
			JobID: suite.jobID,
		},
	)

	suite.NoError(err)
	for _, update := range resp.GetUpdateInfo() {
		suite.Equal(suite.jobID, update.GetJobId())
		suite.Equal(suite.updateConfig, update.GetConfig())
		suite.Equal(updateModel.State, update.GetStatus().GetState())
		suite.Equal(updateModel.InstancesDone,
			update.GetStatus().GetNumTasksDone())
		suite.Equal(updateModel.InstancesTotal-updateModel.InstancesDone,
			update.GetStatus().GetNumTasksRemaining())
		suite.Equal(suite.newJobConfig.ChangeLog.Version,
			update.GetConfigVersion())
		suite.Equal(suite.jobConfig.ChangeLog.Version,
			update.GetPrevConfigVersion())
	}
}

// TestAbortFail tests getting a DB error while aborting an update
func (suite *UpdateSvcTestSuite) TestAbortFail() {
	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(&models.UpdateModel{
			JobID: suite.jobID,
		}, nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(suite.jobRuntime, nil)

	suite.cachedJob.EXPECT().
		AbortWorkflow(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil, fmt.Errorf("fake db error"))

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID)

	suite.goalStateDriver.EXPECT().
		EnqueueUpdate(suite.jobID, suite.updateID, gomock.Any()).
		Return()

	_, err := suite.h.AbortUpdate(
		context.Background(),
		&svc.AbortUpdateRequest{UpdateId: suite.updateID},
	)
	suite.EqualError(err, "fake db error")
}

// TestAbort tests successfully aborting an update
func (suite *UpdateSvcTestSuite) TestAbort() {
	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(&models.UpdateModel{
			JobID: suite.jobID,
		}, nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(suite.jobRuntime, nil)

	suite.cachedJob.EXPECT().
		AbortWorkflow(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(
			suite.updateID,
			versionutil.GetJobEntityVersion(
				suite.jobRuntime.GetConfigurationVersion(),
				suite.jobRuntime.GetDesiredStateVersion(),
				suite.jobRuntime.GetWorkflowVersion()+1),
			nil)

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID)

	suite.goalStateDriver.EXPECT().
		EnqueueUpdate(suite.jobID, suite.updateID, gomock.Any()).
		Return()

	_, err := suite.h.AbortUpdate(
		context.Background(),
		&svc.AbortUpdateRequest{UpdateId: suite.updateID},
	)
	suite.NoError(err)
}

// TestPauseSuccess tests successfully pauses an update
func (suite *UpdateSvcTestSuite) TestPauseSuccess() {
	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(&models.UpdateModel{
			JobID: suite.jobID,
		}, nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(suite.jobRuntime, nil)

	suite.cachedJob.EXPECT().
		PauseWorkflow(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(
			suite.updateID,
			versionutil.GetJobEntityVersion(
				suite.jobRuntime.GetConfigurationVersion(),
				suite.jobRuntime.GetDesiredStateVersion(),
				suite.jobRuntime.GetWorkflowVersion()+1),
			nil)

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID)

	suite.goalStateDriver.EXPECT().
		EnqueueUpdate(suite.jobID, suite.updateID, gomock.Any()).
		Return()

	_, err := suite.h.PauseUpdate(
		context.Background(),
		&svc.PauseUpdateRequest{UpdateId: suite.updateID},
	)
	suite.NoError(err)
}

// TestPauseProgressUpdateFails fails due to update fails to
// update the state
func (suite *UpdateSvcTestSuite) TestPauseProgressUpdateFails() {
	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(&models.UpdateModel{
			JobID: suite.jobID,
		}, nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(suite.jobRuntime, nil)

	suite.cachedJob.EXPECT().
		PauseWorkflow(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil, fmt.Errorf("test error"))

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID)

	suite.goalStateDriver.EXPECT().
		EnqueueUpdate(suite.jobID, suite.updateID, gomock.Any()).
		Return()

	_, err := suite.h.PauseUpdate(
		context.Background(),
		&svc.PauseUpdateRequest{UpdateId: suite.updateID},
	)
	suite.Error(err)
}

// TestPauseSuccess tests successfully resumes an update
func (suite *UpdateSvcTestSuite) TestResumeSuccess() {
	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(&models.UpdateModel{
			JobID: suite.jobID,
		}, nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(suite.jobRuntime, nil)

	suite.cachedJob.EXPECT().
		ResumeWorkflow(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(
			suite.updateID,
			versionutil.GetJobEntityVersion(
				suite.jobRuntime.GetConfigurationVersion(),
				suite.jobRuntime.GetDesiredStateVersion(),
				suite.jobRuntime.GetWorkflowVersion()+1),
			nil)

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID)

	suite.goalStateDriver.EXPECT().
		EnqueueUpdate(suite.jobID, suite.updateID, gomock.Any()).
		Return()

	_, err := suite.h.ResumeUpdate(
		context.Background(),
		&svc.ResumeUpdateRequest{UpdateId: suite.updateID},
	)
	suite.NoError(err)
}

// TestResumeProgressUpdateFails fails due to update fails to
// update the state
func (suite *UpdateSvcTestSuite) TestResumeProgressUpdateFails() {
	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(&models.UpdateModel{
			JobID: suite.jobID,
		}, nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(suite.jobRuntime, nil)

	suite.cachedJob.EXPECT().
		ResumeWorkflow(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil, fmt.Errorf("test error"))

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID)

	suite.goalStateDriver.EXPECT().
		EnqueueUpdate(suite.jobID, suite.updateID, gomock.Any()).
		Return()

	_, err := suite.h.ResumeUpdate(
		context.Background(),
		&svc.ResumeUpdateRequest{UpdateId: suite.updateID},
	)
	suite.Error(err)
}
