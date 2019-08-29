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
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/api/v0/update/svc"
	"github.com/uber/peloton/.gen/peloton/private/models"

	"github.com/uber/peloton/pkg/common"
	versionutil "github.com/uber/peloton/pkg/common/util/entityversion"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	"github.com/uber/peloton/pkg/jobmgr/goalstate"
	jobutil "github.com/uber/peloton/pkg/jobmgr/util/job"
	"github.com/uber/peloton/pkg/storage"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"

	"github.com/pborman/uuid"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
)

// InitServiceHandler initalizes the update service.
func InitServiceHandler(
	d *yarpc.Dispatcher,
	parent tally.Scope,
	ormStore *ormobjects.Store,
	updateStore storage.UpdateStore,
	goalStateDriver goalstate.Driver,
	jobFactory cached.JobFactory,
) {
	handler := &serviceHandler{
		jobConfigOps:    ormobjects.NewJobConfigOps(ormStore),
		jobRuntimeOps:   ormobjects.NewJobRuntimeOps(ormStore),
		updateStore:     updateStore,
		goalStateDriver: goalStateDriver,
		jobFactory:      jobFactory,
		metrics:         NewMetrics(parent.SubScope("jobmgr").SubScope("update")),
	}

	d.Register(svc.BuildUpdateServiceYARPCProcedures(handler))
}

// serviceHandler implements peloton.api.update.svc
type serviceHandler struct {
	jobConfigOps    ormobjects.JobConfigOps
	jobRuntimeOps   ormobjects.JobRuntimeOps
	updateStore     storage.UpdateStore
	goalStateDriver goalstate.Driver
	jobFactory      cached.JobFactory
	metrics         *Metrics
}

// validateJobConfigUpdate validates that the job configuration
// update is valid.
func (h *serviceHandler) validateJobConfigUpdate(
	ctx context.Context,
	jobID *peloton.JobID,
	prevJobConfig *job.JobConfig,
	newJobConfig *job.JobConfig) error {

	// Ensure that the changelog is present
	if newJobConfig.GetChangeLog() == nil {
		return yarpcerrors.InvalidArgumentErrorf(
			"missing changelog in job configuration")
	}

	// job type is immutable
	if newJobConfig.GetType() != prevJobConfig.GetType() {
		return yarpcerrors.InvalidArgumentErrorf("job type is immutable")
	}

	// resource pool identifier is immutable
	if newJobConfig.GetRespoolID().GetValue() !=
		prevJobConfig.GetRespoolID().GetValue() {
		return yarpcerrors.InvalidArgumentErrorf(
			"resource pool identifier is immutable")
	}

	return nil
}

// validateJobRuntime validates that the job state allows updating it
func (h *serviceHandler) validateJobRuntime(jobRuntime *job.RuntimeInfo) error {
	// cannot update a job which is still being created
	if jobRuntime.GetState() == job.JobState_INITIALIZED {
		return yarpcerrors.UnavailableErrorf(
			"cannot update partially created job")
	}

	return nil
}

// Create creates an update for a given job ID.
func (h *serviceHandler) CreateUpdate(
	ctx context.Context,
	req *svc.CreateUpdateRequest) (*svc.CreateUpdateResponse, error) {
	h.metrics.UpdateAPICreate.Inc(1)

	jobID := req.GetJobId()
	pelotonJobID := &peloton.JobID{Value: jobID.GetValue()}
	jobUUID := uuid.Parse(jobID.GetValue())
	if jobUUID == nil {
		// job uuid is not a uuid
		h.metrics.UpdateCreateFail.Inc(1)
		return nil, yarpcerrors.InvalidArgumentErrorf(
			"JobID must be of UUID format")
	}

	if req.GetUpdateConfig().GetInPlace() {
		return nil, yarpcerrors.UnimplementedErrorf("in-place update is not supported yet")
	}

	// Validate that the job does exist
	jobRuntime, err := h.jobRuntimeOps.Get(ctx, pelotonJobID)
	if err != nil {
		h.metrics.UpdateCreateFail.Inc(1)
		return nil, yarpcerrors.NotFoundErrorf("job not found")
	}

	// validate the job is in a state where it can be updated
	if err := h.validateJobRuntime(jobRuntime); err != nil {
		h.metrics.UpdateCreateFail.Inc(1)
		return nil, err
	}

	jobConfig := req.GetJobConfig()

	// Get previous job configuration
	prevJobConfig, prevConfigAddOn, err := h.jobConfigOps.Get(
		ctx,
		pelotonJobID,
		jobRuntime.GetConfigurationVersion(),
	)
	if err != nil {
		h.metrics.UpdateCreateFail.Inc(1)
		return nil, err
	}

	// check that job type is service
	if prevJobConfig.GetType() != job.JobType_SERVICE {
		h.metrics.UpdateCreateFail.Inc(1)
		return nil, yarpcerrors.InvalidArgumentErrorf(
			"job must be of type service")
	}

	// validate the new configuration
	if err = h.validateJobConfigUpdate(
		ctx, jobID, prevJobConfig, jobConfig); err != nil {
		h.metrics.UpdateCreateFail.Inc(1)
		return nil, err
	}

	var respoolPath string
	for _, label := range prevConfigAddOn.GetSystemLabels() {
		if label.GetKey() == common.SystemLabelResourcePool {
			respoolPath = label.GetValue()
		}
	}
	configAddOn := &models.ConfigAddOn{
		SystemLabels: jobutil.ConstructSystemLabels(jobConfig, respoolPath),
	}
	// add this new update to cache and DB
	cachedJob := h.jobFactory.AddJob(jobID)
	updateID, _, err := cachedJob.CreateWorkflow(
		ctx,
		models.WorkflowType_UPDATE,
		req.GetUpdateConfig(),
		versionutil.GetJobEntityVersion(
			jobRuntime.GetConfigurationVersion(),
			jobRuntime.GetDesiredStateVersion(),
			jobRuntime.GetWorkflowVersion()),
		cached.WithConfig(jobConfig, prevJobConfig, configAddOn, nil),
		cached.WithOpaqueData(req.GetOpaqueData()),
	)
	if err != nil {
		// In case of error, since it is not clear if job runtime was
		// persisted with the update ID or not, enqueue the update to
		// the goal state. If the update ID got persisted, update should
		// start running, else, it should be aborted. Enqueueing it into
		// the goal state will ensure both. In case the update was not
		// persisted, clear the cache as well so that it is reloaded
		// from DB and cleaned up.
		h.metrics.UpdateCreateFail.Inc(1)
	}

	// Add update to goal state engine to start it
	if len(updateID.GetValue()) > 0 {
		h.goalStateDriver.EnqueueUpdate(jobID, updateID, time.Now())
	}
	return &svc.CreateUpdateResponse{
		UpdateID: updateID,
	}, err
}

func (h *serviceHandler) GetUpdate(ctx context.Context, req *svc.GetUpdateRequest) (*svc.GetUpdateResponse, error) {
	h.metrics.UpdateAPIGet.Inc(1)
	updateID := req.GetUpdateId()
	if updateID == nil {
		h.metrics.UpdateGetFail.Inc(1)
		return nil, yarpcerrors.InvalidArgumentErrorf("no update ID provided")
	}

	if req.GetStatusOnly() {
		// get only the status
		updateModel, err := h.updateStore.GetUpdateProgress(ctx, updateID)
		if err != nil {
			h.metrics.UpdateGetFail.Inc(1)
			return nil, err
		}

		updateInfo := &update.UpdateInfo{
			UpdateId: updateID,
			Status: &update.UpdateStatus{
				NumTasksDone: updateModel.GetInstancesDone(),
				NumTasksRemaining: updateModel.GetInstancesTotal() -
					updateModel.GetInstancesDone() -
					updateModel.GetInstancesFailed(),
				NumTasksFailed: updateModel.GetInstancesFailed(),
				State:          updateModel.GetState(),
			},
		}

		h.metrics.UpdateGet.Inc(1)
		return &svc.GetUpdateResponse{
			UpdateInfo: updateInfo,
		}, nil
	}

	updateModel, err := h.updateStore.GetUpdate(ctx, updateID)
	if err != nil {
		h.metrics.UpdateGetFail.Inc(1)
		return nil, err
	}

	updateInfo := &update.UpdateInfo{
		UpdateId:          updateID,
		Config:            updateModel.GetUpdateConfig(),
		JobId:             updateModel.GetJobID(),
		ConfigVersion:     updateModel.GetJobConfigVersion(),
		PrevConfigVersion: updateModel.GetPrevJobConfigVersion(),
		OpaqueData:        updateModel.GetOpaqueData(),
		Status: &update.UpdateStatus{
			NumTasksDone: updateModel.GetInstancesDone(),
			NumTasksRemaining: updateModel.GetInstancesTotal() -
				updateModel.GetInstancesDone() -
				updateModel.GetInstancesFailed(),
			NumTasksFailed: updateModel.GetInstancesFailed(),
			State:          updateModel.GetState(),
		},
	}

	h.metrics.UpdateGet.Inc(1)
	return &svc.GetUpdateResponse{
		UpdateInfo: updateInfo,
	}, nil
}

func (h *serviceHandler) GetUpdateCache(ctx context.Context,
	req *svc.GetUpdateCacheRequest) (*svc.GetUpdateCacheResponse, error) {
	h.metrics.UpdateAPIGetCache.Inc(1)

	cachedJob, err := h.getCachedJobWithUpdateID(ctx, req.GetUpdateId())
	if err != nil {
		return nil, err
	}

	workflow := cachedJob.GetWorkflow(req.GetUpdateId())
	if workflow == nil {
		return nil, yarpcerrors.NotFoundErrorf("update not found")
	}

	h.metrics.UpdateGetCache.Inc(1)

	return &svc.GetUpdateCacheResponse{
		JobId:            workflow.JobID(),
		State:            workflow.GetState().State,
		InstancesTotal:   workflow.GetGoalState().Instances,
		InstancesDone:    workflow.GetInstancesDone(),
		InstancesCurrent: workflow.GetInstancesCurrent(),
		InstancesAdded:   workflow.GetInstancesAdded(),
		InstancesUpdated: workflow.GetInstancesUpdated(),
		InstancesFailed:  workflow.GetInstancesFailed(),
	}, nil
}

func (h *serviceHandler) PauseUpdate(
	ctx context.Context,
	req *svc.PauseUpdateRequest,
) (*svc.PauseUpdateResponse, error) {
	var err error

	h.metrics.UpdateAPIPause.Inc(1)

	cachedJob, err := h.getCachedJobWithUpdateID(ctx, req.GetUpdateId())
	if err != nil {
		h.metrics.UpdatePauseFail.Inc(1)
		return nil, err
	}

	runtime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		h.metrics.UpdatePauseFail.Inc(1)
		return nil, err
	}

	if _, _, err = cachedJob.PauseWorkflow(
		ctx,
		versionutil.GetJobEntityVersion(
			runtime.GetConfigurationVersion(),
			runtime.GetDesiredStateVersion(),
			runtime.GetWorkflowVersion()),
		cached.WithOpaqueData(req.GetOpaqueData()),
	); err != nil {
		// In case of error, since it is not clear if job runtime was
		// updated or not, enqueue the update to the goal state.
		h.metrics.UpdatePauseFail.Inc(1)
	} else {
		h.metrics.UpdatePause.Inc(1)
	}

	h.goalStateDriver.EnqueueUpdate(cachedJob.ID(), req.GetUpdateId(), time.Now())

	return &svc.PauseUpdateResponse{}, err
}

func (h *serviceHandler) ResumeUpdate(
	ctx context.Context,
	req *svc.ResumeUpdateRequest,
) (*svc.ResumeUpdateResponse, error) {
	var err error

	h.metrics.UpdateAPIResume.Inc(1)

	cachedJob, err := h.getCachedJobWithUpdateID(ctx, req.GetUpdateId())
	if err != nil {
		h.metrics.UpdateResumeFail.Inc(1)
		return nil, err
	}

	runtime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		h.metrics.UpdateResumeFail.Inc(1)
		return nil, err
	}

	if _, _, err = cachedJob.ResumeWorkflow(
		ctx,
		versionutil.GetJobEntityVersion(
			runtime.GetConfigurationVersion(),
			runtime.GetDesiredStateVersion(),
			runtime.GetWorkflowVersion()),
		cached.WithOpaqueData(req.GetOpaqueData()),
	); err != nil {
		// In case of error, since it is not clear if job runtime was
		// updated or not, enqueue the update to the goal state.
		h.metrics.UpdateResumeFail.Inc(1)
	} else {
		h.metrics.UpdateResume.Inc(1)
	}
	h.goalStateDriver.EnqueueUpdate(cachedJob.ID(), req.GetUpdateId(), time.Now())

	return &svc.ResumeUpdateResponse{}, err
}

func (h *serviceHandler) ListUpdates(ctx context.Context,
	req *svc.ListUpdatesRequest) (*svc.ListUpdatesResponse, error) {
	var updates []*update.UpdateInfo

	h.metrics.UpdateAPIList.Inc(1)
	jobID := req.GetJobID()
	if jobID == nil {
		h.metrics.UpdateListFail.Inc(1)
		return nil, yarpcerrors.InvalidArgumentErrorf("no job ID provided")
	}

	updateIDs, err := h.updateStore.GetUpdatesForJob(ctx, jobID.GetValue())
	if err != nil {
		h.metrics.UpdateListFail.Inc(1)
		return nil, err
	}

	for _, updateID := range updateIDs {
		updateModel, err := h.updateStore.GetUpdate(ctx, updateID)
		if err != nil {
			h.metrics.UpdateListFail.Inc(1)
			return nil, err
		}

		updateInfo := &update.UpdateInfo{
			UpdateId:          updateID,
			Config:            updateModel.GetUpdateConfig(),
			JobId:             updateModel.GetJobID(),
			ConfigVersion:     updateModel.GetJobConfigVersion(),
			PrevConfigVersion: updateModel.GetPrevJobConfigVersion(),
			Status: &update.UpdateStatus{
				NumTasksDone: updateModel.GetInstancesDone(),
				NumTasksRemaining: updateModel.GetInstancesTotal() -
					updateModel.GetInstancesDone() -
					updateModel.GetInstancesFailed(),
				NumTasksFailed: updateModel.GetInstancesFailed(),
				State:          updateModel.GetState(),
			},
		}

		updates = append(updates, updateInfo)
	}

	h.metrics.UpdateList.Inc(1)
	return &svc.ListUpdatesResponse{
		UpdateInfo: updates,
	}, nil
}

func (h *serviceHandler) AbortUpdate(ctx context.Context,
	req *svc.AbortUpdateRequest) (*svc.AbortUpdateResponse, error) {
	h.metrics.UpdateAPIAbort.Inc(1)
	cachedJob, err := h.getCachedJobWithUpdateID(ctx, req.GetUpdateId())
	// TODO: what if the workflow in job is not what is intended to be aborted
	if err != nil {
		h.metrics.UpdateAbortFail.Inc(1)
		return nil, err
	}

	runtime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		h.metrics.UpdatePauseFail.Inc(1)
		return nil, err
	}

	if _, _, err = cachedJob.AbortWorkflow(
		ctx,
		versionutil.GetJobEntityVersion(
			runtime.GetConfigurationVersion(),
			runtime.GetDesiredStateVersion(),
			runtime.GetWorkflowVersion()),
		cached.WithOpaqueData(req.GetOpaqueData()),
	); err != nil {
		h.metrics.UpdateAbortFail.Inc(1)
	} else {
		h.metrics.UpdateAbort.Inc(1)
	}
	h.goalStateDriver.EnqueueUpdate(cachedJob.ID(), req.GetUpdateId(), time.Now())
	return &svc.AbortUpdateResponse{}, err
}

func (h *serviceHandler) RollbackUpdate(ctx context.Context,
	req *svc.RollbackUpdateRequest) (*svc.RollbackUpdateResponse, error) {
	return nil, yarpcerrors.UnimplementedErrorf(
		"UpdateService.RollbackUpdate is not implemented")
}

func (h *serviceHandler) getCachedJobWithUpdateID(
	ctx context.Context,
	updateID *peloton.UpdateID,
) (cached.Job, error) {
	if len(updateID.GetValue()) == 0 {
		return nil, yarpcerrors.InvalidArgumentErrorf("no update ID provided")
	}

	updateModel, err := h.updateStore.GetUpdate(ctx, updateID)
	if err != nil {
		return nil, err
	}

	return h.jobFactory.AddJob(updateModel.GetJobID()), nil
}

// NewTestServiceHandler returns an empty new ServiceHandler ptr for testing.
func NewTestServiceHandler() *serviceHandler {
	return &serviceHandler{}
}
