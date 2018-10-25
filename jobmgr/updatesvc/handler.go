package updatesvc

import (
	"context"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/update/svc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/models"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	"code.uber.internal/infra/peloton/jobmgr/goalstate"
	jobutil "code.uber.internal/infra/peloton/jobmgr/util/job"
	"code.uber.internal/infra/peloton/storage"

	"github.com/pborman/uuid"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
)

// InitServiceHandler initalizes the update service.
func InitServiceHandler(
	d *yarpc.Dispatcher,
	parent tally.Scope,
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	updateStore storage.UpdateStore,
	goalStateDriver goalstate.Driver,
	jobFactory cached.JobFactory,
	updateFactory cached.UpdateFactory) {
	handler := &serviceHandler{
		jobStore:        jobStore,
		taskStore:       taskStore,
		updateStore:     updateStore,
		goalStateDriver: goalStateDriver,
		jobFactory:      jobFactory,
		updateFactory:   updateFactory,
		metrics:         NewMetrics(parent.SubScope("jobmgr").SubScope("update")),
	}

	d.Register(svc.BuildUpdateServiceYARPCProcedures(handler))
}

// serviceHandler implements peloton.api.update.svc
type serviceHandler struct {
	jobStore        storage.JobStore
	taskStore       storage.TaskStore
	updateStore     storage.UpdateStore
	goalStateDriver goalstate.Driver
	jobFactory      cached.JobFactory
	updateFactory   cached.UpdateFactory
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
	jobUUID := uuid.Parse(jobID.GetValue())
	if jobUUID == nil {
		// job uuid is not a uuid
		h.metrics.UpdateCreateFail.Inc(1)
		return nil, yarpcerrors.InvalidArgumentErrorf(
			"JobID must be of UUID format")
	}

	// Validate that the job does exist
	jobRuntime, err := h.jobStore.GetJobRuntime(ctx, jobID)
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
	prevJobConfig, prevConfigAddOn, err := h.jobStore.GetJobConfig(ctx, jobID)
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

	id := &peloton.UpdateID{
		Value: uuid.New(),
	}

	cachedJob := h.jobFactory.AddJob(jobID)
	instancesAdded, instancesUpdated, instancesRemoved, err := cached.GetInstancesToProcessForUpdate(
		ctx,
		cachedJob,
		prevJobConfig,
		jobConfig,
		h.taskStore,
	)
	if err != nil {
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
	cachedUpdate := h.updateFactory.AddUpdate(id)
	err = cachedUpdate.Create(
		ctx,
		jobID,
		jobConfig,
		prevJobConfig,
		configAddOn,
		instancesAdded,
		instancesUpdated,
		instancesRemoved,
		models.WorkflowType_UPDATE,
		req.GetUpdateConfig(),
	)
	if err != nil {
		// In case of error, since it is not clear if job runtime was
		// persisted with the update ID or not, enqueue the update to
		// the goal state. If the update ID got persisted, update should
		// start running, else, it should be aborted. Enqueueing it into
		// the goal state will ensure both. In case the update was not
		// persisted, clear the cache as well so that it is reloaded
		// from DB and cleaned up.
		h.updateFactory.ClearUpdate(id)
		h.metrics.UpdateCreateFail.Inc(1)
	}

	// Add update to goal state engine to start it
	h.goalStateDriver.EnqueueUpdate(jobID, id, time.Now())
	return &svc.CreateUpdateResponse{
		UpdateID: id,
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
	u, err := h.getCachedUpdate(req.GetUpdateId())
	if err != nil {
		return nil, err
	}

	h.metrics.UpdateGetCache.Inc(1)

	return &svc.GetUpdateCacheResponse{
		JobId:            u.JobID(),
		State:            u.GetState().State,
		InstancesTotal:   u.GetGoalState().Instances,
		InstancesDone:    u.GetInstancesDone(),
		InstancesCurrent: u.GetInstancesCurrent(),
		InstancesAdded:   u.GetInstancesAdded(),
		InstancesUpdated: u.GetInstancesUpdated(),
		InstancesFailed:  u.GetInstancesFailed(),
	}, nil
}

func (h *serviceHandler) PauseUpdate(
	ctx context.Context,
	req *svc.PauseUpdateRequest,
) (*svc.PauseUpdateResponse, error) {
	var err error

	h.metrics.UpdateAPIPause.Inc(1)

	cachedUpdate := h.updateFactory.AddUpdate(req.GetUpdateId())
	if err := cachedUpdate.Recover(ctx); err != nil {
		h.metrics.UpdatePauseFail.Inc(1)
		return nil, err
	}

	if err = cachedUpdate.Pause(ctx); err != nil {
		// In case of error, since it is not clear if job runtime was
		// updated or not, enqueue the update to the goal state.
		h.metrics.UpdatePauseFail.Inc(1)
	}
	h.goalStateDriver.EnqueueUpdate(cachedUpdate.JobID(), cachedUpdate.ID(), time.Now())

	h.metrics.UpdatePause.Inc(1)
	return &svc.PauseUpdateResponse{}, err
}

func (h *serviceHandler) ResumeUpdate(
	ctx context.Context,
	req *svc.ResumeUpdateRequest,
) (*svc.ResumeUpdateResponse, error) {
	var err error

	h.metrics.UpdateAPIResume.Inc(1)

	cachedUpdate := h.updateFactory.AddUpdate(req.GetUpdateId())
	if err := cachedUpdate.Recover(ctx); err != nil {
		h.metrics.UpdateResumeFail.Inc(1)
		return nil, err
	}

	if err = cachedUpdate.Resume(ctx); err != nil {
		// In case of error, since it is not clear if job runtime was
		// updated or not, enqueue the update to the goal state.
		h.metrics.UpdateResumeFail.Inc(1)
	}
	h.goalStateDriver.EnqueueUpdate(cachedUpdate.JobID(), cachedUpdate.ID(), time.Now())

	h.metrics.UpdateResume.Inc(1)
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

	updateIDs, err := h.updateStore.GetUpdatesForJob(ctx, jobID)
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
	updateID := req.GetUpdateId()

	err := cached.AbortJobUpdate(
		ctx,
		updateID,
		h.updateStore,
		h.updateFactory,
	)
	if err != nil {
		h.metrics.UpdateAbortFail.Inc(1)
	} else {
		h.metrics.UpdateAbort.Inc(1)
	}

	cachedUpdate := h.updateFactory.GetUpdate(updateID)
	h.goalStateDriver.EnqueueUpdate(cachedUpdate.JobID(), updateID, time.Now())
	return &svc.AbortUpdateResponse{}, err
}

func (h *serviceHandler) RollbackUpdate(ctx context.Context,
	req *svc.RollbackUpdateRequest) (*svc.RollbackUpdateResponse, error) {
	return nil, yarpcerrors.UnimplementedErrorf(
		"UpdateService.RollbackUpdate is not implemented")
}

func (h *serviceHandler) getCachedUpdate(updateID *peloton.UpdateID) (cached.Update, error) {
	if updateID == nil {
		h.metrics.UpdateGetCacheFail.Inc(1)
		return nil, yarpcerrors.InvalidArgumentErrorf("no update ID provided")
	}

	u := h.updateFactory.GetUpdate(updateID)
	if u == nil {
		h.metrics.UpdateGetCacheFail.Inc(1)
		return nil, yarpcerrors.NotFoundErrorf("update not found")
	}
	return u, nil
}
