package aurorabridge

import (
	"context"
	"errors"

	statelesssvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/thrift/aurora/api"
	"github.com/uber/peloton/aurorabridge/atop"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/thriftrw/ptr"
	"go.uber.org/yarpc/yarpcerrors"
)

var errUnimplemented = errors.New("rpc is unimplemented")

// ServiceHandler implements a partial Aurora API. Various unneeded methods have
// been left intentionally unimplemented.
type ServiceHandler struct {
	metrics   *Metrics
	jobClient statelesssvc.JobServiceYARPCClient
	respoolID *peloton.ResourcePoolID
}

// NewServiceHandler creates a new ServiceHandler.
func NewServiceHandler(
	parent tally.Scope,
	jobClient statelesssvc.JobServiceYARPCClient,
	respoolID *peloton.ResourcePoolID,
) *ServiceHandler {
	return &ServiceHandler{
		metrics:   NewMetrics(parent.SubScope("aurorabridge").SubScope("api")),
		jobClient: jobClient,
		respoolID: respoolID,
	}
}

// GetJobSummary returns a summary of jobs, optionally only those owned by a specific role.
func (h *ServiceHandler) GetJobSummary(
	ctx context.Context,
	role *string) (*api.Response, error) {
	return nil, errUnimplemented
}

// GetTasksWithoutConfigs is the same as getTaskStatus but without the TaskConfig.ExecutorConfig
// data set.
func (h *ServiceHandler) GetTasksWithoutConfigs(
	ctx context.Context,
	query *api.TaskQuery) (*api.Response, error) {
	return nil, errUnimplemented
}

// GetConfigSummary fetches the configuration summary of active tasks for the specified job.
func (h *ServiceHandler) GetConfigSummary(
	ctx context.Context,
	job *api.JobKey) (*api.Response, error) {
	return nil, errUnimplemented
}

// GetJobs fetches the status of jobs. ownerRole is optional, in which case all jobs are returned.
func (h *ServiceHandler) GetJobs(
	ctx context.Context,
	ownerRole *string) (*api.Response, error) {
	return nil, errUnimplemented
}

// GetJobUpdateSummaries gets job update summaries.
func (h *ServiceHandler) GetJobUpdateSummaries(
	ctx context.Context,
	jobUpdateQuery *api.JobUpdateQuery) (*api.Response, error) {

	return nil, errUnimplemented
}

// GetJobUpdateDetails gets job update details.
func (h *ServiceHandler) GetJobUpdateDetails(
	ctx context.Context,
	key *api.JobUpdateKey,
	query *api.JobUpdateQuery) (*api.Response, error) {

	return nil, errUnimplemented
}

// GetJobUpdateDiff gets the diff between client (desired) and server (current) job states.
func (h *ServiceHandler) GetJobUpdateDiff(
	ctx context.Context,
	request *api.JobUpdateRequest) (*api.Response, error) {
	return nil, errUnimplemented
}

// GetTierConfigs is a no-op. It is only used to determine liveness of the scheduler.
func (h *ServiceHandler) GetTierConfigs(
	ctx context.Context) (*api.Response, error) {
	return nil, errUnimplemented
}

// KillTasks initiates a kill on tasks.
func (h *ServiceHandler) KillTasks(
	ctx context.Context,
	job *api.JobKey,
	instances map[int32]struct{},
	message *string) (*api.Response, error) {

	return nil, errUnimplemented
}

// StartJobUpdate starts update of the existing service job.
func (h *ServiceHandler) StartJobUpdate(
	ctx context.Context,
	request *api.JobUpdateRequest,
	message *string,
) (*api.Response, error) {

	result, err := h.startJobUpdate(ctx, request, message)
	if err != nil {
		log.WithFields(log.Fields{
			"params": log.Fields{
				"request": request,
				"message": message,
			},
			"code":  err.responseCode,
			"error": err.msg,
		}).Error("StartJobUpdate error")
	}
	return newResponse(result, err), nil
}

func (h *ServiceHandler) startJobUpdate(
	ctx context.Context,
	request *api.JobUpdateRequest,
	message *string,
) (*api.Result, *auroraError) {

	jobKey := request.GetTaskConfig().GetJob()

	jobSpec, err := atop.NewJobSpecFromJobUpdateRequest(request, h.respoolID)
	if err != nil {
		return nil, auroraErrorf("new job spec: %s", err)
	}

	// TODO(codyg): We'll use the new job's entity version as the update id.
	// Not sure if this will work.
	var newVersion *peloton.EntityVersion

	id, err := h.getJobID(ctx, jobKey)
	if err != nil {
		if yarpcerrors.IsNotFound(err) {
			// Job does not exist. Create it.
			req := &statelesssvc.CreateJobRequest{
				Spec: jobSpec,
			}
			resp, err := h.jobClient.CreateJob(ctx, req)
			if err != nil {
				if yarpcerrors.IsAlreadyExists(err) {
					// Upgrade conflict.
					return nil, auroraErrorf(
						"create job: %s", err).
						code(api.ResponseCodeInvalidRequest)
				}
				return nil, auroraErrorf("create job: %s", err)
			}
			newVersion = resp.GetVersion()
		} else {
			return nil, auroraErrorf("get job id: %s", err)
		}
	} else {
		// Job already exists. Replace it.
		v, err := h.getCurrentJobVersion(ctx, id)
		if err != nil {
			return nil, auroraErrorf("get current job version: %s", err)
		}
		req := &statelesssvc.ReplaceJobRequest{
			JobId:      id,
			Spec:       jobSpec,
			UpdateSpec: atop.NewUpdateSpec(request.GetSettings()),
			Version:    v,
		}
		resp, err := h.jobClient.ReplaceJob(ctx, req)
		if err != nil {
			if yarpcerrors.IsAborted(err) {
				// Upgrade conflict.
				return nil, auroraErrorf(
					"replace job: %s", err).
					code(api.ResponseCodeInvalidRequest)
			}
			return nil, auroraErrorf("replace job: %s", err)
		}
		newVersion = resp.GetVersion()
	}

	return &api.Result{
		StartJobUpdateResult: &api.StartJobUpdateResult{
			Key: &api.JobUpdateKey{
				Job: jobKey,
				ID:  ptr.String(newVersion.String()),
			},
			UpdateSummary: nil, // TODO(codyg): Should we set this?
		},
	}, nil
}

// PauseJobUpdate pauses the specified job update. Can be resumed by resumeUpdate call.
func (h *ServiceHandler) PauseJobUpdate(
	ctx context.Context,
	key *api.JobUpdateKey,
	message *string,
) (*api.Response, error) {

	result, err := h.pauseJobUpdate(ctx, key, message)
	if err != nil {
		log.WithFields(log.Fields{
			"params": log.Fields{
				"key":     key,
				"message": message,
			},
			"code":  err.responseCode,
			"error": err.msg,
		}).Error("PauseJobUpdate error")
	}
	return newResponse(result, err), nil
}

func (h *ServiceHandler) pauseJobUpdate(
	ctx context.Context,
	key *api.JobUpdateKey,
	message *string,
) (*api.Result, *auroraError) {

	id, err := h.getJobID(ctx, key.GetJob())
	if err != nil {
		return nil, auroraErrorf("get job id: %s", err)
	}
	v, err := h.getCurrentJobVersion(ctx, id)
	if err != nil {
		return nil, auroraErrorf("get current job version: %s", err)
	}
	req := &statelesssvc.PauseJobWorkflowRequest{
		JobId:   id,
		Version: v,
	}
	if _, err := h.jobClient.PauseJobWorkflow(ctx, req); err != nil {
		return nil, auroraErrorf("pause job workflow: %s", err)
	}
	return &api.Result{}, nil
}

// ResumeJobUpdate resumes progress of a previously paused job update.
func (h *ServiceHandler) ResumeJobUpdate(
	ctx context.Context,
	key *api.JobUpdateKey,
	message *string,
) (*api.Response, error) {

	result, err := h.resumeJobUpdate(ctx, key, message)
	if err != nil {
		log.WithFields(log.Fields{
			"params": log.Fields{
				"key":     key,
				"message": message,
			},
			"code":  err.responseCode,
			"error": err.msg,
		}).Error("ResumeJobUpdate error")
	}
	return newResponse(result, err), nil
}

func (h *ServiceHandler) resumeJobUpdate(
	ctx context.Context,
	key *api.JobUpdateKey,
	message *string,
) (*api.Result, *auroraError) {

	id, err := h.getJobID(ctx, key.GetJob())
	if err != nil {
		return nil, auroraErrorf("get job id: %s", err)
	}
	v, err := h.getCurrentJobVersion(ctx, id)
	if err != nil {
		return nil, auroraErrorf("get current job version: %s", err)
	}
	req := &statelesssvc.ResumeJobWorkflowRequest{
		JobId:   id,
		Version: v,
	}
	if _, err := h.jobClient.ResumeJobWorkflow(ctx, req); err != nil {
		return nil, auroraErrorf("resume job workflow: %s", err)
	}
	return &api.Result{}, nil
}

// AbortJobUpdate permanently aborts the job update. Does not remove the update history.
func (h *ServiceHandler) AbortJobUpdate(
	ctx context.Context,
	key *api.JobUpdateKey,
	message *string,
) (*api.Response, error) {

	result, err := h.abortJobUpdate(ctx, key, message)
	if err != nil {
		log.WithFields(log.Fields{
			"params": log.Fields{
				"key":     key,
				"message": message,
			},
			"code":  err.responseCode,
			"error": err.msg,
		}).Error("AbortJobUpdate error")
	}
	return newResponse(result, err), nil
}

func (h *ServiceHandler) abortJobUpdate(
	ctx context.Context,
	key *api.JobUpdateKey,
	message *string,
) (*api.Result, *auroraError) {

	id, err := h.getJobID(ctx, key.GetJob())
	if err != nil {
		return nil, auroraErrorf("get job id: %s", err)
	}
	v, err := h.getCurrentJobVersion(ctx, id)
	if err != nil {
		return nil, auroraErrorf("get current job version: %s", err)
	}
	req := &statelesssvc.AbortJobWorkflowRequest{
		JobId:   id,
		Version: v,
	}
	if _, err := h.jobClient.AbortJobWorkflow(ctx, req); err != nil {
		return nil, auroraErrorf("abort job workflow: %s", err)
	}
	return &api.Result{}, nil
}

// RollbackJobUpdate rollbacks the specified active job update to the initial state.
func (h *ServiceHandler) RollbackJobUpdate(
	ctx context.Context,
	key *api.JobUpdateKey,
	message *string) (*api.Response, error) {

	return nil, errUnimplemented
}

// PulseJobUpdate allows progress of the job update in case blockIfNoPulsesAfterMs is specified in
// JobUpdateSettings. Unblocks progress if the update was previously blocked.
// Responds with ResponseCode.INVALID_REQUEST in case an unknown update key is specified.
func (h *ServiceHandler) PulseJobUpdate(
	ctx context.Context,
	key *api.JobUpdateKey) (*api.Response, error) {
	return nil, errUnimplemented
}

func (h *ServiceHandler) getJobID(
	ctx context.Context,
	k *api.JobKey,
) (*peloton.JobID, error) {

	req := &statelesssvc.GetJobIDFromJobNameRequest{
		JobName: atop.NewJobName(k),
	}
	resp, err := h.jobClient.GetJobIDFromJobName(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.GetJobId()[0], nil // Return the latest id.
}

func (h *ServiceHandler) getCurrentJobVersion(
	ctx context.Context,
	id *peloton.JobID,
) (*peloton.EntityVersion, error) {

	req := &statelesssvc.GetJobRequest{
		SummaryOnly: true,
		JobId:       id,
	}
	resp, err := h.jobClient.GetJob(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.GetSummary().GetStatus().GetVersion(), nil
}
