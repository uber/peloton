package stateless

import (
	"context"
	"time"

	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	v1alphapeloton "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"

	"code.uber.internal/infra/peloton/jobmgr/cached"
	jobmgrcommon "code.uber.internal/infra/peloton/jobmgr/common"
	"code.uber.internal/infra/peloton/jobmgr/goalstate"
	handlerutil "code.uber.internal/infra/peloton/jobmgr/util/handler"
	jobutil "code.uber.internal/infra/peloton/jobmgr/util/job"
	"code.uber.internal/infra/peloton/leader"
	"code.uber.internal/infra/peloton/storage"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
)

type serviceHandler struct {
	jobStore        storage.JobStore
	jobFactory      cached.JobFactory
	goalStateDriver goalstate.Driver
	candidate       leader.Candidate
}

// InitV1AlphaJobServiceHandler initializes the Job Manager V1Alpha Service Handler
func InitV1AlphaJobServiceHandler(
	d *yarpc.Dispatcher,
	jobStore storage.JobStore,
	jobFactory cached.JobFactory,
	goalStateDriver goalstate.Driver,
	candidate leader.Candidate,
) {
	handler := &serviceHandler{
		jobStore:        jobStore,
		jobFactory:      jobFactory,
		goalStateDriver: goalStateDriver,
		candidate:       candidate,
	}
	d.Register(svc.BuildJobServiceYARPCProcedures(handler))
}

func (h *serviceHandler) CreateJob(
	ctx context.Context,
	req *svc.CreateJobRequest) (*svc.CreateJobResponse, error) {
	return &svc.CreateJobResponse{}, nil
}

func (h *serviceHandler) ReplaceJob(
	ctx context.Context,
	req *svc.ReplaceJobRequest) (*svc.ReplaceJobResponse, error) {
	return &svc.ReplaceJobResponse{}, nil
}

func (h *serviceHandler) PatchJob(
	ctx context.Context,
	req *svc.PatchJobRequest) (*svc.PatchJobResponse, error) {
	return &svc.PatchJobResponse{}, nil
}

func (h *serviceHandler) RestartJob(
	ctx context.Context,
	req *svc.RestartJobRequest) (*svc.RestartJobResponse, error) {
	return &svc.RestartJobResponse{}, nil
}

func (h *serviceHandler) PauseJobWorkflow(
	ctx context.Context,
	req *svc.PauseJobWorkflowRequest) (*svc.PauseJobWorkflowResponse, error) {
	return &svc.PauseJobWorkflowResponse{}, nil
}

func (h *serviceHandler) ResumeJobWorkflow(
	ctx context.Context,
	req *svc.ResumeJobWorkflowRequest) (*svc.ResumeJobWorkflowResponse, error) {
	return &svc.ResumeJobWorkflowResponse{}, nil
}

func (h *serviceHandler) AbortJobWorkflow(
	ctx context.Context,
	req *svc.AbortJobWorkflowRequest) (*svc.AbortJobWorkflowResponse, error) {
	return &svc.AbortJobWorkflowResponse{}, nil
}

func (h *serviceHandler) StartJob(
	ctx context.Context,
	req *svc.StartJobRequest) (*svc.StartJobResponse, error) {
	return &svc.StartJobResponse{}, nil
}
func (h *serviceHandler) StopJob(
	ctx context.Context,
	req *svc.StopJobRequest) (*svc.StopJobResponse, error) {
	return &svc.StopJobResponse{}, nil
}
func (h *serviceHandler) DeleteJob(
	ctx context.Context,
	req *svc.DeleteJobRequest) (*svc.DeleteJobResponse, error) {
	return &svc.DeleteJobResponse{}, nil
}
func (h *serviceHandler) GetJob(
	ctx context.Context,
	req *svc.GetJobRequest) (*svc.GetJobResponse, error) {
	return &svc.GetJobResponse{}, nil
}
func (h *serviceHandler) ListPods(
	req *svc.ListPodsRequest,
	stream svc.JobServiceServiceListPodsYARPCServer) error {
	return nil
}
func (h *serviceHandler) QueryPods(
	ctx context.Context,
	req *svc.QueryPodsRequest) (*svc.QueryPodsResponse, error) {
	return &svc.QueryPodsResponse{}, nil
}
func (h *serviceHandler) QueryJobs(
	ctx context.Context,
	req *svc.QueryJobsRequest) (*svc.QueryJobsResponse, error) {
	return &svc.QueryJobsResponse{}, nil
}
func (h *serviceHandler) ListJobs(
	req *svc.ListJobsRequest,
	stream svc.JobServiceServiceListJobsYARPCServer) error {
	return nil
}
func (h *serviceHandler) ListJobUpdates(
	ctx context.Context,
	req *svc.ListJobUpdatesRequest) (*svc.ListJobUpdatesResponse, error) {
	return &svc.ListJobUpdatesResponse{}, nil
}
func (h *serviceHandler) RefreshJob(
	ctx context.Context,
	req *svc.RefreshJobRequest) (resp *svc.RefreshJobResponse, err error) {
	defer func() {
		if err != nil {
			log.WithField("request", req).
				WithError(err).
				Warn("JobSVC.RefreshJob failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
			Info("JobSVC.RefreshJob succeeded")
	}()

	if !h.candidate.IsLeader() {
		return nil,
			yarpcerrors.UnavailableErrorf("JobSVC.RefreshJob is not supported on non-leader")
	}

	pelotonJobID := &peloton.JobID{Value: req.GetJobId().GetValue()}

	jobConfig, configAddOn, err := h.jobStore.GetJobConfig(ctx, pelotonJobID)
	if err != nil {
		return nil, errors.Wrap(err, "fail to get job config")
	}

	jobRuntime, err := h.jobStore.GetJobRuntime(ctx, pelotonJobID)
	if err != nil {
		return nil, errors.Wrap(err, "fail to get job runtime")
	}

	cachedJob := h.jobFactory.AddJob(pelotonJobID)
	cachedJob.Update(ctx, &pbjob.JobInfo{
		Config:  jobConfig,
		Runtime: jobRuntime,
	}, configAddOn,
		cached.UpdateCacheOnly)
	h.goalStateDriver.EnqueueJob(pelotonJobID, time.Now())
	return &svc.RefreshJobResponse{}, nil
}

func (h *serviceHandler) GetJobCache(
	ctx context.Context,
	req *svc.GetJobCacheRequest) (resp *svc.GetJobCacheResponse, err error) {
	defer func() {
		if err != nil {
			log.WithField("request", req).
				WithError(err).
				Warn("JobSVC.GetJobCache failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
			Debug("JobSVC.GetJobCache succeeded")
	}()

	cachedJob := h.jobFactory.GetJob(&peloton.JobID{Value: req.GetJobId().GetValue()})
	if cachedJob == nil {
		return nil,
			yarpcerrors.NotFoundErrorf("job not found in cache")
	}

	runtime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "fail to get job runtime")
	}

	config, err := cachedJob.GetConfig(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "fail to get job config")
	}

	var cachedWorkflow cached.Update
	if len(runtime.GetUpdateID().GetValue()) > 0 {
		cachedWorkflow = cachedJob.GetWorkflow(runtime.GetUpdateID())
	}

	return &svc.GetJobCacheResponse{
		Spec:   convertToJobSpec(config),
		Status: convertToJobStatus(runtime, cachedWorkflow),
	}, nil
}

func convertToJobSpec(config jobmgrcommon.JobConfig) *stateless.JobSpec {
	result := &stateless.JobSpec{}
	// set the fields used by both job config and cached job config
	result.InstanceCount = config.GetInstanceCount()
	result.RespoolId = &v1alphapeloton.ResourcePoolID{
		Value: config.GetRespoolID().GetValue(),
	}
	if config.GetSLA() != nil {
		result.Sla = &stateless.SlaSpec{
			Priority:                    config.GetSLA().GetPriority(),
			Preemptible:                 config.GetSLA().GetPreemptible(),
			Revocable:                   config.GetSLA().GetRevocable(),
			MaximumUnavailableInstances: config.GetSLA().GetMaximumUnavailableInstances(),
		}
	}
	result.Revision = &v1alphapeloton.Revision{
		Version:   config.GetChangeLog().GetVersion(),
		CreatedAt: config.GetChangeLog().GetCreatedAt(),
		UpdatedAt: config.GetChangeLog().GetUpdatedAt(),
		UpdatedBy: config.GetChangeLog().GetUpdatedBy(),
	}

	if _, ok := config.(*pbjob.JobConfig); ok {
		// TODO: set the rest of the fields in result
		// if the config passed in is a full config
	}

	return result
}

func convertToJobStatus(
	runtime *pbjob.RuntimeInfo,
	cachedWorkflow cached.Update,
) *stateless.JobStatus {
	result := &stateless.JobStatus{}
	result.Revision = &v1alphapeloton.Revision{
		Version:   runtime.GetRevision().GetVersion(),
		CreatedAt: runtime.GetRevision().GetCreatedAt(),
		UpdatedAt: runtime.GetRevision().GetUpdatedAt(),
		UpdatedBy: runtime.GetRevision().GetUpdatedBy(),
	}
	result.State = stateless.JobState(runtime.GetState())
	result.CreationTime = runtime.GetCreationTime()
	result.PodStats = runtime.TaskStats
	result.DesiredState = stateless.JobState(runtime.GetGoalState())
	result.Version = jobutil.GetEntityVersion(runtime.GetConfigurationVersion())

	if cachedWorkflow == nil {
		return result
	}

	workflowStatus := &stateless.WorkflowStatus{}
	workflowStatus.Type = stateless.WorkflowType(cachedWorkflow.GetWorkflowType())
	workflowStatus.State = stateless.WorkflowState(cachedWorkflow.GetState().State)
	workflowStatus.NumInstancesCompleted = uint32(len(cachedWorkflow.GetInstancesDone()))
	workflowStatus.NumInstancesFailed = uint32(len(cachedWorkflow.GetInstancesFailed()))
	workflowStatus.NumInstancesRemaining =
		uint32(len(cachedWorkflow.GetGoalState().Instances) -
			len(cachedWorkflow.GetInstancesDone()) -
			len(cachedWorkflow.GetInstancesFailed()))
	workflowStatus.CurrentInstances = cachedWorkflow.GetInstancesCurrent()
	workflowStatus.PrevVersion = jobutil.GetEntityVersion(cachedWorkflow.GetState().JobVersion)
	workflowStatus.Version = jobutil.GetEntityVersion(cachedWorkflow.GetGoalState().JobVersion)

	result.WorkflowStatus = workflowStatus
	return result
}
