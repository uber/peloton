package stateless

import (
	"context"
	"time"

	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	v1alphapeloton "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"
	pelotonv1alphaquery "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/query"
	"code.uber.internal/infra/peloton/.gen/peloton/private/models"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	jobmgrcommon "code.uber.internal/infra/peloton/jobmgr/common"
	"code.uber.internal/infra/peloton/jobmgr/goalstate"
	jobmgrtask "code.uber.internal/infra/peloton/jobmgr/task"
	handlerutil "code.uber.internal/infra/peloton/jobmgr/util/handler"
	jobutil "code.uber.internal/infra/peloton/jobmgr/util/job"
	"code.uber.internal/infra/peloton/leader"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"

	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
)

type serviceHandler struct {
	jobStore        storage.JobStore
	updateStore     storage.UpdateStore
	respoolClient   respool.ResourceManagerYARPCClient
	jobFactory      cached.JobFactory
	goalStateDriver goalstate.Driver
	candidate       leader.Candidate
}

// InitV1AlphaJobServiceHandler initializes the Job Manager V1Alpha Service Handler
func InitV1AlphaJobServiceHandler(
	d *yarpc.Dispatcher,
	jobStore storage.JobStore,
	updateStore storage.UpdateStore,
	jobFactory cached.JobFactory,
	goalStateDriver goalstate.Driver,
	candidate leader.Candidate,
) {
	handler := &serviceHandler{
		jobStore:        jobStore,
		updateStore:     updateStore,
		respoolClient:   respool.NewResourceManagerYARPCClient(d.ClientConfig(common.PelotonResourceManager)),
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
	req *svc.ReplaceJobRequest) (resp *svc.ReplaceJobResponse, err error) {
	defer func() {
		jobID := req.GetJobId().GetValue()
		specVersion := req.GetSpec().GetRevision().GetVersion()
		entityVersion := req.GetVersion().GetValue()

		if err != nil {
			log.WithField("job_id", jobID).
				WithField("spec_version", specVersion).
				WithField("entity_version", entityVersion).
				WithError(err).
				Warn("JobSVC.ReplaceJob failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("job_id", jobID).
			WithField("spec_version", specVersion).
			WithField("entity_version", entityVersion).
			WithField("response", resp).
			Info("JobSVC.ReplaceJob succeeded")
	}()

	// TODO: handle secretes
	jobUUID := uuid.Parse(req.GetJobId().GetValue())
	if jobUUID == nil {
		return nil, yarpcerrors.InvalidArgumentErrorf(
			"JobID must be of UUID format")
	}

	jobID := &peloton.JobID{Value: req.GetJobId().GetValue()}

	cachedJob := h.jobFactory.AddJob(jobID)
	jobRuntime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		return nil, err
	}

	// do not allow update initialized job for now, because
	// the job would be updated by both job and update goal
	// state engine. The constraint may be removed later
	if jobRuntime.GetState() == pbjob.JobState_INITIALIZED {
		return nil, yarpcerrors.UnavailableErrorf(
			"cannot update partially created job")
	}

	jobConfig, err := handlerutil.ConvertJobSpecToJobConfig(req.GetSpec())
	if err != nil {
		return nil, err
	}
	prevJobConfig, prevConfigAddOn, err := h.jobStore.GetJobConfigWithVersion(
		ctx,
		jobID,
		jobRuntime.GetConfigurationVersion())
	if err != nil {
		return nil, err
	}

	if err := validateJobConfigUpdate(prevJobConfig, jobConfig); err != nil {
		return nil, err
	}

	// get the new configAddOn
	var respoolPath string
	for _, label := range prevConfigAddOn.GetSystemLabels() {
		if label.GetKey() == common.SystemLabelResourcePool {
			respoolPath = label.GetValue()
		}
	}
	configAddOn := &models.ConfigAddOn{
		SystemLabels: jobutil.ConstructSystemLabels(jobConfig, respoolPath),
	}

	// if change log is set, CreateWorkflow would use the version inside
	// to do concurrency control.
	// However, for replace job, concurrency control is done by entity version.
	// User should not be required to provide config version when entity version is
	// provided.
	jobConfig.ChangeLog = nil
	updateID, newEntityVersion, err := cachedJob.CreateWorkflow(
		ctx,
		models.WorkflowType_UPDATE,
		handlerutil.ConvertUpdateSpecToUpdateConfig(req.GetUpdateSpec()),
		req.GetVersion(),
		cached.WithConfig(jobConfig, prevJobConfig, configAddOn),
	)

	// In case of error, since it is not clear if job runtime was
	// persisted with the update ID or not, enqueue the update to
	// the goal state. If the update ID got persisted, update should
	// start running, else, it should be aborted. Enqueueing it into
	// the goal state will ensure both. In case the update was not
	// persisted, clear the cache as well so that it is reloaded
	// from DB and cleaned up.
	if len(updateID.GetValue()) > 0 {
		h.goalStateDriver.EnqueueUpdate(jobID, updateID, time.Now())
	}

	if err != nil {
		return nil, err
	}

	return &svc.ReplaceJobResponse{Version: newEntityVersion}, nil
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
	req *svc.PauseJobWorkflowRequest) (resp *svc.PauseJobWorkflowResponse, err error) {
	defer func() {
		if err != nil {
			log.WithField("request", req).
				WithError(err).
				Warn("JobSVC.PauseJobWorkflow failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
			Info("JobSVC.PauseJobWorkflow succeeded")
	}()

	cachedJob := h.jobFactory.AddJob(&peloton.JobID{Value: req.GetJobId().GetValue()})
	updateID, newEntityVersion, err := cachedJob.PauseWorkflow(ctx, req.GetVersion())

	if len(updateID.GetValue()) > 0 {
		h.goalStateDriver.EnqueueUpdate(cachedJob.ID(), updateID, time.Now())
	}

	if err != nil {
		return nil, err
	}

	return &svc.PauseJobWorkflowResponse{Version: newEntityVersion}, nil
}

func (h *serviceHandler) ResumeJobWorkflow(
	ctx context.Context,
	req *svc.ResumeJobWorkflowRequest) (resp *svc.ResumeJobWorkflowResponse, err error) {
	defer func() {
		if err != nil {
			log.WithField("request", req).
				WithError(err).
				Warn("JobSVC.ResumeJobWorkflow failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
			Info("JobSVC.ResumeJobWorkflow succeeded")
	}()

	cachedJob := h.jobFactory.AddJob(&peloton.JobID{Value: req.GetJobId().GetValue()})
	updateID, newEntityVersion, err := cachedJob.ResumeWorkflow(ctx, req.GetVersion())

	if len(updateID.GetValue()) > 0 {
		h.goalStateDriver.EnqueueUpdate(cachedJob.ID(), updateID, time.Now())
	}

	if err != nil {
		return nil, err
	}

	return &svc.ResumeJobWorkflowResponse{Version: newEntityVersion}, nil
}

func (h *serviceHandler) AbortJobWorkflow(
	ctx context.Context,
	req *svc.AbortJobWorkflowRequest) (resp *svc.AbortJobWorkflowResponse, err error) {
	defer func() {
		if err != nil {
			log.WithField("request", req).
				WithError(err).
				Warn("JobSVC.AbortJobWorkflow failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
			Info("JobSVC.AbortJobWorkflow succeeded")
	}()

	cachedJob := h.jobFactory.AddJob(&peloton.JobID{Value: req.GetJobId().GetValue()})
	updateID, newEntityVersion, err := cachedJob.AbortWorkflow(ctx, req.GetVersion())

	if len(updateID.GetValue()) > 0 {
		h.goalStateDriver.EnqueueUpdate(cachedJob.ID(), updateID, time.Now())
	}

	if err != nil {
		return nil, err
	}

	return &svc.AbortJobWorkflowResponse{Version: newEntityVersion}, nil
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

func (h *serviceHandler) getJobSummary(
	ctx context.Context,
	jobID *v1alphapeloton.JobID) (*svc.GetJobResponse, error) {
	jobSummary, err := h.jobStore.GetJobSummaryFromIndex(
		ctx,
		&peloton.JobID{Value: jobID.GetValue()},
	)
	if err != nil {
		return nil, errors.Wrap(err, "fail to get job summary")
	}

	var updateInfo *models.UpdateModel
	if len(jobSummary.GetRuntime().GetUpdateID().GetValue()) > 0 {
		updateInfo, err = h.updateStore.GetUpdate(
			ctx,
			jobSummary.GetRuntime().GetUpdateID(),
		)
		if err != nil {
			return nil, errors.Wrap(err, "fail to get update information")
		}
	}

	return &svc.GetJobResponse{
		Summary: handlerutil.ConvertJobSummary(jobSummary, updateInfo),
	}, nil
}

func (h *serviceHandler) getJobConfigurationWithVersion(
	ctx context.Context,
	jobID *v1alphapeloton.JobID,
	version *v1alphapeloton.EntityVersion) (*svc.GetJobResponse, error) {
	configVersion, _, err := jobutil.ParseJobEntityVersion(version)
	if err != nil {
		return nil, err
	}

	jobConfig, _, err := h.jobStore.GetJobConfigWithVersion(
		ctx,
		&peloton.JobID{Value: jobID.GetValue()},
		configVersion,
	)
	if err != nil {
		return nil, errors.Wrap(err, "fail to get job spec")
	}

	return &svc.GetJobResponse{
		JobInfo: &stateless.JobInfo{
			JobId: jobID,
			Spec:  handlerutil.ConvertJobConfigToJobSpec(jobConfig),
		},
	}, nil
}

func (h *serviceHandler) GetJob(
	ctx context.Context,
	req *svc.GetJobRequest) (resp *svc.GetJobResponse, err error) {
	defer func() {
		if err != nil {
			log.WithField("request", req).
				WithError(err).
				Info("StatelessJobSvc.GetJob failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("req", req).
			Debug("StatelessJobSvc.GetJob succeeded")
	}()

	// Get the summary only
	if req.GetSummaryOnly() == true {
		return h.getJobSummary(ctx, req.GetJobId())
	}

	// Get the configuration for a given version only
	if req.GetVersion() != nil {
		return h.getJobConfigurationWithVersion(ctx, req.GetJobId(), req.GetVersion())
	}

	// Get the latest configuration and runtime
	jobConfig, _, err := h.jobStore.GetJobConfig(
		ctx,
		&peloton.JobID{Value: req.GetJobId().GetValue()},
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get job spec")
	}

	// Do not display the secret volumes in defaultconfig that were added by
	// handleSecrets. They should remain internal to peloton logic.
	// Secret ID and Path should be returned using the peloton.Secret
	// proto message.
	secretVolumes := util.RemoveSecretVolumesFromJobConfig(jobConfig)

	jobRuntime, err := h.jobStore.GetJobRuntime(
		ctx,
		&peloton.JobID{Value: req.GetJobId().GetValue()},
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get job status")
	}

	var updateInfo *models.UpdateModel
	if len(jobRuntime.GetUpdateID().GetValue()) > 0 {
		updateInfo, err = h.updateStore.GetUpdate(
			ctx,
			jobRuntime.GetUpdateID(),
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get update information")
		}
	}

	return &svc.GetJobResponse{
		JobInfo: &stateless.JobInfo{
			JobId:  req.GetJobId(),
			Spec:   handlerutil.ConvertJobConfigToJobSpec(jobConfig),
			Status: handlerutil.ConvertRuntimeInfoToJobStatus(jobRuntime, updateInfo),
		},
		Secrets: handlerutil.ConvertSecrets(
			jobmgrtask.CreateSecretsFromVolumes(secretVolumes)),
		WorkflowInfo: handlerutil.ConvertUpdateModelToWorkflowInfo(updateInfo),
	}, nil
}

func (h *serviceHandler) GetJobIDFromJobName(
	ctx context.Context,
	req *svc.GetJobIDFromJobNameRequest) (resp *svc.GetJobIDFromJobNameResponse, err error) {
	return &svc.GetJobIDFromJobNameResponse{}, nil
}

func (h *serviceHandler) GetWorkflowEvents(
	ctx context.Context,
	req *svc.GetWorkflowEventsRequest) (*svc.GetWorkflowEventsResponse, error) {
	return &svc.GetWorkflowEventsResponse{}, nil
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
	req *svc.QueryJobsRequest) (resp *svc.QueryJobsResponse, err error) {
	defer func() {
		if err != nil {
			log.WithField("request", req).
				WithError(err).
				Warn("JobSVC.QueryJobs failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("num_of_results", len(resp.GetRecords())).
			Debug("JobSVC.QueryJobs succeeded")
	}()

	var respoolID *peloton.ResourcePoolID
	if len(req.GetSpec().GetRespool().GetValue()) > 0 {
		respoolResp, err := h.respoolClient.LookupResourcePoolID(ctx, &respool.LookupRequest{
			Path: &respool.ResourcePoolPath{Value: req.GetSpec().GetRespool().GetValue()},
		})
		if err != nil {
			return nil, errors.Wrap(err, "fail to get respool id")
		}
		respoolID = respoolResp.GetId()
	}

	querySpec := handlerutil.ConvertStatelessQuerySpecToJobQuerySpec(req.GetSpec())
	log.WithField("spec", querySpec).
		Info("converted spec")
	_, jobSummaries, total, err := h.jobStore.QueryJobs(
		ctx,
		respoolID,
		querySpec,
		true)
	if err != nil {
		return nil, errors.Wrap(err, "fail to get job summary")
	}

	var statelessJobSummaries []*stateless.JobSummary
	for _, jobSummary := range jobSummaries {
		var statelessJobLabels []*v1alphapeloton.Label
		for _, label := range jobSummary.GetLabels() {
			statelessJobLabels = append(statelessJobLabels, &v1alphapeloton.Label{
				Key:   label.GetKey(),
				Value: label.GetValue(),
			})
		}

		var updateModel *models.UpdateModel
		if len(jobSummary.GetRuntime().GetUpdateID().GetValue()) > 0 {
			updateModel, err = h.updateStore.GetUpdate(ctx, jobSummary.GetRuntime().GetUpdateID())
			if err != nil {
				return nil, errors.Wrap(err, "fail to get update")
			}
		}

		statelessJobSummary := handlerutil.ConvertJobSummary(jobSummary, updateModel)
		statelessJobSummaries = append(statelessJobSummaries, statelessJobSummary)
	}

	return &svc.QueryJobsResponse{
		Records: statelessJobSummaries,
		Pagination: &pelotonv1alphaquery.Pagination{
			Offset: req.GetSpec().GetPagination().GetOffset(),
			Limit:  req.GetSpec().GetPagination().GetLimit(),
			Total:  total,
		},
		Spec: req.GetSpec(),
	}, nil
}

func (h *serviceHandler) ListJobs(
	req *svc.ListJobsRequest,
	stream svc.JobServiceServiceListJobsYARPCServer) (err error) {
	defer func() {
		if err != nil {
			log.WithError(err).
				Info("JobSVC.ListJobs failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.Debug("JobSVC.ListJobs succeeded")
	}()

	jobSummaries, err := h.jobStore.GetAllJobsInJobIndex(context.Background())
	if err != nil {
		return err
	}

	for _, jobSummary := range jobSummaries {
		var updateInfo *models.UpdateModel

		if len(jobSummary.GetRuntime().GetUpdateID().GetValue()) > 0 {
			updateInfo, err = h.updateStore.GetUpdate(
				context.Background(),
				jobSummary.GetRuntime().GetUpdateID(),
			)
			if err != nil {
				return err
			}
		}

		resp := &svc.ListJobsResponse{
			Jobs: []*stateless.JobSummary{
				handlerutil.ConvertJobSummary(jobSummary, updateInfo),
			},
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}

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

	status := convertCacheToJobStatus(runtime)
	status.WorkflowStatus = convertCacheToWorkflowStatus(cachedWorkflow)

	return &svc.GetJobCacheResponse{
		Spec:   convertCacheJobConfigToJobSpec(config),
		Status: status,
	}, nil
}

func validateJobConfigUpdate(
	prevJobConfig *pbjob.JobConfig,
	newJobConfig *pbjob.JobConfig,
) error {
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

func convertCacheJobConfigToJobSpec(config jobmgrcommon.JobConfig) *stateless.JobSpec {
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

func convertCacheToJobStatus(
	runtime *pbjob.RuntimeInfo,
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
	result.Version = jobutil.GetJobEntityVersion(
		runtime.GetConfigurationVersion(),
		runtime.GetWorkflowVersion())
	return result
}

func convertCacheToWorkflowStatus(
	cachedWorkflow cached.Update,
) *stateless.WorkflowStatus {
	workflowStatus := &stateless.WorkflowStatus{}
	workflowStatus.Type = stateless.WorkflowType(cachedWorkflow.GetWorkflowType())
	workflowStatus.State = stateless.WorkflowState(cachedWorkflow.GetState().State)
	workflowStatus.NumInstancesCompleted = uint32(len(cachedWorkflow.GetInstancesDone()))
	workflowStatus.NumInstancesFailed = uint32(len(cachedWorkflow.GetInstancesFailed()))
	workflowStatus.NumInstancesRemaining =
		uint32(len(cachedWorkflow.GetGoalState().Instances) -
			len(cachedWorkflow.GetInstancesDone()) -
			len(cachedWorkflow.GetInstancesFailed()))
	workflowStatus.InstancesCurrent = cachedWorkflow.GetInstancesCurrent()
	workflowStatus.PrevVersion = jobutil.GetPodEntityVersion(cachedWorkflow.GetState().JobVersion)
	workflowStatus.Version = jobutil.GetPodEntityVersion(cachedWorkflow.GetGoalState().JobVersion)
	return workflowStatus
}
