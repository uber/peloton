package jobsvc

import (
	"context"
	"fmt"
	"time"

	api_errors "code.uber.internal/infra/peloton/.gen/peloton/api/errors"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/query"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/jobmgr/job/updater"
	task_config "code.uber.internal/infra/peloton/jobmgr/task/config"
	"code.uber.internal/infra/peloton/jobmgr/tracked"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"

	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	_defaultRPCTimeout = 10 * time.Second
)

var (
	errNullResourcePoolID   = errors.New("resource pool ID is null")
	errResourcePoolNotFound = errors.New("resource pool not found")
	errRootResourcePoolID   = errors.New("cannot submit jobs to the `root` resource pool")
	errNonLeafResourcePool  = errors.New("cannot submit jobs to a non leaf " +
		"resource pool")
)

// InitServiceHandler initializes the job manager
func InitServiceHandler(
	d *yarpc.Dispatcher,
	parent tally.Scope,
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	trackedManager tracked.Manager,
	clientName string,
	jobSvcCfg Config) {

	jobSvcCfg.normalize()
	handler := &serviceHandler{
		jobStore:       jobStore,
		taskStore:      taskStore,
		respoolClient:  respool.NewResourceManagerYARPCClient(d.ClientConfig(clientName)),
		resmgrClient:   resmgrsvc.NewResourceManagerServiceYARPCClient(d.ClientConfig(clientName)),
		rootCtx:        context.Background(),
		trackedManager: trackedManager,
		metrics:        NewMetrics(parent.SubScope("jobmgr").SubScope("job")),
		jobSvcCfg:      jobSvcCfg,
	}

	d.Register(job.BuildJobManagerYARPCProcedures(handler))
}

// serviceHandler implements peloton.api.job.JobManager
type serviceHandler struct {
	jobStore       storage.JobStore
	taskStore      storage.TaskStore
	respoolClient  respool.ResourceManagerYARPCClient
	resmgrClient   resmgrsvc.ResourceManagerServiceYARPCClient
	rootCtx        context.Context
	trackedManager tracked.Manager
	metrics        *Metrics
	jobSvcCfg      Config
}

// Create creates a job object for a given job configuration and
// enqueues the tasks for scheduling
func (h *serviceHandler) Create(
	ctx context.Context,
	req *job.CreateRequest) (*job.CreateResponse, error) {

	log.WithField("request", req).Debug("JobManager.Create called")

	h.metrics.JobAPICreate.Inc(1)

	jobID := req.Id
	// It is possible that jobId is nil since protobuf doesn't enforce it
	if jobID == nil {
		jobID = &peloton.JobID{Value: ""}
	}

	if len(jobID.Value) == 0 {
		jobID.Value = uuid.New()
		log.WithField("jobID", jobID).Info("Genarating UUID ID for empty job ID")
	} else {
		if uuid.Parse(jobID.Value) == nil {
			log.WithField("job_id", jobID.Value).Warn("JobID is not valid UUID")
			h.metrics.JobCreateFail.Inc(1)
			return &job.CreateResponse{
				Error: &job.CreateResponse_Error{
					InvalidJobId: &job.InvalidJobId{
						Id:      req.Id,
						Message: "JobID must be valid UUID",
					},
				},
			}, nil
		}
	}
	jobConfig := req.Config

	err := h.validateResourcePool(jobConfig.RespoolID)
	if err != nil {
		h.metrics.JobCreateFail.Inc(1)
		return &job.CreateResponse{
			Error: &job.CreateResponse_Error{
				InvalidConfig: &job.InvalidJobConfig{
					Id:      req.Id,
					Message: err.Error(),
				},
			},
		}, nil
	}

	log.WithField("config", jobConfig).Infof("JobManager.Create called")

	// Validate job config with default task configs
	err = task_config.ValidateTaskConfig(jobConfig, h.jobSvcCfg.MaxTasksPerJob)
	if err != nil {
		h.metrics.JobCreateFail.Inc(1)
		return &job.CreateResponse{
			Error: &job.CreateResponse_Error{
				InvalidConfig: &job.InvalidJobConfig{
					Id:      req.Id,
					Message: err.Error(),
				},
			},
		}, nil
	}

	err = h.jobStore.CreateJob(ctx, jobID, jobConfig, "peloton")
	if err != nil {
		h.metrics.JobCreateFail.Inc(1)
		return &job.CreateResponse{
			Error: &job.CreateResponse_Error{
				AlreadyExists: &job.JobAlreadyExists{
					Id:      req.Id,
					Message: err.Error(),
				},
			},
		}, nil
	}
	h.metrics.JobCreate.Inc(1)

	// Put job in tracked manager to create tasks
	jobInfo := &job.JobInfo{
		Id:     jobID,
		Config: jobConfig,
	}
	h.trackedManager.SetJob(jobID, jobInfo, tracked.UpdateAndSchedule)

	return &job.CreateResponse{
		JobId: jobID,
	}, nil
}

// Update updates a job object for a given job configuration and
// performs the appropriate action based on the change
func (h *serviceHandler) Update(
	ctx context.Context,
	req *job.UpdateRequest) (*job.UpdateResponse, error) {

	h.metrics.JobAPIUpdate.Inc(1)

	jobID := req.Id
	jobRuntime, err := h.jobStore.GetJobRuntime(ctx, jobID)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID.GetValue()).
			Error("Failed to GetJobRuntime")
		h.metrics.JobUpdateFail.Inc(1)
		return nil, err
	}

	if util.IsPelotonJobStateTerminal(jobRuntime.State) {
		msg := fmt.Sprintf("Job is in a terminal state:%s", jobRuntime.State)
		h.metrics.JobUpdateFail.Inc(1)
		return &job.UpdateResponse{
			Error: &job.UpdateResponse_Error{
				InvalidJobId: &job.InvalidJobId{
					Id:      req.Id,
					Message: msg,
				},
			},
		}, nil
	}

	newConfig := req.Config
	oldConfig, err := h.jobStore.GetJobConfig(ctx, jobID)

	if newConfig.RespoolID == nil {
		newConfig.RespoolID = oldConfig.RespoolID
	}

	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID.GetValue()).
			Error("Failed to GetJobConfig")
		h.metrics.JobUpdateFail.Inc(1)
		return nil, err
	}

	diff, err := updater.CalculateJobDiff(jobID, oldConfig, newConfig)
	if err != nil {
		h.metrics.JobUpdateFail.Inc(1)
		return &job.UpdateResponse{
			Error: &job.UpdateResponse_Error{
				InvalidConfig: &job.InvalidJobConfig{
					Id:      jobID,
					Message: err.Error(),
				},
			},
		}, nil
	}

	if diff.IsNoop() {
		log.WithField("job_id", jobID.GetValue()).
			Info("update is a noop")
		return nil, nil
	}

	err = h.jobStore.UpdateJobConfig(ctx, jobID, newConfig)
	if err != nil {
		h.metrics.JobUpdateFail.Inc(1)
		return &job.UpdateResponse{
			Error: &job.UpdateResponse_Error{
				JobNotFound: &job.JobNotFound{
					Id:      req.Id,
					Message: err.Error(),
				},
			},
		}, nil
	}

	// Update the config in the cache.
	jobInfo := &job.JobInfo{
		Id:     jobID,
		Config: newConfig,
	}
	h.trackedManager.SetJob(jobID, jobInfo, tracked.UpdateOnly)

	log.WithField("job_id", jobID.GetValue()).
		Infof("adding %d instances", len(diff.InstancesToAdd))

	if err := h.taskStore.CreateTaskConfigs(ctx, jobID, newConfig); err != nil {
		h.metrics.JobUpdateFail.Inc(1)
		return nil, err
	}

	for id, runtime := range diff.InstancesToAdd {
		if err := h.taskStore.CreateTaskRuntime(ctx, jobID, id, runtime, "peloton"); err != nil {
			log.WithError(err).WithField("job_id", jobID.GetValue()).Error("Failed to create task for job")
			h.metrics.TaskCreateFail.Inc(1)
			h.metrics.JobUpdateFail.Inc(1)
			return nil, err
		}
		h.metrics.TaskCreate.Inc(1)
		runtimes := make(map[uint32]*pb_task.RuntimeInfo)
		runtimes[id] = runtime
		h.trackedManager.SetTasks(jobID, runtimes, tracked.UpdateAndSchedule)
	}

	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID.GetValue()).
			Error("Failed to update job runtime")
		h.metrics.JobUpdateFail.Inc(1)
		return nil, err
	}

	h.metrics.JobUpdate.Inc(1)
	msg := fmt.Sprintf("added %d instances", len(diff.InstancesToAdd))
	return &job.UpdateResponse{
		Id:      jobID,
		Message: msg,
	}, nil
}

// Get returns a job config for a given job ID
func (h *serviceHandler) Get(
	ctx context.Context,
	req *job.GetRequest) (*job.GetResponse, error) {

	log.WithField("request", req).Debug("JobManager.Get called")
	h.metrics.JobAPIGet.Inc(1)

	jobConfig, err := h.jobStore.GetJobConfig(ctx, req.Id)
	if err != nil {
		h.metrics.JobGetFail.Inc(1)
		log.WithError(err).
			WithField("job_id", req.Id.Value).
			Debug("GetJobConfig failed")
		return &job.GetResponse{
			Error: &job.GetResponse_Error{
				NotFound: &api_errors.JobNotFound{
					Id:      req.Id,
					Message: err.Error(),
				},
			},
		}, nil
	}
	jobRuntime, err := h.jobStore.GetJobRuntime(ctx, req.Id)
	if err != nil {
		h.metrics.JobGetFail.Inc(1)
		log.WithError(err).
			WithField("job_id", req.Id.Value).
			Debug("Get jobRuntime failed")
		return &job.GetResponse{
			Error: &job.GetResponse_Error{
				GetRuntimeFail: &api_errors.JobGetRuntimeFail{
					Id:      req.Id,
					Message: err.Error(),
				},
			},
		}, nil
	}

	h.metrics.JobGet.Inc(1)
	resp := &job.GetResponse{
		JobInfo: &job.JobInfo{
			Id:      req.GetId(),
			Config:  jobConfig,
			Runtime: jobRuntime,
		},
	}
	log.WithField("response", resp).Debug("JobManager.Get returned")
	return resp, nil
}

// Refresh loads the task runtime state from DB, updates the cache,
// and enqueues it to goal state for evaluation.
func (h *serviceHandler) Refresh(ctx context.Context, req *job.RefreshRequest) (*job.RefreshResponse, error) {
	log.WithField("request", req).Debug("JobManager.Refresh called")
	h.metrics.JobAPIRefresh.Inc(1)

	jobConfig, err := h.jobStore.GetJobConfig(ctx, req.GetId())
	if err != nil {
		log.WithError(err).
			WithField("job_id", req.GetId().GetValue()).
			Error("failed to get job config in refresh job")
		h.metrics.JobRefreshFail.Inc(1)
		return &job.RefreshResponse{}, yarpcerrors.NotFoundErrorf("job not found")
	}

	jobRuntime, err := h.jobStore.GetJobRuntime(ctx, req.GetId())
	if err != nil {
		log.WithError(err).
			WithField("job_id", req.GetId().GetValue()).
			Error("failed to get job runtime in refresh job")
		h.metrics.JobRefreshFail.Inc(1)
		return &job.RefreshResponse{}, yarpcerrors.NotFoundErrorf("job not found")
	}

	jobInfo := &job.JobInfo{
		Id:      req.GetId(),
		Config:  jobConfig,
		Runtime: jobRuntime,
	}
	h.trackedManager.SetJob(req.GetId(), jobInfo, tracked.UpdateAndSchedule)
	h.metrics.JobRefresh.Inc(1)
	return &job.RefreshResponse{}, nil
}

// Query returns a list of jobs matching the given query
func (h *serviceHandler) Query(ctx context.Context, req *job.QueryRequest) (*job.QueryResponse, error) {
	log.WithField("request", req).Info("JobManager.Query called")
	h.metrics.JobAPIQuery.Inc(1)
	callStart := time.Now()

	jobConfigs, jobSummary, total, err := h.jobStore.QueryJobs(ctx, req.GetRespoolID(), req.GetSpec(), req.GetSummaryOnly())
	if err != nil {
		h.metrics.JobQueryFail.Inc(1)
		log.WithError(err).Error("Query job failed with error")
		return &job.QueryResponse{
			Error: &job.QueryResponse_Error{
				Err: &api_errors.UnknownError{
					Message: err.Error(),
				},
			},
			Spec: req.GetSpec(),
		}, nil
	}

	h.metrics.JobQuery.Inc(1)
	resp := &job.QueryResponse{
		Records: jobConfigs,
		Results: jobSummary,
		Pagination: &query.Pagination{
			Offset: req.GetSpec().GetPagination().GetOffset(),
			Limit:  req.GetSpec().GetPagination().GetLimit(),
			Total:  total,
		},
		Spec: req.GetSpec(),
	}
	callDuration := time.Since(callStart)
	h.metrics.JobQueryHandlerDuration.Record(callDuration)
	log.WithField("response", resp).Debug("JobManager.Query returned")
	return resp, nil
}

// Delete kills all running tasks in a job
func (h *serviceHandler) Delete(
	ctx context.Context,
	req *job.DeleteRequest) (*job.DeleteResponse, error) {

	h.metrics.JobAPIDelete.Inc(1)

	jobRuntime, err := h.jobStore.GetJobRuntime(ctx, req.GetId())
	if err != nil {
		log.WithError(err).
			WithField("job_id", req.GetId().GetValue()).
			Error("Failed to GetJobRuntime")
		h.metrics.JobDeleteFail.Inc(1)
		return nil, err
	}

	if !util.IsPelotonJobStateTerminal(jobRuntime.State) {
		h.metrics.JobDeleteFail.Inc(1)
		return nil, fmt.Errorf("Job is not in a terminal state: %s", jobRuntime.State)
	}

	if err := h.jobStore.DeleteJob(ctx, req.Id); err != nil {
		h.metrics.JobDeleteFail.Inc(1)
		log.Errorf("Delete job failed with error %v", err)
		return &job.DeleteResponse{
			Error: &job.DeleteResponse_Error{
				NotFound: &api_errors.JobNotFound{
					Id:      req.Id,
					Message: err.Error(),
				},
			},
		}, nil
	}

	h.metrics.JobDelete.Inc(1)
	return &job.DeleteResponse{}, nil
}

// validateResourcePool validates the resource pool before submitting job
func (h *serviceHandler) validateResourcePool(
	respoolID *peloton.ResourcePoolID,
) error {
	ctx, cancelFunc := context.WithTimeout(h.rootCtx, 10*time.Second)
	defer cancelFunc()

	if respoolID == nil {
		return errNullResourcePoolID
	}

	if respoolID.GetValue() == common.RootResPoolID {
		return errRootResourcePoolID
	}

	var request = &respool.GetRequest{
		Id: respoolID,
	}
	response, err := h.respoolClient.GetResourcePool(ctx, request)
	if err != nil {
		return err
	}

	if response.GetError() != nil {
		return errResourcePoolNotFound
	}

	if response.GetPoolinfo() != nil && response.GetPoolinfo().Id != nil {
		if response.GetPoolinfo().Id.Value != respoolID.Value {
			return errResourcePoolNotFound
		}
	} else {
		return errResourcePoolNotFound
	}

	if len(response.GetPoolinfo().GetChildren()) > 0 {
		return errNonLeafResourcePool
	}

	return nil
}
