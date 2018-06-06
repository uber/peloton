package jobsvc

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	apierrors "code.uber.internal/infra/peloton/.gen/peloton/api/errors"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/query"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common"

	"code.uber.internal/infra/peloton/jobmgr/cached"
	"code.uber.internal/infra/peloton/jobmgr/goalstate"
	"code.uber.internal/infra/peloton/jobmgr/job/config"
	jobmgrtask "code.uber.internal/infra/peloton/jobmgr/task"
	"code.uber.internal/infra/peloton/leader"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"

	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
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
	secretStore storage.SecretStore,
	jobFactory cached.JobFactory,
	goalStateDriver goalstate.Driver,
	candidate leader.Candidate,
	clientName string,
	jobSvcCfg Config) {

	jobSvcCfg.normalize()
	handler := &serviceHandler{
		jobStore:        jobStore,
		taskStore:       taskStore,
		secretStore:     secretStore,
		respoolClient:   respool.NewResourceManagerYARPCClient(d.ClientConfig(clientName)),
		resmgrClient:    resmgrsvc.NewResourceManagerServiceYARPCClient(d.ClientConfig(clientName)),
		rootCtx:         context.Background(),
		jobFactory:      jobFactory,
		goalStateDriver: goalStateDriver,
		candidate:       candidate,
		metrics:         NewMetrics(parent.SubScope("jobmgr").SubScope("job")),
		jobSvcCfg:       jobSvcCfg,
	}

	d.Register(job.BuildJobManagerYARPCProcedures(handler))
}

// serviceHandler implements peloton.api.job.JobManager
type serviceHandler struct {
	jobStore        storage.JobStore
	taskStore       storage.TaskStore
	secretStore     storage.SecretStore
	respoolClient   respool.ResourceManagerYARPCClient
	resmgrClient    resmgrsvc.ResourceManagerServiceYARPCClient
	rootCtx         context.Context
	jobFactory      cached.JobFactory
	goalStateDriver goalstate.Driver
	candidate       leader.Candidate
	metrics         *Metrics
	jobSvcCfg       Config
}

// Create creates a job object for a given job configuration and
// enqueues the tasks for scheduling
func (h *serviceHandler) Create(
	ctx context.Context,
	req *job.CreateRequest) (*job.CreateResponse, error) {

	h.metrics.JobAPICreate.Inc(1)

	jobID := req.GetId()
	// It is possible that jobId is nil since protobuf doesn't enforce it
	if jobID == nil {
		jobID = &peloton.JobID{Value: ""}
	}

	if !h.candidate.IsLeader() {
		h.metrics.JobCreateFail.Inc(1)
		return nil, yarpcerrors.UnavailableErrorf("Job Create API not suppported on non-leader")
	}

	if len(jobID.Value) == 0 {
		jobID.Value = uuid.New()
		log.WithField("job_id", jobID).Info("Genarating UUID for empty job ID")
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
	jobConfig := req.GetConfig()

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
	err = jobconfig.ValidateTaskConfig(jobConfig, h.jobSvcCfg.MaxTasksPerJob)
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

	err = h.handleSecrets(ctx, jobID, jobConfig, req.GetSecrets())
	if err != nil {
		h.metrics.JobCreateFail.Inc(1)
		return &job.CreateResponse{}, err
	}

	// Create job in cache and db
	cachedJob := h.jobFactory.AddJob(jobID)
	err = cachedJob.Create(ctx, jobConfig, "peloton")
	if err != nil {
		// best effort to clean up cache and db when job creation fails
		h.jobFactory.ClearJob(jobID)
		h.jobStore.DeleteJob(ctx, jobID)

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

	// Enqueue job into goal state engine
	h.goalStateDriver.EnqueueJob(jobID, time.Now())

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

	if !h.candidate.IsLeader() {
		h.metrics.JobUpdateFail.Inc(1)
		return nil, yarpcerrors.UnavailableErrorf("Job Update API not suppported on non-leader")
	}

	jobID := req.Id
	cachedJob := h.jobFactory.AddJob(jobID)
	jobRuntime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID.GetValue()).
			Error("Failed to get runtime")
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

	err = jobconfig.ValidateUpdatedConfig(oldConfig, newConfig, h.jobSvcCfg.MaxTasksPerJob)
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

	diff := jobconfig.CalculateJobDiff(jobID, oldConfig, newConfig)
	if diff.IsNoop() {
		log.WithField("job_id", jobID.GetValue()).
			Info("update is a noop")
		return nil, nil
	}

	// TODO (adityacb): handle secrets here
	// Do not update secret id of existing secrets.
	// Just replace the secret data in the database for that id
	err = cachedJob.Update(ctx, &job.JobInfo{
		Config: newConfig,
	}, cached.UpdateCacheAndDB)
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

	log.WithField("job_id", jobID.GetValue()).
		Infof("adding %d instances", len(diff.InstancesToAdd))

	if err := h.taskStore.CreateTaskConfigs(ctx, jobID, newConfig); err != nil {
		h.metrics.JobUpdateFail.Inc(1)
		return nil, err
	}

	for id, runtime := range diff.InstancesToAdd {
		runtime.ConfigVersion = newConfig.GetChangeLog().GetVersion()
		runtime.DesiredConfigVersion = newConfig.GetChangeLog().GetVersion()
		runtimes := make(map[uint32]*pbtask.RuntimeInfo)
		runtimes[id] = runtime
		if err := cachedJob.CreateTasks(ctx, runtimes, "peloton"); err != nil {
			log.WithError(err).WithField("job_id", jobID.GetValue()).Error("Failed to create task for job")
			h.metrics.TaskCreateFail.Inc(1)
			h.metrics.JobUpdateFail.Inc(1)
			return nil, err
		}
		h.metrics.TaskCreate.Inc(1)
		h.goalStateDriver.EnqueueTask(jobID, id, time.Now())
	}

	goalstate.EnqueueJobWithDefaultDelay(jobID, h.goalStateDriver, cachedJob)

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
				NotFound: &apierrors.JobNotFound{
					Id:      req.Id,
					Message: err.Error(),
				},
			},
		}, nil
	}

	cachedJob := h.jobFactory.AddJob(req.Id)
	jobRuntime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		h.metrics.JobGetFail.Inc(1)
		log.WithError(err).
			WithField("job_id", req.Id.Value).
			Debug("failed to get runtime")
		return &job.GetResponse{
			Error: &job.GetResponse_Error{
				GetRuntimeFail: &apierrors.JobGetRuntimeFail{
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

	if !h.candidate.IsLeader() {
		h.metrics.JobRefreshFail.Inc(1)
		return nil, yarpcerrors.UnavailableErrorf("Job Refresh API not suppported on non-leader")
	}

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

	// Update cache and enqueue job into goal state
	cachedJob := h.jobFactory.AddJob(req.GetId())
	cachedJob.Update(ctx, &job.JobInfo{
		Config:  jobConfig,
		Runtime: jobRuntime,
	}, cached.UpdateCacheOnly)
	h.goalStateDriver.EnqueueJob(req.GetId(), time.Now())
	h.metrics.JobRefresh.Inc(1)
	return &job.RefreshResponse{}, nil
}

// Query returns a list of jobs matching the given query
// List/Query API should not use cachedJob
// because we would not clean up the cache for untracked job
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
				Err: &apierrors.UnknownError{
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

	cachedJob := h.jobFactory.AddJob(req.Id)
	jobRuntime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		log.WithError(err).
			WithField("job_id", req.GetId().GetValue()).
			Error("Failed to get runtime")
		h.metrics.JobDeleteFail.Inc(1)
		return nil, yarpcerrors.NotFoundErrorf("job not found")
	}

	if !util.IsPelotonJobStateTerminal(jobRuntime.State) {
		h.metrics.JobDeleteFail.Inc(1)
		return nil, yarpcerrors.InternalErrorf(
			fmt.Sprintf("Job is not in a terminal state: %s", jobRuntime.State))
	}

	if err := h.jobStore.DeleteJob(ctx, req.Id); err != nil {
		h.metrics.JobDeleteFail.Inc(1)
		log.Errorf("Delete job failed with error %v", err)
		return nil, err
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

// handleSecrets will validate secrets in Create/Update job
// request. It will then construct the mesos volume of type
// secret and add it to the defaultconfig for this job.
// This will be a noop if no secrets are provided in the
// request. If secrets are provided in the request but are
// not enabled on the cluster, this will result in error.
func (h *serviceHandler) handleSecrets(
	ctx context.Context,
	jobID *peloton.JobID,
	jobConfig *job.JobConfig,
	secrets []*peloton.Secret,
) error {
	if len(secrets) > 0 {
		if !h.jobSvcCfg.EnableSecrets {
			return yarpcerrors.InvalidArgumentErrorf(
				"secrets not supported by cluster",
			)
		}

		// Secrets will be common for all instances in a job.
		// They will be a part of default container config.
		// This means that if a job is created with secrets,
		// we will ensure that the job also has a default config
		// with mesos containerizer. The secrets will be used by
		// all tasks in that job and all tasks must use
		// mesos containerizer for processing secrets.
		if jobConfig.GetDefaultConfig() == nil {
			return yarpcerrors.InvalidArgumentErrorf(
				"default config cannot be nil when using secrets",
			)
		}
		if jobConfig.GetDefaultConfig().GetContainer() == nil {
			return yarpcerrors.InvalidArgumentErrorf(
				"default config doesn't have containerizer info",
			)
		}
		// make sure default config uses mesos containerizer
		if jobConfig.GetDefaultConfig().GetContainer().GetType() != mesos.ContainerInfo_MESOS {
			return yarpcerrors.InvalidArgumentErrorf(
				fmt.Sprintf("secrets not supported for container type %v",
					jobConfig.GetDefaultConfig().GetContainer().GetType()),
			)
		}

		// Go through each instance config and make sure they are
		// using mesos containerizer
		for _, taskConfig := range jobConfig.GetInstanceConfig() {
			if taskConfig != nil && taskConfig.GetContainer() != nil {
				if taskConfig.GetContainer().GetType() != mesos.ContainerInfo_MESOS {
					return yarpcerrors.InvalidArgumentErrorf(
						fmt.Sprintf("secrets not supported for "+
							"container type %v",
							taskConfig.GetContainer().GetType()),
					)
				}
			}
		}

		for _, secret := range secrets {
			if len(secret.GetId().GetValue()) == 0 {
				secret.Id = &peloton.SecretID{
					Value: uuid.New(),
				}
				log.WithField("job_id", secret.GetId().GetValue()).
					Info("Genarating UUID for empty secret ID")
			}

			// Validate that secret is base64 encoded
			_, err := base64.StdEncoding.DecodeString(
				string(secret.GetValue().GetData()),
			)
			if err != nil {
				return yarpcerrors.InvalidArgumentErrorf(
					fmt.Sprintf("failed to decode secret: %v %v",
						secret.GetValue().GetData(), err),
				)
			}

			// store secret in DB
			err = h.secretStore.CreateSecret(ctx, secret, jobID)
			if err != nil {
				return err
			}

			// Add volume/secret to default container config with this secret
			// Use secretID instead of secret data when storing as
			// part of default config in DB.
			// This is done to prevent secrets leaks via logging/API etc.
			// At the time of task launch, launcher will read the
			// secret by secret-id and replace it by secret data.
			jobConfig.DefaultConfig.Container.Volumes =
				append(
					jobConfig.DefaultConfig.Container.Volumes,
					jobmgrtask.CreateSecretVolume(secret.GetPath(),
						secret.GetId().GetValue()),
				)
		}
	}
	return nil
}
