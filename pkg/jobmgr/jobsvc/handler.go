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

package jobsvc

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	apierrors "github.com/uber/peloton/.gen/peloton/api/v0/errors"
	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/query"
	"github.com/uber/peloton/.gen/peloton/api/v0/respool"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	pbupdate "github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/private/models"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/leader"
	"github.com/uber/peloton/pkg/common/util"
	versionutil "github.com/uber/peloton/pkg/common/util/entityversion"
	yarpcutil "github.com/uber/peloton/pkg/common/util/yarpc"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	"github.com/uber/peloton/pkg/jobmgr/goalstate"
	"github.com/uber/peloton/pkg/jobmgr/job/config"
	jobmgrtask "github.com/uber/peloton/pkg/jobmgr/task"
	"github.com/uber/peloton/pkg/jobmgr/util/handler"
	jobutil "github.com/uber/peloton/pkg/jobmgr/util/job"
	"github.com/uber/peloton/pkg/storage"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"

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
	ormStore *ormobjects.Store,
	jobFactory cached.JobFactory,
	goalStateDriver goalstate.Driver,
	candidate leader.Candidate,
	clientName string,
	jobSvcCfg Config) {

	jobSvcCfg.normalize()
	handler := &serviceHandler{
		jobStore:        jobStore,
		taskStore:       taskStore,
		jobIndexOps:     ormobjects.NewJobIndexOps(ormStore),
		jobConfigOps:    ormobjects.NewJobConfigOps(ormStore),
		jobRuntimeOps:   ormobjects.NewJobRuntimeOps(ormStore),
		secretInfoOps:   ormobjects.NewSecretInfoOps(ormStore),
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
	activeJobsOps   ormobjects.ActiveJobsOps
	jobIndexOps     ormobjects.JobIndexOps
	jobConfigOps    ormobjects.JobConfigOps
	jobRuntimeOps   ormobjects.JobRuntimeOps
	secretInfoOps   ormobjects.SecretInfoOps
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
	req *job.CreateRequest) (resp *job.CreateResponse, err error) {
	defer func() {
		jobID := req.GetId().GetValue()
		instanceCount := req.GetConfig().GetInstanceCount()
		headers := yarpcutil.GetHeaders(ctx)

		if err != nil || resp.GetError() != nil {
			entry := log.WithField("job_id", jobID).
				WithField("instance_count", instanceCount).
				WithField("headers", headers)

			if err != nil {
				entry = entry.WithError(err)
			}

			if resp.GetError() != nil {
				entry = entry.WithField("create_error", resp.GetError().String())
			}

			entry.Warn("JobManager.CreateJob failed")
			return
		}

		log.WithField("job_id", jobID).
			WithField("response", resp).
			WithField("instance_count", instanceCount).
			WithField("headers", headers).
			Info("JobSVC.CreateJob succeeded")
	}()

	h.metrics.JobAPICreate.Inc(1)

	if !h.candidate.IsLeader() {
		h.metrics.JobCreateFail.Inc(1)
		return nil, yarpcerrors.UnavailableErrorf(
			"Job Create API not suppported on non-leader")
	}

	jobID := req.GetId()
	// It is possible that jobId is nil since protobuf doesn't enforce it
	if jobID == nil || len(jobID.GetValue()) == 0 {
		jobID = &peloton.JobID{Value: uuid.New()}
	}

	if uuid.Parse(jobID.GetValue()) == nil {
		log.WithField("job_id", jobID.GetValue()).Warn("JobID is not valid UUID")
		h.metrics.JobCreateFail.Inc(1)
		return &job.CreateResponse{
			Error: &job.CreateResponse_Error{
				InvalidJobId: &job.InvalidJobId{
					Id:      jobID,
					Message: "JobID must be valid UUID",
				},
			},
		}, nil
	}

	jobConfig := req.GetConfig()

	respoolPath, err := h.validateResourcePool(jobConfig.GetRespoolID())
	if err != nil {
		h.metrics.JobCreateFail.Inc(1)
		return &job.CreateResponse{
			Error: &job.CreateResponse_Error{
				InvalidConfig: &job.InvalidJobConfig{
					Id:      jobID,
					Message: err.Error(),
				},
			},
		}, nil
	}

	// Validate job config with default task configs
	err = jobconfig.ValidateConfig(jobConfig, h.jobSvcCfg.MaxTasksPerJob)
	if err != nil {
		h.metrics.JobCreateFail.Inc(1)
		return &job.CreateResponse{
			Error: &job.CreateResponse_Error{
				InvalidConfig: &job.InvalidJobConfig{
					Id:      jobID,
					Message: err.Error(),
				},
			},
		}, nil
	}

	// check secrets and config for input sanity
	if err = h.validateSecretsAndConfig(
		jobConfig, req.GetSecrets()); err != nil {
		return &job.CreateResponse{}, err
	}

	// create secrets in the DB and add them as secret volumes to defaultconfig
	err = h.handleCreateSecrets(ctx, jobID, jobConfig, req.GetSecrets())
	if err != nil {
		h.metrics.JobCreateFail.Inc(1)
		return &job.CreateResponse{}, err
	}

	// Create job in cache and db
	cachedJob := h.jobFactory.AddJob(jobID)

	systemLabels := jobutil.ConstructSystemLabels(jobConfig, respoolPath.GetValue())
	configAddOn := &models.ConfigAddOn{
		SystemLabels: systemLabels,
	}
	err = cachedJob.Create(ctx, jobConfig, configAddOn, nil)
	// if err is not nil, still enqueue to goal state engine,
	// because job may be partially created. Goal state engine
	// knows if the job can be recovered
	h.goalStateDriver.EnqueueJob(jobID, time.Now())

	if err != nil {
		h.metrics.JobCreateFail.Inc(1)
		return &job.CreateResponse{
			Error: &job.CreateResponse_Error{
				AlreadyExists: &job.JobAlreadyExists{
					Id:      req.Id,
					Message: err.Error(),
				},
			},
			JobId: jobID, // should return the jobID even when error occurs
			// because the job may be running
		}, nil
	}
	h.metrics.JobCreate.Inc(1)

	return &job.CreateResponse{
		JobId: jobID,
	}, nil
}

// Update updates a job object for a given job configuration and
// performs the appropriate action based on the change
func (h *serviceHandler) Update(
	ctx context.Context,
	req *job.UpdateRequest) (resp *job.UpdateResponse, err error) {
	defer func() {
		jobID := req.GetId().GetValue()
		headers := yarpcutil.GetHeaders(ctx)
		configVersion := req.GetConfig().GetChangeLog().GetVersion()

		if err != nil || resp.GetError() != nil {
			entry := log.WithField("job_id", jobID).
				WithField("headers", headers).
				WithField("config_version", configVersion)

			if err != nil {
				entry = entry.WithError(err)
			}

			if resp.GetError() != nil {
				entry = entry.WithField("update_error", resp.GetError().String())
			}

			entry.Warn("JobManager.Update failed")
			return
		}

		log.WithField("job_id", jobID).
			WithField("response", resp).
			WithField("config_version", configVersion).
			WithField("headers", headers).
			Info("JobManager.Update succeeded")
	}()

	h.metrics.JobAPIUpdate.Inc(1)

	if !h.candidate.IsLeader() {
		h.metrics.JobUpdateFail.Inc(1)
		return nil, yarpcerrors.UnavailableErrorf(
			"Job Update API not suppported on non-leader")
	}

	jobID := req.GetId()
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
		return nil, yarpcerrors.InvalidArgumentErrorf(msg)
	}

	newConfig := req.GetConfig()

	oldConfig, oldConfigAddOn, err := h.jobConfigOps.Get(
		ctx,
		jobID,
		jobRuntime.GetConfigurationVersion())
	if err != nil {
		h.metrics.JobUpdateFail.Inc(1)
		return nil, err
	}

	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID.GetValue()).
			Error("Failed to GetJobConfig")
		h.metrics.JobUpdateFail.Inc(1)
		return nil, err
	}

	if oldConfig.GetType() != job.JobType_BATCH {
		return nil, yarpcerrors.InvalidArgumentErrorf(
			"job update is only supported for batch jobs")
	}

	if newConfig.GetRespoolID() == nil {
		newConfig.RespoolID = oldConfig.GetRespoolID()
	}

	// Remove the existing secret volumes from the config. These were added by
	// peloton at the time of secret creation. We will add them to new config
	// after validating the new config at the time of handling secrets. If we
	// keep these volumes in oldConfig, ValidateUpdatedConfig will fail.
	existingSecretVolumes := util.RemoveSecretVolumesFromJobConfig(oldConfig)

	// check secrets and new config for input sanity
	if err := h.validateSecretsAndConfig(newConfig, req.GetSecrets()); err != nil {
		return nil, err
	}
	err = jobconfig.ValidateUpdatedConfig(oldConfig, newConfig, h.jobSvcCfg.MaxTasksPerJob)
	if err != nil {
		h.metrics.JobUpdateFail.Inc(1)
		return nil, yarpcerrors.InvalidArgumentErrorf(err.Error())
	}

	if err = h.handleUpdateSecrets(ctx, jobID, existingSecretVolumes, newConfig,
		req.GetSecrets()); err != nil {
		h.metrics.JobUpdateFail.Inc(1)
		return nil, err
	}

	instancesToAdd := newConfig.GetInstanceCount() -
		oldConfig.GetInstanceCount()
	// You could just update secrets of a job without changing instance count.
	// In that case, do not treat this Update as NOOP.
	if instancesToAdd <= 0 && len(req.GetSecrets()) == 0 {
		log.WithField("job_id", jobID.GetValue()).
			Info("update is a noop")
		return nil, nil
	}

	var respoolPath string
	for _, label := range oldConfigAddOn.GetSystemLabels() {
		if label.GetKey() == common.SystemLabelResourcePool {
			respoolPath = label.GetValue()
		}
	}
	newConfigAddOn := &models.ConfigAddOn{
		SystemLabels: jobutil.ConstructSystemLabels(newConfig, respoolPath),
	}
	// first persist the configuration
	newUpdatedConfig, err := cachedJob.CompareAndSetConfig(
		ctx,
		mergeInstanceConfig(oldConfig, newConfig),
		newConfigAddOn,
		nil)
	if err != nil {
		h.metrics.JobUpdateFail.Inc(1)
		return nil, err
	}

	// next persist the runtime state and the new configuration version
	err = cachedJob.Update(ctx, &job.JobInfo{
		Runtime: &job.RuntimeInfo{
			ConfigurationVersion: newUpdatedConfig.GetChangeLog().GetVersion(),
			State:                job.JobState_INITIALIZED,
		},
	}, nil,
		nil,
		cached.UpdateCacheAndDB)
	if err != nil {
		h.metrics.JobUpdateFail.Inc(1)
		return nil, err
	}

	h.goalStateDriver.EnqueueJob(jobID, time.Now())

	h.metrics.JobUpdate.Inc(1)
	msg := fmt.Sprintf("added %d instances", instancesToAdd)
	return &job.UpdateResponse{
		Id:      jobID,
		Message: msg,
	}, nil
}

// Get returns a job config for a given job ID
func (h *serviceHandler) Get(
	ctx context.Context,
	req *job.GetRequest) (resp *job.GetResponse, err error) {
	defer func() {
		jobID := req.GetId().GetValue()
		headers := yarpcutil.GetHeaders(ctx)

		if err != nil || resp.GetError() != nil {
			entry := log.WithField("job_id", jobID).
				WithField("headers", headers)

			if err != nil {
				entry = entry.WithError(err)
			}

			if resp.GetError() != nil {
				entry = entry.WithField("get_error", resp.GetError().String())
			}

			entry.Warn("JobManager.Get failed")
			return
		}

		log.WithField("job_id", jobID).
			WithField("response", resp).
			WithField("headers", headers).
			Debug("JobManager.Get succeeded")
	}()

	h.metrics.JobAPIGet.Inc(1)

	jobRuntime, err := handler.GetJobRuntimeWithoutFillingCache(
		ctx,
		req.Id,
		h.jobFactory,
		h.jobRuntimeOps,
	)
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

	jobConfig, _, err := h.jobConfigOps.Get(
		ctx,
		req.GetId(),
		jobRuntime.GetConfigurationVersion())
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

	// Do not display the secret volumes in defaultconfig that were added by
	// handleSecrets. They should remain internal to peloton logic.
	// Secret ID and Path should be returned using the peloton.Secret
	// proto message.
	secretVolumes := util.RemoveSecretVolumesFromJobConfig(jobConfig)

	h.metrics.JobGet.Inc(1)
	resp = &job.GetResponse{
		JobInfo: &job.JobInfo{
			Id:      req.GetId(),
			Config:  jobConfig,
			Runtime: jobRuntime,
		},
		Secrets: jobmgrtask.CreateSecretsFromVolumes(secretVolumes),
	}
	return resp, nil
}

// Refresh loads the task runtime state from DB, updates the cache,
// and enqueues it to goal state for evaluation.
func (h *serviceHandler) Refresh(ctx context.Context, req *job.RefreshRequest) (resp *job.RefreshResponse, err error) {
	defer func() {
		jobID := req.GetId().GetValue()
		headers := yarpcutil.GetHeaders(ctx)

		if err != nil {
			log.WithField("job_id", jobID).
				WithField("headers", headers).
				WithError(err).
				Warn("JobManager.Refresh failed")
			return
		}

		log.WithField("job_id", jobID).
			WithField("headers", headers).
			Debug("JobManager.Refresh succeeded")
	}()

	h.metrics.JobAPIRefresh.Inc(1)

	if !h.candidate.IsLeader() {
		h.metrics.JobRefreshFail.Inc(1)
		return nil, yarpcerrors.UnavailableErrorf("Job Refresh API not suppported on non-leader")
	}

	jobRuntime, err := h.jobRuntimeOps.Get(ctx, req.GetId())
	if err != nil {
		log.WithError(err).
			WithField("job_id", req.GetId().GetValue()).
			Error("failed to get job runtime in refresh job")
		h.metrics.JobRefreshFail.Inc(1)
		return &job.RefreshResponse{}, yarpcerrors.NotFoundErrorf("job not found")
	}

	jobConfig, configAddOn, err := h.jobConfigOps.Get(
		ctx,
		req.GetId(),
		jobRuntime.GetConfigurationVersion())
	if err != nil {
		log.WithError(err).
			WithField("job_id", req.GetId().GetValue()).
			Error("failed to get job config in refresh job")
		h.metrics.JobRefreshFail.Inc(1)
		return &job.RefreshResponse{}, yarpcerrors.NotFoundErrorf("job not found")
	}

	// Update cache and enqueue job into goal state
	cachedJob := h.jobFactory.AddJob(req.GetId())
	cachedJob.Update(ctx, &job.JobInfo{
		Config:  jobConfig,
		Runtime: jobRuntime,
	}, configAddOn,
		nil,
		cached.UpdateCacheOnly)
	h.goalStateDriver.EnqueueJob(req.GetId(), time.Now())
	h.metrics.JobRefresh.Inc(1)
	return &job.RefreshResponse{}, nil
}

// Query returns a list of jobs matching the given query
// List/Query API should not use cachedJob
// because we would not clean up the cache for untracked job
func (h *serviceHandler) Query(ctx context.Context, req *job.QueryRequest) (resp *job.QueryResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)

		if err != nil || resp.GetError() != nil {
			entry := log.WithField("headers", headers)

			if err != nil {
				entry = entry.WithError(err)
			}

			if resp.GetError() != nil {
				entry = entry.WithField("query_error", resp.GetError().String())
			}
			entry.Warn("JobManager.Query failed")
			return
		}

		log.WithField("headers", headers).
			WithField("response", resp).
			Debug("JobManager.Query succeeded")
	}()

	h.metrics.JobAPIQuery.Inc(1)
	callStart := time.Now()

	jobConfigs, jobSummary, total, err := h.jobStore.QueryJobs(ctx, req.GetRespoolID(), req.GetSpec(), req.GetSummaryOnly())
	if err != nil {
		h.metrics.JobQueryFail.Inc(1)
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
	resp = &job.QueryResponse{
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
	return resp, nil
}

// Delete removes jobs metadata from storage for a terminal job
func (h *serviceHandler) Delete(
	ctx context.Context,
	req *job.DeleteRequest) (resp *job.DeleteResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		jobID := req.GetId().GetValue()

		if err != nil || resp.GetError() != nil {
			entry := log.WithField("job_id", jobID).
				WithField("headers", headers)

			if err != nil {
				entry = entry.WithError(err)
			}

			if resp.GetError() != nil {
				entry = entry.WithField("delete_error", resp.GetError().String())
			}
			entry.Warn("JobManager.Delete failed")
			return
		}

		log.WithField("job_id", jobID).
			WithField("headers", headers).
			Info("JobManager.Delete succeeded")
	}()

	h.metrics.JobAPIDelete.Inc(1)

	jobRuntime, err := handler.GetJobRuntimeWithoutFillingCache(
		ctx, req.Id, h.jobFactory, h.jobRuntimeOps)
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

	// Delete job from DB
	if err := h.jobStore.DeleteJob(ctx, req.GetId().GetValue()); err != nil {
		h.metrics.JobDeleteFail.Inc(1)
		log.Errorf("Delete job failed with error %v", err)
		return nil, err
	}

	if err := h.jobIndexOps.Delete(ctx, req.GetId()); err != nil {
		h.metrics.JobDeleteFail.Inc(1)
		log.WithField("job_id", req.GetId()).
			WithError(err).Error("Failed to delete job from job_index")
		return nil, err
	}

	// Delete job from goalstate and cache
	cachedJob := h.jobFactory.GetJob(req.GetId())
	if cachedJob != nil {
		taskMap := cachedJob.GetAllTasks()
		for instID := range taskMap {
			h.goalStateDriver.DeleteTask(req.GetId(), instID)
		}
		h.goalStateDriver.DeleteJob(req.GetId())
		h.jobFactory.ClearJob(req.GetId())
	}

	h.metrics.JobDelete.Inc(1)
	return &job.DeleteResponse{}, nil
}

func (h *serviceHandler) Restart(
	ctx context.Context,
	req *job.RestartRequest) (resp *job.RestartResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		jobID := req.GetId().GetValue()

		if err != nil {
			log.WithField("job_id", jobID).
				WithField("headers", headers).
				WithError(err).
				Warn("JobManager.Restart failed")
			return
		}

		log.WithField("job_id", jobID).
			WithField("headers", headers).
			Info("JobManager.Restart succeeded")
	}()

	h.metrics.JobAPIRestart.Inc(1)

	updateID, resourceVersion, err := h.createNonUpdateWorkflow(
		ctx,
		req.GetId(),
		req.GetResourceVersion(),
		req.GetRanges(),
		req.GetRestartConfig().GetBatchSize(),
		models.WorkflowType_RESTART,
	)

	if err != nil {
		h.metrics.JobRestartFail.Inc(1)
		return nil, err
	}

	h.metrics.JobRestart.Inc(1)
	return &job.RestartResponse{
		UpdateID:        updateID,
		ResourceVersion: resourceVersion,
	}, nil
}

func (h *serviceHandler) Start(
	ctx context.Context,
	req *job.StartRequest) (resp *job.StartResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		jobID := req.GetId().GetValue()

		if err != nil {
			log.WithField("job_id", jobID).
				WithField("headers", headers).
				WithError(err).
				Warn("JobManager.Start failed")
			return
		}

		log.WithField("job_id", jobID).
			WithField("headers", headers).
			Info("JobManager.Start succeeded")
	}()

	h.metrics.JobAPIStart.Inc(1)

	updateID, resourceVersion, err := h.createNonUpdateWorkflow(
		ctx,
		req.GetId(),
		req.GetResourceVersion(),
		req.GetRanges(),
		req.GetStartConfig().GetBatchSize(),
		models.WorkflowType_START,
	)

	if err != nil {
		h.metrics.JobStartFail.Inc(1)
		return nil, err
	}

	h.metrics.JobStart.Inc(1)
	return &job.StartResponse{
		UpdateID:        updateID,
		ResourceVersion: resourceVersion,
	}, nil
}

func (h *serviceHandler) Stop(
	ctx context.Context,
	req *job.StopRequest) (resp *job.StopResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		jobID := req.GetId().GetValue()

		if err != nil {
			log.WithField("job_id", jobID).
				WithField("headers", headers).
				WithError(err).
				Warn("JobManager.Stop failed")
			return
		}

		log.WithField("job_id", jobID).
			WithField("headers", headers).
			Info("JobManager.Stop succeeded")
	}()

	h.metrics.JobAPIStop.Inc(1)

	updateID, resourceVersion, err := h.createNonUpdateWorkflow(
		ctx,
		req.GetId(),
		req.GetResourceVersion(),
		req.GetRanges(),
		req.GetStopConfig().GetBatchSize(),
		models.WorkflowType_STOP,
	)

	if err != nil {
		h.metrics.JobStopFail.Inc(1)
		return nil, err
	}

	h.metrics.JobStop.Inc(1)
	return &job.StopResponse{
		UpdateID:        updateID,
		ResourceVersion: resourceVersion,
	}, nil
}

// createNonUpdateWorkflow creates a workflow excluding UPDATE
// (i.e RESTART/START/STOP are supported)
// it returns updateID, new resource version upon success
func (h *serviceHandler) createNonUpdateWorkflow(
	ctx context.Context,
	jobID *peloton.JobID,
	resourceVersion uint64,
	ranges []*task.InstanceRange,
	batchSize uint32,
	workflowType models.WorkflowType,
) (*peloton.UpdateID, uint64, error) {
	if workflowType == models.WorkflowType_UNKNOWN || workflowType == models.WorkflowType_UPDATE {
		return nil, 0,
			yarpcerrors.InvalidArgumentErrorf(
				"unexpected WorkflowType_%s", workflowType.String())
	}

	if !h.candidate.IsLeader() {
		return nil, 0,
			yarpcerrors.UnavailableErrorf(
				"Job %s API not suppported on non-leader", workflowType.String())
	}

	cachedJob := h.jobFactory.AddJob(jobID)
	runtime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		return nil, 0, err
	}

	jobConfig, configAddOn, err := h.jobConfigOps.Get(
		ctx,
		jobID,
		runtime.GetConfigurationVersion(),
	)

	if err != nil {
		return nil, 0, err
	}

	if jobConfig.GetType() != job.JobType_SERVICE {
		return nil, 0, yarpcerrors.InvalidArgumentErrorf(
			"%s supported only for service jobs", workflowType.String())
	}

	// copy the config with provided resource version number
	newConfig := *jobConfig
	now := time.Now()
	newConfig.ChangeLog = &peloton.ChangeLog{
		Version:   resourceVersion,
		CreatedAt: uint64(now.UnixNano()),
		UpdatedAt: uint64(now.UnixNano()),
	}
	if ranges == nil {
		ranges = []*task.InstanceRange{
			{From: 0, To: newConfig.GetInstanceCount()},
		}
	}

	updateID, _, err := cachedJob.CreateWorkflow(
		ctx,
		workflowType,
		&pbupdate.UpdateConfig{
			BatchSize: batchSize,
		},
		versionutil.GetJobEntityVersion(
			runtime.GetConfigurationVersion(),
			runtime.GetDesiredStateVersion(),
			runtime.GetWorkflowVersion()),
		cached.WithInstanceToProcess(
			nil,
			convertRangesToSlice(ranges, newConfig.GetInstanceCount()),
			nil),
		cached.WithConfig(
			&newConfig,
			jobConfig,
			configAddOn,
			nil,
		),
	)

	// In case of error, since it is not clear if job runtime was
	// persisted with the update ID or not, enqueue the update to
	// the goal state. If the update ID got persisted, update should
	// start running, else, it should be aborted. Enqueueing it into
	// the goal state will ensure both. In case the update was not
	// persisted, clear the cache as well so that it is reloaded
	// from DB and cleaned up.
	// Add update to goal state engine to start it
	if len(updateID.GetValue()) > 0 {
		h.goalStateDriver.EnqueueUpdate(jobID, updateID, time.Now())
	}

	if err != nil {
		return nil, 0, err
	}

	cachedConfig, err := cachedJob.GetConfig(ctx)
	if err != nil {
		return nil, 0, err
	}

	return updateID, cachedConfig.GetChangeLog().GetVersion(), err
}

func (h *serviceHandler) GetCache(
	ctx context.Context,
	req *job.GetCacheRequest) (resp *job.GetCacheResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		jobID := req.GetId().GetValue()

		if err != nil {
			log.WithField("job_id", jobID).
				WithField("headers", headers).
				WithError(err).
				Warn("JobManager.GetCache failed")
			return
		}

		log.WithField("job_id", jobID).
			WithField("headers", headers).
			Debug("JobManager.GetCache succeeded")
	}()

	cachedJob := h.jobFactory.GetJob(req.GetId())
	if cachedJob == nil {
		return nil,
			yarpcerrors.NotFoundErrorf("Job not found in cache")
	}

	runtime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		return nil,
			yarpcerrors.InternalErrorf("Cannot get job runtime with error %v", err)
	}

	config, err := cachedJob.GetConfig(ctx)
	if err != nil {
		return nil,
			yarpcerrors.InternalErrorf("Cannot get job config with error %v", err)
	}

	return &job.GetCacheResponse{
		Runtime: runtime,
		Config: &job.JobConfig{
			ChangeLog:     config.GetChangeLog(),
			SLA:           config.GetSLA(),
			RespoolID:     config.GetRespoolID(),
			Type:          config.GetType(),
			InstanceCount: config.GetInstanceCount(),
		},
	}, nil
}

// GetActiveJobs is a debug only API used to get the list of active job IDs
// stored in Peloton. It will be temporarily used for testing the consistency
// between active_jobs table and mv_job_by_state materialzied view
func (h *serviceHandler) GetActiveJobs(
	ctx context.Context,
	req *job.GetActiveJobsRequest) (resp *job.GetActiveJobsResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)

		if err != nil {
			log.WithField("headers", headers).
				WithError(err).
				Warn("JobManager.GetActiveJobs failed")
			return
		}

		log.WithField("headers", headers).
			Debug("JobManager.GetActiveJobs succeeded")
	}()

	jobIDs, err := h.activeJobsOps.GetAll(ctx)
	if err != nil {
		return nil, err
	}

	return &job.GetActiveJobsResponse{
		Ids: jobIDs,
	}, nil
}

// validateResourcePool validates the resource pool before submitting job
func (h *serviceHandler) validateResourcePool(
	respoolID *peloton.ResourcePoolID,
) (*respool.ResourcePoolPath, error) {
	ctx, cancelFunc := context.WithTimeout(h.rootCtx, 10*time.Second)
	defer cancelFunc()

	if respoolID == nil {
		return nil, errNullResourcePoolID
	}

	if respoolID.GetValue() == common.RootResPoolID {
		return nil, errRootResourcePoolID
	}

	var request = &respool.GetRequest{
		Id: respoolID,
	}
	response, err := h.respoolClient.GetResourcePool(ctx, request)
	if err != nil {
		return nil, err
	}

	if response.GetError() != nil {
		return nil, errResourcePoolNotFound
	}

	if response.GetPoolinfo() != nil && response.GetPoolinfo().Id != nil {
		if response.GetPoolinfo().Id.Value != respoolID.Value {
			return nil, errResourcePoolNotFound
		}
	} else {
		return nil, errResourcePoolNotFound
	}

	if len(response.GetPoolinfo().GetChildren()) > 0 {
		return nil, errNonLeafResourcePool
	}

	return response.GetPoolinfo().GetPath(), nil
}

// validateSecretsAndConfig checks the secrets for input sanity and makes sure
// that config does not contain any existing secret volumes because that is
// not supported.
func (h *serviceHandler) validateSecretsAndConfig(
	config *job.JobConfig, secrets []*peloton.Secret) error {
	// make sure that config doesn't have any secret volumes
	if util.ConfigHasSecretVolumes(config.GetDefaultConfig()) {
		return yarpcerrors.InvalidArgumentErrorf(
			"adding secret volumes directly in config is not allowed",
		)
	}
	// validate secrets payload for input sanity
	if len(secrets) == 0 {
		return nil
	}

	if !h.jobSvcCfg.EnableSecrets && len(secrets) > 0 {
		return yarpcerrors.InvalidArgumentErrorf(
			"secrets not supported by cluster",
		)
	}
	for _, secret := range secrets {
		if secret.GetPath() == "" {
			return yarpcerrors.InvalidArgumentErrorf(
				"secret does not have a path")
		}
		// Validate that secret is base64 encoded
		_, err := base64.StdEncoding.DecodeString(
			string(secret.GetValue().GetData()))
		if err != nil {
			return yarpcerrors.InvalidArgumentErrorf(
				fmt.Sprintf("failed to decode secret with error: %v", err),
			)
		}
	}
	return nil
}

// validateMesosContainerizerForSecrets returns error if default config doesn't
// use mesos containerizer. Secrets will be common for all instances in a job.
// They will be a part of default container config. This means that if a job is
// created with secrets, we will ensure that the job also has a default config
// with mesos containerizer. The secrets will be used by all tasks in that job
// and all tasks must use mesos containerizer for processing secrets.
// We will not enforce that instance config has mesos containerizer and let
// instance config override this to keep with existing convention.
func validateMesosContainerizerForSecrets(jobConfig *job.JobConfig) error {
	// make sure that default config uses mesos containerizer
	if jobConfig.GetDefaultConfig().GetContainer().GetType() !=
		mesos.ContainerInfo_MESOS {
		return yarpcerrors.InvalidArgumentErrorf(
			fmt.Sprintf("container type %v does not match %v",
				jobConfig.GetDefaultConfig().GetContainer().GetType(),
				mesos.ContainerInfo_MESOS),
		)
	}
	return nil
}

// handleCreateSecrets handles secrets to be added at the time of creating a job
func (h *serviceHandler) handleCreateSecrets(
	ctx context.Context, jobID *peloton.JobID,
	config *job.JobConfig, secrets []*peloton.Secret,
) error {
	// if there are no secrets in the request,
	// job create doesn't need to handle secrets
	if len(secrets) == 0 {
		return nil
	}
	// Make sure that the default config is using Mesos containerizer
	if err := validateMesosContainerizerForSecrets(config); err != nil {
		return err
	}
	// for each secret, store it in DB and add a secret volume to defaultconfig
	err := h.addSecretsToDBAndConfig(ctx, jobID, config, secrets, false)
	return err
}

// handleUpdateSecrets handles secrets to be added/updated for a job
func (h *serviceHandler) handleUpdateSecrets(
	ctx context.Context, jobID *peloton.JobID, secretVolumes []*mesos.Volume,
	newConfig *job.JobConfig, secrets []*peloton.Secret,
) error {
	// if there are no existing secret volumes and no secrets in the request,
	// this job update doesn't need to handle secrets
	if len(secretVolumes) == 0 && len(secrets) == 0 {
		return nil
	}
	// Make sure all existing secret volumes are covered in the secrets.
	// Separate secrets into adds and updates.
	addSecrets, updateSecrets, err := h.validateExistingSecretVolumes(
		ctx, secretVolumes, secrets)
	if err != nil {
		return err
	}
	// add new secrets in DB and add them as secret volumes to defaultconfig
	if err = h.addSecretsToDBAndConfig(
		ctx, jobID, newConfig, addSecrets, false); err != nil {
		return err
	}
	// update secrets in DB and add them as secret volumes to defaultconfig
	err = h.addSecretsToDBAndConfig(ctx, jobID, newConfig, updateSecrets, true)
	return err
}

func (h *serviceHandler) addSecretsToDBAndConfig(
	ctx context.Context, jobID *peloton.JobID, jobConfig *job.JobConfig,
	secrets []*peloton.Secret, update bool) error {
	// for each secret, store it in DB and add a secret volume to defaultconfig
	for _, secret := range secrets {
		if secret.GetId().GetValue() == "" {
			secret.Id = &peloton.SecretID{
				Value: uuid.New(),
			}
			log.WithField("job_id", secret.GetId().GetValue()).
				Info("Genarating UUID for empty secret ID")
		}
		// store secret in DB
		if update {
			if err := h.secretInfoOps.UpdateSecretData(
				ctx,
				jobID.GetValue(),
				string(secret.GetValue().GetData()),
			); err != nil {
				return err
			}
		} else {
			if err := h.secretInfoOps.CreateSecret(
				ctx,
				jobID.GetValue(),
				time.Now(),
				secret.Id.GetValue(),
				string(secret.GetValue().GetData()),
				secret.Path,
			); err != nil {
				return err
			}
		}
		// Add volume/secret to default container config with this secret
		// Use secretID instead of secret data when storing as
		// part of default config in DB.
		// This is done to prevent secrets leaks via logging/API etc.
		// At the time of task launch, launcher will read the
		// secret by secret-id and replace it by secret data.
		jobConfig.GetDefaultConfig().GetContainer().Volumes =
			append(jobConfig.GetDefaultConfig().GetContainer().Volumes,
				util.CreateSecretVolume(secret.GetPath(),
					secret.GetId().GetValue()),
			)
	}
	return nil
}

// validateExistingSecretVolumes goes through existing secret volumes and
// validates that the new secrets list contains a secret as existing secrets
// for that job. It splits the secrets in request as addSecrets and
// updateSecrets. addSecrets will be created newly in DB and added to the
// defaultconfig. updateSecrets will be updated in the DB only because they
// are already present in defaultconfig.
//
// We do not have authN/authZ support on Peloton as of now.
// So there could be a security hole like this:
// 		Alice launches jobA with secrets a1,a2,a3
//		Bob updates jobA and adds more tasks to it
// 		Bob is not authorized to use secrets a1,a2,a3 but the new task on jobA
//      would still be able to access them
// To fix this hole, until authN/authZ is available, we will ensure that any
// Update request to a job that has secrets associated with it, contains
// existing secrets (same ID or path) as part of the request. The secret data
// could be different. This ensures that in the above example, Bob can never
// have access to a1,a2,a3.
// TODO: Remove this restriction after authN/authZ is enabled
// At that time, we will be sure that the job owner is also the secret
// owner and is updating the job
func (h *serviceHandler) validateExistingSecretVolumes(
	ctx context.Context, secretVolumes []*mesos.Volume,
	secrets []*peloton.Secret,
) (addSecrets []*peloton.Secret, updateSecrets []*peloton.Secret, err error) {
	// the number of secrets in the request should be >= the number of
	// existing secrets in the job config
	if len(secrets) < len(secretVolumes) {
		return nil, nil, yarpcerrors.InvalidArgumentErrorf(
			"number of secrets in request should be >= existing secrets")
	}
	// create a map of new secrets provided in the request
	secretMap := make(map[string]*peloton.Secret)
	for _, secret := range secrets {
		if secret.GetId().GetValue() != "" {
			secretMap[secret.GetId().GetValue()] = secret
		} else if secret.GetPath() != "" {
			// TODO: Remove this after we have separate API
			// for maintaining secrets at which point, secrets should be always
			// identified by secretID or name (and not created as part of Job
			// Create/Update API)
			// currently, the provided secrets may or may not have an ID
			// so we can identify them with Path
			secretMap[secret.GetPath()] = secret
		}
	}

	// Go through each secret volume, then verify that the secret is also
	// present in the new secrets list
	for _, volume := range secretVolumes {
		// verify that the secret ID or Path in the existing secret volume
		// is present in the secrets provided in the API request
		existingSecretID := volume.GetSource().GetSecret().GetValue().GetData()
		existingSecretPath := volume.GetContainerPath()
		if secret, ok := secretMap[string(existingSecretID)]; ok {
			updateSecrets = append(updateSecrets, secret)
			delete(secretMap, string(existingSecretID))
		} else if secret, ok := secretMap[string(existingSecretPath)]; ok {
			// provided secret doesn't have ID but matches the path of an
			// existing secret. Assign existing secretID to this.
			secret.GetId().Value = string(existingSecretID)
			updateSecrets = append(updateSecrets, secret)
			delete(secretMap, string(existingSecretPath))
		} else {
			return nil, nil, yarpcerrors.InvalidArgumentErrorf(
				fmt.Sprintf("request missing secret with id %v path %v",
					string(existingSecretID), existingSecretPath))
		}
	}
	// Now the secrets that remain in the secretMap don't already exist.
	// They should be added not updated.
	for _, secret := range secretMap {
		addSecrets = append(addSecrets, secret)
	}
	return addSecrets, updateSecrets, nil
}

// In batch job update, instancesConfig of newConfig only need to include the config
// for the additional instances. If a job config is directly updated to newConfig,
// JobMgr would lose the track of previous instance config. As a result, JobMgr has
// to use the merged result of instanceConfig in oldConfig and newConfig.
// configs passed in mergeInstanceConfig must have been validated.
func mergeInstanceConfig(oldConfig *job.JobConfig, newConfig *job.JobConfig) *job.JobConfig {
	result := *newConfig
	newInstanceConfig := make(map[uint32]*task.TaskConfig)
	for instanceID, instanceConfig := range oldConfig.InstanceConfig {
		newInstanceConfig[instanceID] = instanceConfig
	}
	for instanceID, instanceConfig := range newConfig.InstanceConfig {
		newInstanceConfig[instanceID] = instanceConfig
	}
	result.InstanceConfig = newInstanceConfig
	return &result
}

// convertRangesToSlice merges ranges into a single slice and remove
// any duplicated item
// need the instanceCount because cli may send max uint32 when range is not specified.
// TODO: cli send nil ranges when not specified
func convertRangesToSlice(ranges []*task.InstanceRange, instanceCount uint32) []uint32 {
	var result []uint32
	set := make(map[uint32]bool)

	for _, instanceRange := range ranges {
		for i := instanceRange.GetFrom(); i < instanceRange.GetTo(); i++ {
			// ignore instances above instanceCount
			if i >= instanceCount {
				break
			}
			// dedup result
			if !set[i] {
				result = append(result, i)
				set[i] = true
			}
		}
	}
	return result
}

// NewTestServiceHandler returns an empty new ServiceHandler ptr for testing.
func NewTestServiceHandler() *serviceHandler {
	return &serviceHandler{}
}
