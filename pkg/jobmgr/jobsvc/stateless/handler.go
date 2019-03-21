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

package stateless

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/respool"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	pbupdate "github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	v1alphapeloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	v1alphaquery "github.com/uber/peloton/.gen/peloton/api/v1alpha/query"
	"github.com/uber/peloton/.gen/peloton/private/models"
	"github.com/uber/peloton/pkg/common/concurrency"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/leader"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	"github.com/uber/peloton/pkg/jobmgr/goalstate"
	jobconfig "github.com/uber/peloton/pkg/jobmgr/job/config"
	"github.com/uber/peloton/pkg/jobmgr/jobsvc"
	jobmgrtask "github.com/uber/peloton/pkg/jobmgr/task"
	"github.com/uber/peloton/pkg/jobmgr/task/activermtask"
	handlerutil "github.com/uber/peloton/pkg/jobmgr/util/handler"
	jobutil "github.com/uber/peloton/pkg/jobmgr/util/job"
	"github.com/uber/peloton/pkg/storage"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"

	"github.com/gocql/gocql"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
)

type serviceHandler struct {
	jobStore        storage.JobStore
	updateStore     storage.UpdateStore
	taskStore       storage.TaskStore
	jobIndexOps     ormobjects.JobIndexOps
	jobNameToIDOps  ormobjects.JobNameToIDOps
	secretInfoOps   ormobjects.SecretInfoOps
	respoolClient   respool.ResourceManagerYARPCClient
	jobFactory      cached.JobFactory
	goalStateDriver goalstate.Driver
	candidate       leader.Candidate
	rootCtx         context.Context
	jobSvcCfg       jobsvc.Config
	activeRMTasks   activermtask.ActiveRMTasks
}

var (
	errNullResourcePoolID   = yarpcerrors.InvalidArgumentErrorf("resource pool ID is null")
	errResourcePoolNotFound = yarpcerrors.NotFoundErrorf("resource pool not found")
	errRootResourcePoolID   = yarpcerrors.InvalidArgumentErrorf("cannot submit jobs to the `root` resource pool")
	errNonLeafResourcePool  = yarpcerrors.InvalidArgumentErrorf("cannot submit jobs to a non leaf resource pool")
)

const (
	// Represents number of goroutine workers to fetch instance workflow events
	_defaultInstanceWorkflowEventsWorker = 25
)

// InitV1AlphaJobServiceHandler initializes the Job Manager V1Alpha Service Handler
func InitV1AlphaJobServiceHandler(
	d *yarpc.Dispatcher,
	jobStore storage.JobStore,
	updateStore storage.UpdateStore,
	taskStore storage.TaskStore,
	ormStore *ormobjects.Store,
	jobFactory cached.JobFactory,
	goalStateDriver goalstate.Driver,
	candidate leader.Candidate,
	jobSvcCfg jobsvc.Config,
	activeRMTasks activermtask.ActiveRMTasks,
) {
	handler := &serviceHandler{
		jobStore:       jobStore,
		updateStore:    updateStore,
		taskStore:      taskStore,
		jobIndexOps:    ormobjects.NewJobIndexOps(ormStore),
		jobNameToIDOps: ormobjects.NewJobNameToIDOps(ormStore),
		secretInfoOps:  ormobjects.NewSecretInfoOps(ormStore),
		respoolClient: respool.NewResourceManagerYARPCClient(
			d.ClientConfig(common.PelotonResourceManager),
		),
		jobFactory:      jobFactory,
		goalStateDriver: goalStateDriver,
		candidate:       candidate,
		jobSvcCfg:       jobSvcCfg,
		activeRMTasks:   activeRMTasks,
	}
	d.Register(svc.BuildJobServiceYARPCProcedures(handler))
}

func (h *serviceHandler) CreateJob(
	ctx context.Context,
	req *svc.CreateJobRequest,
) (resp *svc.CreateJobResponse, err error) {
	defer func() {
		jobID := req.GetJobId().GetValue()
		specVersion := req.GetSpec().GetRevision().GetVersion()
		instanceCount := req.GetSpec().GetInstanceCount()

		if err != nil {
			log.WithField("job_id", jobID).
				WithField("spec_version", specVersion).
				WithField("instance_count", instanceCount).
				WithError(err).
				Warn("JobSVC.CreateJob failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("job_id", jobID).
			WithField("spec_version", specVersion).
			WithField("response", resp).
			WithField("instance_count", instanceCount).
			Info("JobSVC.CreateJob succeeded")
	}()

	if !h.candidate.IsLeader() {
		return nil,
			yarpcerrors.UnavailableErrorf("JobSVC.CreateJob is not supported on non-leader")
	}

	pelotonJobID := &peloton.JobID{Value: req.GetJobId().GetValue()}

	// It is possible that jobId is nil since protobuf doesn't enforce it
	if len(pelotonJobID.GetValue()) == 0 {
		pelotonJobID = &peloton.JobID{Value: uuid.New()}
	}

	if uuid.Parse(pelotonJobID.GetValue()) == nil {
		return nil, yarpcerrors.InvalidArgumentErrorf("jobID is not valid UUID")
	}

	jobSpec := req.GetSpec()

	respoolPath, err := h.validateResourcePoolForJobCreation(ctx, jobSpec.GetRespoolId())
	if err != nil {
		return nil, errors.Wrap(err, "failed to validate resource pool")
	}

	jobConfig, err := handlerutil.ConvertJobSpecToJobConfig(jobSpec)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert job spec")
	}

	// Validate job config with default task configs
	err = jobconfig.ValidateConfig(
		jobConfig,
		h.jobSvcCfg.MaxTasksPerJob,
	)
	if err != nil {
		return nil, errors.Wrap(err, "invalid job spec")
	}

	// check secrets and config for input sanity
	if err = h.validateSecretsAndConfig(jobSpec, req.GetSecrets()); err != nil {
		return nil, errors.Wrap(err, "input cannot contain secret volume")
	}

	// create secrets in the DB and add them as secret volumes to defaultconfig
	err = h.handleCreateSecrets(ctx, pelotonJobID.GetValue(), jobSpec, req.GetSecrets())
	if err != nil {
		return nil, errors.Wrap(err, "failed to handle create-secrets")
	}

	// Create job in cache and db
	cachedJob := h.jobFactory.AddJob(pelotonJobID)

	systemLabels := jobutil.ConstructSystemLabels(jobConfig, respoolPath.GetValue())
	configAddOn := &models.ConfigAddOn{
		SystemLabels: systemLabels,
	}

	var opaqueData *peloton.OpaqueData
	if len(req.GetOpaqueData().GetData()) != 0 {
		opaqueData = &peloton.OpaqueData{
			Data: req.GetOpaqueData().GetData(),
		}
	}

	err = cachedJob.RollingCreate(
		ctx,
		jobConfig,
		configAddOn,
		handlerutil.ConvertCreateSpecToUpdateConfig(req.GetCreateSpec()),
		opaqueData,
		"peloton", /*createBy*/
	)

	// enqueue the job into goal state engine even in failure case.
	// Because the state may be updated, let goal state engine decide what to do
	h.goalStateDriver.EnqueueJob(pelotonJobID, time.Now())

	if err != nil {
		return nil, errors.Wrap(err, "failed to create job in db")
	}

	runtimeInfo, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get job runtime from cache")
	}

	return &svc.CreateJobResponse{
		JobId: &v1alphapeloton.JobID{Value: pelotonJobID.GetValue()},
		Version: jobutil.GetJobEntityVersion(
			runtimeInfo.GetConfigurationVersion(),
			runtimeInfo.GetDesiredStateVersion(),
			runtimeInfo.GetWorkflowVersion(),
		),
	}, nil
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

	if !h.candidate.IsLeader() {
		return nil,
			yarpcerrors.UnavailableErrorf("JobSVC.ReplaceJob is not supported on non-leader")
	}

	// TODO: handle secretes
	jobUUID := uuid.Parse(req.GetJobId().GetValue())
	if jobUUID == nil {
		return nil, yarpcerrors.InvalidArgumentErrorf(
			"JobID must be of UUID format")
	}

	jobConfig, err := handlerutil.ConvertJobSpecToJobConfig(req.GetSpec())
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert job spec")
	}

	err = jobconfig.ValidateConfig(
		jobConfig,
		h.jobSvcCfg.MaxTasksPerJob,
	)
	if err != nil {
		return nil, errors.Wrap(err, "invalid job spec")
	}

	jobID := &peloton.JobID{Value: req.GetJobId().GetValue()}

	cachedJob := h.jobFactory.AddJob(jobID)
	jobRuntime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get job runtime from cache")
	}

	prevJobConfig, prevConfigAddOn, err := h.jobStore.GetJobConfigWithVersion(
		ctx,
		jobID.GetValue(),
		jobRuntime.GetConfigurationVersion())
	if err != nil {
		return nil, errors.Wrap(err, "failed to get previous job spec")
	}

	if err := validateJobConfigUpdate(prevJobConfig, jobConfig); err != nil {
		return nil, errors.Wrap(err, "failed to validate spec update")
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

	opaque := cached.WithOpaqueData(nil)
	if req.GetOpaqueData() != nil {
		opaque = cached.WithOpaqueData(&peloton.OpaqueData{
			Data: req.GetOpaqueData().GetData(),
		})
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
		opaque,
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
		return nil, errors.Wrap(err, "failed to create update workload")
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
	req *svc.RestartJobRequest) (resp *svc.RestartJobResponse, err error) {
	defer func() {
		if err != nil {
			log.WithField("request", req).
				WithError(err).
				Warn("JobSVC.RestartJob failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
			Info("JobSVC.RestartJob succeeded")
	}()

	if !h.candidate.IsLeader() {
		return nil, yarpcerrors.UnavailableErrorf("JobSVC.RestartJob is not supported on non-leader")
	}

	jobID := &peloton.JobID{Value: req.GetJobId().GetValue()}
	cachedJob := h.jobFactory.AddJob(jobID)
	runtime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "fail to get job runtime")
	}

	jobConfig, configAddOn, err := h.jobStore.GetJobConfigWithVersion(
		ctx,
		jobID.GetValue(),
		runtime.GetConfigurationVersion(),
	)
	if err != nil {
		return nil, errors.Wrap(err, "fail to get job config")
	}

	// copy the config with provided resource version number
	newConfig := *jobConfig
	now := time.Now()
	newConfig.ChangeLog = &peloton.ChangeLog{
		Version:   jobConfig.GetChangeLog().GetVersion(),
		CreatedAt: uint64(now.UnixNano()),
		UpdatedAt: uint64(now.UnixNano()),
	}

	opaque := cached.WithOpaqueData(nil)
	if req.GetOpaqueData() != nil {
		opaque = cached.WithOpaqueData(&peloton.OpaqueData{
			Data: req.GetOpaqueData().GetData(),
		})
	}

	instancesToUpdate := convertInstanceIDRangesToSlice(req.GetRestartSpec().GetRanges(), newConfig.GetInstanceCount())
	if len(instancesToUpdate) == 0 {
		for i := uint32(0); i < newConfig.GetInstanceCount(); i++ {
			instancesToUpdate = append(instancesToUpdate, i)
		}
	}

	updateID, newEntityVersion, err := cachedJob.CreateWorkflow(
		ctx,
		models.WorkflowType_RESTART,
		&pbupdate.UpdateConfig{
			BatchSize: req.GetRestartSpec().GetBatchSize(),
			InPlace:   req.GetRestartSpec().GetInPlace(),
		},
		req.GetVersion(),
		cached.WithInstanceToProcess(
			nil,
			instancesToUpdate,
			nil),
		cached.WithConfig(
			&newConfig,
			jobConfig,
			configAddOn,
		),
		opaque,
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
		return nil, err
	}

	return &svc.RestartJobResponse{Version: newEntityVersion}, nil
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
	opaque := cached.WithOpaqueData(nil)
	if req.GetOpaqueData() != nil {
		opaque = cached.WithOpaqueData(&peloton.OpaqueData{
			Data: req.GetOpaqueData().GetData(),
		})
	}

	updateID, newEntityVersion, err := cachedJob.PauseWorkflow(
		ctx,
		req.GetVersion(),
		opaque,
	)

	if len(updateID.GetValue()) > 0 {
		h.goalStateDriver.EnqueueUpdate(cachedJob.ID(), updateID, time.Now())
	}

	if err != nil {
		return nil, errors.Wrap(err, "failed to pause workload")
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

	if !h.candidate.IsLeader() {
		return nil, yarpcerrors.UnavailableErrorf("JobSVC.ResumeJobWorkflow is not supported on non-leader")
	}

	cachedJob := h.jobFactory.AddJob(&peloton.JobID{Value: req.GetJobId().GetValue()})
	opaque := cached.WithOpaqueData(nil)
	if req.GetOpaqueData() != nil {
		opaque = cached.WithOpaqueData(&peloton.OpaqueData{
			Data: req.GetOpaqueData().GetData(),
		})
	}

	updateID, newEntityVersion, err := cachedJob.ResumeWorkflow(
		ctx,
		req.GetVersion(),
		opaque,
	)

	if len(updateID.GetValue()) > 0 {
		h.goalStateDriver.EnqueueUpdate(cachedJob.ID(), updateID, time.Now())
	}

	if err != nil {
		return nil, errors.Wrap(err, "failed to resume workload")
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

	if !h.candidate.IsLeader() {
		return nil, yarpcerrors.UnavailableErrorf("JobSVC.AbortJobWorkflow is not supported on non-leader")
	}

	cachedJob := h.jobFactory.AddJob(&peloton.JobID{Value: req.GetJobId().GetValue()})
	opaque := cached.WithOpaqueData(nil)
	if req.GetOpaqueData() != nil {
		opaque = cached.WithOpaqueData(&peloton.OpaqueData{
			Data: req.GetOpaqueData().GetData(),
		})
	}

	updateID, newEntityVersion, err := cachedJob.AbortWorkflow(
		ctx,
		req.GetVersion(),
		opaque,
	)

	if len(updateID.GetValue()) > 0 {
		h.goalStateDriver.EnqueueUpdate(cachedJob.ID(), updateID, time.Now())
	}

	if err != nil {
		return nil, errors.Wrap(err, "failed to abort workflow")
	}

	return &svc.AbortJobWorkflowResponse{Version: newEntityVersion}, nil
}

func (h *serviceHandler) StartJob(
	ctx context.Context,
	req *svc.StartJobRequest,
) (resp *svc.StartJobResponse, err error) {
	defer func() {
		if err != nil {
			log.WithField("request", req).
				WithError(err).
				Warn("JobSVC.StartJob failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
			Info("JobSVC.StartJob succeeded")
	}()

	if !h.candidate.IsLeader() {
		return nil, yarpcerrors.UnavailableErrorf("JobSVC.StartJob is not supported on non-leader")
	}

	pelotonJobID := &peloton.JobID{Value: req.GetJobId().GetValue()}

	var jobRuntime *pbjob.RuntimeInfo
	count := 0
	cachedJob := h.jobFactory.AddJob(pelotonJobID)

	for {
		jobRuntime, err = cachedJob.GetRuntime(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "fail to get runtime")
		}
		entityVersion := jobutil.GetJobEntityVersion(
			jobRuntime.GetConfigurationVersion(),
			jobRuntime.GetDesiredStateVersion(),
			jobRuntime.GetWorkflowVersion(),
		)
		if entityVersion.GetValue() !=
			req.GetVersion().GetValue() {
			return nil, jobmgrcommon.InvalidEntityVersionError
		}

		jobRuntime.GoalState = pbjob.JobState_RUNNING
		jobRuntime.DesiredStateVersion++

		if jobRuntime, err = cachedJob.CompareAndSetRuntime(ctx, jobRuntime); err != nil {
			if err == jobmgrcommon.UnexpectedVersionError {
				// concurrency error; retry MaxConcurrencyErrorRetry times
				count = count + 1
				if count < jobmgrcommon.MaxConcurrencyErrorRetry {
					continue
				}
			}
			// it is uncertain whether job runtime is updated successfully,
			// let goal state engine figure it out.
			h.goalStateDriver.EnqueueJob(pelotonJobID, time.Now())
			return nil, errors.Wrap(err, "fail to update job runtime")
		}

		h.goalStateDriver.EnqueueJob(pelotonJobID, time.Now())
		return &svc.StartJobResponse{
			Version: jobutil.GetJobEntityVersion(
				jobRuntime.GetConfigurationVersion(),
				jobRuntime.GetDesiredStateVersion(),
				jobRuntime.GetWorkflowVersion(),
			),
		}, nil
	}
}

func (h *serviceHandler) StopJob(
	ctx context.Context,
	req *svc.StopJobRequest,
) (resp *svc.StopJobResponse, err error) {
	defer func() {
		if err != nil {
			log.WithField("request", req).
				WithError(err).
				Warn("JobSVC.StopJob failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
			Info("JobSVC.StopJob succeeded")
	}()

	if !h.candidate.IsLeader() {
		return nil, yarpcerrors.UnavailableErrorf("JobSVC.StopJob is not supported on non-leader")
	}

	cachedJob := h.jobFactory.AddJob(&peloton.JobID{
		Value: req.GetJobId().GetValue(),
	})

	count := 0
	for {
		jobRuntime, err := cachedJob.GetRuntime(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "fail to get runtime")
		}
		entityVersion := jobutil.GetJobEntityVersion(
			jobRuntime.GetConfigurationVersion(),
			jobRuntime.GetDesiredStateVersion(),
			jobRuntime.GetWorkflowVersion(),
		)
		if entityVersion.GetValue() !=
			req.GetVersion().GetValue() {
			return nil, jobmgrcommon.InvalidEntityVersionError
		}

		jobRuntime.GoalState = pbjob.JobState_KILLED
		jobRuntime.DesiredStateVersion++

		if jobRuntime, err = cachedJob.CompareAndSetRuntime(ctx, jobRuntime); err != nil {
			if err == jobmgrcommon.UnexpectedVersionError {
				// concurrency error; retry MaxConcurrencyErrorRetry times
				count = count + 1
				if count < jobmgrcommon.MaxConcurrencyErrorRetry {
					continue
				}
			}
			// it is uncertain whether job runtime is updated successfully,
			// let goal state engine figure it out.
			h.goalStateDriver.EnqueueJob(cachedJob.ID(), time.Now())
			return nil, errors.Wrap(err, "fail to update job runtime")
		}

		h.goalStateDriver.EnqueueJob(cachedJob.ID(), time.Now())
		return &svc.StopJobResponse{
			Version: jobutil.GetJobEntityVersion(
				jobRuntime.GetConfigurationVersion(),
				jobRuntime.GetDesiredStateVersion(),
				jobRuntime.GetWorkflowVersion(),
			),
		}, nil
	}
}

func (h *serviceHandler) DeleteJob(
	ctx context.Context,
	req *svc.DeleteJobRequest,
) (resp *svc.DeleteJobResponse, err error) {
	defer func() {
		if err != nil {
			log.WithField("request", req).
				WithError(err).
				Warn("JobSVC.DeleteJob failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			Info("JobSVC.DeleteJob succeeded")
	}()

	if !h.candidate.IsLeader() {
		return nil, yarpcerrors.UnavailableErrorf("JobSVC.DeleteJob is not supported on non-leader")
	}

	cachedJob := h.jobFactory.AddJob(&peloton.JobID{
		Value: req.GetJobId().GetValue(),
	})

	count := 0
	for {
		runtime, err := cachedJob.GetRuntime(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get job runtime")
		}

		entityVersion := jobutil.GetJobEntityVersion(
			runtime.GetConfigurationVersion(),
			runtime.GetDesiredStateVersion(),
			runtime.GetWorkflowVersion(),
		)
		if entityVersion.GetValue() !=
			req.GetVersion().GetValue() {
			return nil, jobmgrcommon.InvalidEntityVersionError
		}

		if !req.GetForce() && runtime.GetState() != pbjob.JobState_KILLED {
			return nil, yarpcerrors.AbortedErrorf("job is not stopped")
		}

		runtime.GoalState = pbjob.JobState_DELETED
		runtime.DesiredStateVersion++

		if runtime, err = cachedJob.CompareAndSetRuntime(ctx, runtime); err != nil {
			if err == jobmgrcommon.UnexpectedVersionError {
				// concurrency error; retry MaxConcurrencyErrorRetry times
				count = count + 1
				if count < jobmgrcommon.MaxConcurrencyErrorRetry {
					continue
				}
			}
			// it is uncertain whether job runtime is updated successfully,
			// let goal state engine figure it out.
			h.goalStateDriver.EnqueueJob(cachedJob.ID(), time.Now())
			return nil, errors.Wrap(err, "fail to update job runtime")
		}

		h.goalStateDriver.EnqueueJob(cachedJob.ID(), time.Now())
		return &svc.DeleteJobResponse{}, nil
	}
}

func (h *serviceHandler) getJobSummary(
	ctx context.Context,
	jobID *v1alphapeloton.JobID) (*svc.GetJobResponse, error) {
	jobSummary, err := h.jobIndexOps.GetSummary(
		ctx, &peloton.JobID{Value: jobID.GetValue()})
	if err != nil {
		if err == gocql.ErrNotFound {
			return nil, yarpcerrors.NotFoundErrorf("job:%s not found", jobID)
		}
		return nil, errors.Wrap(err, "failed to get job summary from DB")
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
	configVersion, _, _, err := jobutil.ParseJobEntityVersion(version)
	if err != nil {
		return nil, err
	}

	jobConfig, _, err := h.jobStore.GetJobConfigWithVersion(
		ctx,
		jobID.GetValue(),
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
				Warn("StatelessJobSvc.GetJob failed")
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
		req.GetJobId().GetValue(),
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
		req.GetJobId().GetValue(),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get job status")
	}

	var updateInfo *models.UpdateModel
	var workflowEvents []*stateless.WorkflowEvent
	if len(jobRuntime.GetUpdateID().GetValue()) > 0 {
		updateInfo, err = h.updateStore.GetUpdate(
			ctx,
			jobRuntime.GetUpdateID(),
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get update information")
		}

		workflowEvents, err = h.updateStore.GetJobUpdateEvents(
			ctx,
			jobRuntime.GetUpdateID())
		if err != nil {
			return nil, errors.Wrap(err, "fail to get job update events")
		}
	}

	return &svc.GetJobResponse{
		JobInfo: &stateless.JobInfo{
			JobId:  req.GetJobId(),
			Spec:   handlerutil.ConvertJobConfigToJobSpec(jobConfig),
			Status: handlerutil.ConvertRuntimeInfoToJobStatus(jobRuntime, updateInfo),
		},
		Secrets: handlerutil.ConvertV0SecretsToV1Secrets(
			jobmgrtask.CreateSecretsFromVolumes(secretVolumes)),
		WorkflowInfo: handlerutil.ConvertUpdateModelToWorkflowInfo(
			jobRuntime,
			updateInfo,
			workflowEvents,
			nil),
	}, nil
}

// GetJobIDFromJobName looks up job ids for provided job name and
// job ids are returned in descending create timestamp
func (h *serviceHandler) GetJobIDFromJobName(
	ctx context.Context,
	req *svc.GetJobIDFromJobNameRequest) (resp *svc.GetJobIDFromJobNameResponse, err error) {
	defer func() {
		if err != nil {
			log.WithField("request", req).
				WithError(err).
				Warn("StatelessJobSvc.GetJobIDFromJobName failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("req", req).
			Debug("StatelessJobSvc.GetJobIDFromJobName succeeded")
	}()

	jobNameToIDs, err := h.jobNameToIDOps.GetAll(ctx, req.GetJobName())
	if err != nil {
		return nil, errors.Wrap(err, "failed to get job identifiers from job name")
	}

	if len(jobNameToIDs) == 0 {
		return nil, yarpcerrors.NotFoundErrorf("job id for job name not found: \"%s\"", req.GetJobName())
	}

	var jobIDs []*v1alphapeloton.JobID
	for _, jobNameToID := range jobNameToIDs {
		jobIDs = append(jobIDs, &v1alphapeloton.JobID{
			Value: jobNameToID.JobID,
		})
	}

	return &svc.GetJobIDFromJobNameResponse{
		JobId: jobIDs,
	}, nil
}

// GetWorkflowEvents gets most recent workflow events for an instance of a job
func (h *serviceHandler) GetWorkflowEvents(
	ctx context.Context,
	req *svc.GetWorkflowEventsRequest) (resp *svc.GetWorkflowEventsResponse, err error) {
	defer func() {
		if err != nil {
			log.WithField("request", req).
				WithError(err).
				Warn("StatelessJobSvc.GetWorkflowEvents failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("req", req).
			Debug("StatelessJobSvc.GetWorkflowEvents succeeded")
	}()

	jobUUID := uuid.Parse(req.GetJobId().GetValue())
	if jobUUID == nil {
		return nil, yarpcerrors.InvalidArgumentErrorf("job ID must be of UUID format")
	}

	jobID := &peloton.JobID{Value: req.GetJobId().GetValue()}
	cachedJob := h.jobFactory.AddJob(jobID)
	jobRuntime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get job runtime")
	}

	// check if any active or completed workflow exists for this job
	if len(jobRuntime.GetUpdateID().GetValue()) == 0 {
		return nil, yarpcerrors.UnavailableErrorf("job runtime does not have workflow")
	}

	workflowEvents, err := h.updateStore.GetWorkflowEvents(
		ctx,
		jobRuntime.GetUpdateID(),
		req.GetInstanceId())
	if err != nil {
		return nil, errors.Wrap(err,
			fmt.Sprintf("failed to get workflow events for an update %s",
				jobRuntime.GetUpdateID().GetValue()))
	}

	return &svc.GetWorkflowEventsResponse{
		Events: workflowEvents,
	}, nil
}

func (h *serviceHandler) ListPods(
	req *svc.ListPodsRequest,
	stream svc.JobServiceServiceListPodsYARPCServer,
) (err error) {
	var instanceRange *task.InstanceRange

	defer func() {
		if err != nil {
			log.WithError(err).
				WithField("job_id", req.GetJobId().GetValue()).
				Warn("JobSVC.ListPods failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("job_id", req.GetJobId().GetValue()).
			Debug("JobSVC.ListPods succeeded")
	}()

	if req.GetRange() != nil {
		instanceRange = &task.InstanceRange{
			From: req.GetRange().GetFrom(),
			To:   req.GetRange().GetTo(),
		}
	}

	taskRuntimes, err := h.taskStore.GetTaskRuntimesForJobByRange(
		context.Background(),
		&peloton.JobID{Value: req.GetJobId().GetValue()},
		instanceRange,
	)
	if err != nil {
		return errors.Wrap(err, "failed to get tasks")
	}

	for instID, taskRuntime := range taskRuntimes {
		resp := &svc.ListPodsResponse{
			Pods: []*pod.PodSummary{
				{
					PodName: &v1alphapeloton.PodName{
						Value: util.CreatePelotonTaskID(req.GetJobId().GetValue(), instID),
					},
					Status: handlerutil.ConvertTaskRuntimeToPodStatus(taskRuntime),
				},
			},
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}

	return nil
}

func (h *serviceHandler) QueryPods(
	ctx context.Context,
	req *svc.QueryPodsRequest,
) (resp *svc.QueryPodsResponse, err error) {
	defer func() {
		if err != nil {
			log.WithField("request", req).
				WithError(err).
				Warn("JobSVC.QueryPods failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("num_of_results", len(resp.GetPods())).
			Debug("JobSVC.QueryPods succeeded")
	}()

	pelotonJobID := &peloton.JobID{Value: req.GetJobId().GetValue()}

	_, _, err = h.jobStore.GetJobConfig(
		ctx,
		req.GetJobId().GetValue(),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to find job")
	}

	taskQuerySpec := handlerutil.ConvertPodQuerySpecToTaskQuerySpec(
		req.GetSpec(),
	)
	taskInfos, total, err := h.taskStore.QueryTasks(ctx, pelotonJobID, taskQuerySpec)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query tasks from DB")
	}

	h.fillReasonForPendingTasksFromResMgr(
		ctx,
		req.GetJobId().GetValue(),
		taskInfos,
	)

	return &svc.QueryPodsResponse{
		Pods: handlerutil.ConvertTaskInfosToPodInfos(taskInfos),
		Pagination: &v1alphaquery.Pagination{
			Offset: req.GetPagination().GetOffset(),
			Limit:  req.GetPagination().GetLimit(),
			Total:  total,
		},
	}, nil
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
	log.WithField("spec", querySpec).Debug("converted spec")

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
		Pagination: &v1alphaquery.Pagination{
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
				Warn("JobSVC.ListJobs failed")
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

func (h *serviceHandler) ListJobWorkflows(
	ctx context.Context,
	req *svc.ListJobWorkflowsRequest) (resp *svc.ListJobWorkflowsResponse, err error) {
	defer func() {
		if err != nil {
			log.WithField("request", req).
				WithError(err).
				Warn("JobSVC.ListJobWorkflows failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("num_of_workflows", len(resp.GetWorkflowInfos())).
			Debug("JobSVC.ListJobWorkflows succeeded")
	}()

	if len(req.GetJobId().GetValue()) == 0 {
		return nil, yarpcerrors.InvalidArgumentErrorf("no job id provided")
	}

	updateIDs, err := h.updateStore.GetUpdatesForJob(ctx, req.GetJobId().GetValue())
	if err != nil {
		return nil, err
	}

	if req.GetUpdatesLimit() > 0 {
		updateIDs = updateIDs[:util.Min(uint32(len(updateIDs)), req.GetUpdatesLimit())]
	}

	var updateInfos []*stateless.WorkflowInfo

	jobRuntime, err := h.jobStore.GetJobRuntime(ctx, req.GetJobId().GetValue())
	if err != nil {
		return nil, errors.Wrap(err, "fail to get job runtime")
	}
	for _, updateID := range updateIDs {
		updateModel, err := h.updateStore.GetUpdate(ctx, updateID)
		if err != nil {
			return nil, err
		}

		workflowEvents, err := h.updateStore.GetJobUpdateEvents(
			ctx,
			updateID)
		if err != nil {
			return nil, errors.Wrap(err, "fail to get job workflow events")
		}

		var instanceWorkflowEvents []*stateless.WorkflowInfoInstanceWorkflowEvents
		if req.GetInstanceEvents() {
			instanceWorkflowEvents, err = h.getInstanceWorkflowEvents(
				ctx,
				updateModel)
			if err != nil {
				return nil, errors.Wrap(err, "fail to get instance workflow events")
			}
		}

		updateInfos = append(updateInfos,
			handlerutil.ConvertUpdateModelToWorkflowInfo(
				jobRuntime,
				updateModel,
				workflowEvents,
				instanceWorkflowEvents))
	}

	return &svc.ListJobWorkflowsResponse{WorkflowInfos: updateInfos}, nil
}

// getInstanceWorkflowEvents gets the workflow events for instances that were
// included in an update or restart.
// Fixed number of go routine workers are spawed to get per instance workflow
// events, and if an error occurs all workers are aborted.
func (h *serviceHandler) getInstanceWorkflowEvents(
	ctx context.Context,
	updateModel *models.UpdateModel,
) ([]*stateless.WorkflowInfoInstanceWorkflowEvents, error) {

	f := func(ctx context.Context, instance_id interface{}) (interface{}, error) {
		workflowEvents, err := h.updateStore.GetWorkflowEvents(
			ctx,
			updateModel.GetUpdateID(),
			instance_id.(uint32))
		if err != nil {
			return nil, errors.Wrap(err,
				fmt.Sprintf("failed to get workflow events for update %s, instance %d",
					updateModel.GetUpdateID(),
					instance_id.(uint32)))
		}

		return &stateless.WorkflowInfoInstanceWorkflowEvents{
			InstanceId: instance_id.(uint32),
			Events:     workflowEvents,
		}, nil
	}

	instances := append(updateModel.GetInstancesAdded(), updateModel.GetInstancesRemoved()...)
	instances = append(instances, updateModel.GetInstancesUpdated()...)

	var inputs []interface{}
	for _, i := range instances {
		inputs = append(inputs, i)
	}

	outputs, err := concurrency.Map(
		ctx,
		concurrency.MapperFunc(f),
		inputs,
		_defaultInstanceWorkflowEventsWorker)
	if err != nil {
		return nil, err
	}

	var events []*stateless.WorkflowInfoInstanceWorkflowEvents
	for _, o := range outputs {
		events = append(events, o.(*stateless.WorkflowInfoInstanceWorkflowEvents))
	}

	return events, nil
}

func (h *serviceHandler) GetReplaceJobDiff(
	ctx context.Context,
	req *svc.GetReplaceJobDiffRequest,
) (resp *svc.GetReplaceJobDiffResponse, err error) {
	defer func() {
		jobID := req.GetJobId().GetValue()
		entityVersion := req.GetVersion().GetValue()

		if err != nil {
			log.WithField("job_id", jobID).
				WithField("entity_version", entityVersion).
				WithError(err).
				Warn("JobSVC.GetReplaceJobDiff failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("job_id", jobID).
			WithField("entity_version", entityVersion).
			Debug("JobSVC.GetReplaceJobDiff succeeded")
	}()

	jobUUID := uuid.Parse(req.GetJobId().GetValue())
	if jobUUID == nil {
		return nil, yarpcerrors.InvalidArgumentErrorf(
			"JobID must be of UUID format")
	}

	jobID := &peloton.JobID{Value: req.GetJobId().GetValue()}
	cachedJob := h.jobFactory.AddJob(jobID)
	jobRuntime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get job status")
	}

	if err := cachedJob.ValidateEntityVersion(
		ctx,
		req.GetVersion(),
	); err != nil {
		return nil, err
	}

	jobConfig, err := handlerutil.ConvertJobSpecToJobConfig(req.GetSpec())
	if err != nil {
		return nil, err
	}

	prevJobConfig, _, err := h.jobStore.GetJobConfigWithVersion(
		ctx,
		jobID.GetValue(),
		jobRuntime.GetConfigurationVersion())
	if err != nil {
		return nil, errors.Wrap(err, "failed to get previous configuration")
	}

	if err := validateJobConfigUpdate(prevJobConfig, jobConfig); err != nil {
		return nil, err
	}

	added, updated, removed, unchanged, err :=
		cached.GetInstancesToProcessForUpdate(
			ctx,
			jobID,
			prevJobConfig,
			jobConfig,
			h.taskStore,
		)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get configuration difference")
	}

	return &svc.GetReplaceJobDiffResponse{
		InstancesAdded:     util.ConvertInstanceIDListToInstanceRange(added),
		InstancesRemoved:   util.ConvertInstanceIDListToInstanceRange(removed),
		InstancesUpdated:   util.ConvertInstanceIDListToInstanceRange(updated),
		InstancesUnchanged: util.ConvertInstanceIDListToInstanceRange(unchanged),
	}, nil
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

	jobConfig, configAddOn, err := h.jobStore.GetJobConfig(ctx, req.GetJobId().GetValue())
	if err != nil {
		return nil, errors.Wrap(err, "fail to get job config")
	}

	jobRuntime, err := h.jobStore.GetJobRuntime(ctx, req.GetJobId().GetValue())
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
		runtime.GetDesiredStateVersion(),
		runtime.GetWorkflowVersion())
	return result
}

func convertCacheToWorkflowStatus(
	cachedWorkflow cached.Update,
) *stateless.WorkflowStatus {
	if cachedWorkflow == nil {
		return nil
	}

	workflowStatus := &stateless.WorkflowStatus{}
	workflowStatus.Type = stateless.WorkflowType(cachedWorkflow.GetWorkflowType())
	workflowStatus.State = stateless.WorkflowState(cachedWorkflow.GetState().State)
	workflowStatus.PrevState = stateless.WorkflowState(cachedWorkflow.GetPrevState())
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

// validateResourcePoolForJobCreation validates the resource pool before submitting job
func (h *serviceHandler) validateResourcePoolForJobCreation(
	ctx context.Context,
	respoolID *v1alphapeloton.ResourcePoolID,
) (*respool.ResourcePoolPath, error) {
	if respoolID == nil {
		return nil, errNullResourcePoolID
	}

	if respoolID.GetValue() == common.RootResPoolID {
		return nil, errRootResourcePoolID
	}

	request := &respool.GetRequest{
		Id: &peloton.ResourcePoolID{Value: respoolID.GetValue()},
	}
	response, err := h.respoolClient.GetResourcePool(ctx, request)
	if err != nil {
		return nil, err
	}

	if response.GetPoolinfo().GetId() == nil ||
		response.GetPoolinfo().GetId().GetValue() != respoolID.GetValue() {
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
	spec *stateless.JobSpec, secrets []*v1alphapeloton.Secret) error {
	// validate secrets payload for input sanity
	if len(secrets) == 0 {
		return nil
	}

	config, err := handlerutil.ConvertJobSpecToJobConfig(spec)
	if err != nil {
		return err
	}
	// make sure that config doesn't have any secret volumes
	if util.ConfigHasSecretVolumes(config.GetDefaultConfig()) {
		return yarpcerrors.InvalidArgumentErrorf(
			"adding secret volumes directly in config is not allowed",
		)
	}

	if !h.jobSvcCfg.EnableSecrets && len(secrets) > 0 {
		return yarpcerrors.InvalidArgumentErrorf(
			"secrets not enabled in cluster",
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
				"failed to decode secret with error: %v", err,
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
func validateMesosContainerizerForSecrets(jobSpec *stateless.JobSpec) error {
	// make sure that default config uses mesos containerizer
	for _, container := range jobSpec.GetDefaultSpec().GetContainers() {
		if container.GetContainer().GetType() != mesos.ContainerInfo_MESOS {
			return yarpcerrors.InvalidArgumentErrorf(
				"container type %v does not match %v",
				jobSpec.GetDefaultSpec().GetContainers()[0].GetContainer().GetType(),
				mesos.ContainerInfo_MESOS,
			)
		}
	}
	return nil
}

// handleCreateSecrets handles secrets to be added at the time of creating a job
func (h *serviceHandler) handleCreateSecrets(
	ctx context.Context, jobID string,
	spec *stateless.JobSpec, secrets []*v1alphapeloton.Secret,
) error {
	// if there are no secrets in the request,
	// job create doesn't need to handle secrets
	if len(secrets) == 0 {
		return nil
	}
	// Make sure that the default config is using Mesos containerizer
	if err := validateMesosContainerizerForSecrets(spec); err != nil {
		return err
	}
	// for each secret, store it in DB and add a secret volume to defaultconfig
	return h.addSecretsToDBAndConfig(ctx, jobID, spec, secrets, false)
}

func (h *serviceHandler) addSecretsToDBAndConfig(
	ctx context.Context, jobID string, jobSpec *stateless.JobSpec,
	secrets []*v1alphapeloton.Secret, update bool) error {
	// for each secret, store it in DB and add a secret volume to defaultconfig
	for _, secret := range handlerutil.ConvertV1SecretsToV0Secrets(secrets) {
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
				jobID,
				string(secret.Value.Data),
			); err != nil {
				return err
			}
		} else {
			if err := h.secretInfoOps.CreateSecret(
				ctx,
				jobID,
				time.Now(),
				secret.Id.Value,
				string(secret.Value.Data),
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
		for _, container := range jobSpec.GetDefaultSpec().GetContainers() {
			container.GetContainer().Volumes =
				append(container.GetContainer().Volumes,
					util.CreateSecretVolume(secret.GetPath(),
						secret.GetId().GetValue()),
				)
		}
	}
	return nil
}

// convertInstanceIDRangesToSlice merges ranges into a single slice and remove
// any duplicated item. Instance count is needed because cli may send max uint32
// when range is not specified.
func convertInstanceIDRangesToSlice(ranges []*pod.InstanceIDRange, instanceCount uint32) []uint32 {
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

// TODO: remove this function once eventstream is enabled in RM
// fillReasonForPendingTasksFromResMgr takes a list of taskinfo and
// fills in the reason for pending tasks from ResourceManager.
// All the tasks in `taskInfos` should belong to the same job
func (h *serviceHandler) fillReasonForPendingTasksFromResMgr(
	ctx context.Context,
	jobID string,
	taskInfos []*task.TaskInfo,
) {
	// only need to consult ResourceManager for PENDING tasks,
	// because only tasks with PENDING states are being processed by ResourceManager
	for _, taskInfo := range taskInfos {
		if taskInfo.GetRuntime().GetState() == task.TaskState_PENDING {
			// attach the reason from the taskEntry in activeRMTasks
			taskEntry := h.activeRMTasks.GetTask(
				util.CreatePelotonTaskID(
					jobID,
					taskInfo.GetInstanceId(),
				),
			)
			if taskEntry != nil {
				taskInfo.GetRuntime().Reason = taskEntry.GetReason()
			}
		}
	}
}
