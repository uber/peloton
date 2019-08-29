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

package private

import (
	"context"
	"time"

	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	v1alphapeloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/private/jobmgrsvc"

	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/common/leader"
	"github.com/uber/peloton/pkg/common/util"
	versionutil "github.com/uber/peloton/pkg/common/util/entityversion"
	yarpcutil "github.com/uber/peloton/pkg/common/util/yarpc"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	"github.com/uber/peloton/pkg/jobmgr/goalstate"
	"github.com/uber/peloton/pkg/storage"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"

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
	jobConfigOps    ormobjects.JobConfigOps
	jobRuntimeOps   ormobjects.JobRuntimeOps
	jobNameToIDOps  ormobjects.JobNameToIDOps
	jobFactory      cached.JobFactory
	goalStateDriver goalstate.Driver
	candidate       leader.Candidate
	rootCtx         context.Context
}

// InitPrivateJobServiceHandler initializes the Job
// Manager's private API Service Handler
func InitPrivateJobServiceHandler(
	d *yarpc.Dispatcher,
	jobStore storage.JobStore,
	updateStore storage.UpdateStore,
	taskStore storage.TaskStore,
	ormStore *ormobjects.Store,
	jobFactory cached.JobFactory,
	goalStateDriver goalstate.Driver,
	candidate leader.Candidate,
) {
	handler := &serviceHandler{
		jobStore:        jobStore,
		updateStore:     updateStore,
		taskStore:       taskStore,
		jobIndexOps:     ormobjects.NewJobIndexOps(ormStore),
		jobConfigOps:    ormobjects.NewJobConfigOps(ormStore),
		jobRuntimeOps:   ormobjects.NewJobRuntimeOps(ormStore),
		jobNameToIDOps:  ormobjects.NewJobNameToIDOps(ormStore),
		jobFactory:      jobFactory,
		goalStateDriver: goalStateDriver,
		candidate:       candidate,
	}
	d.Register(jobmgrsvc.BuildJobManagerServiceYARPCProcedures(handler))
}

func (h *serviceHandler) GetThrottledPods(
	ctx context.Context,
	req *jobmgrsvc.GetThrottledPodsRequest,
) (resp *jobmgrsvc.GetThrottledPodsResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		if err != nil {
			log.WithField("request", req).
				WithField("headers", headers).
				WithError(err).
				Info("JobSVC.GetThrottledTasks failed")
			err = yarpcutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("headers", headers).
			Debug("JobSVC.GetThrottledTasks succeeded")
	}()

	var throttledPods []*v1alphapeloton.PodName
	cachedJobs := h.jobFactory.GetAllJobs()

	for jobID, cachedJob := range cachedJobs {
		cachedConfig, tmpErr := cachedJob.GetConfig(ctx)
		if tmpErr != nil {
			log.WithError(tmpErr).
				WithField("job_id", jobID).
				Info("Failed to get job config during fetching throttled pods")
			continue
		}

		if cachedConfig.GetType() != pbjob.JobType_SERVICE {
			continue
		}

		cachedTasks := cachedJob.GetAllTasks()
		for instID, cachedTask := range cachedTasks {
			runtime, tmpErr := cachedTask.GetRuntime(ctx)
			if tmpErr != nil {
				log.WithError(tmpErr).
					WithFields(log.Fields{
						"job_id":      jobID,
						"instance_id": instID,
					}).
					Info("Failed to get task runtime during fetching throttled pods")
				continue
			}

			if util.IsTaskThrottled(runtime.GetState(), runtime.GetMessage()) {
				podName := &v1alphapeloton.PodName{
					Value: util.CreatePelotonTaskID(jobID, instID),
				}
				throttledPods = append(throttledPods, podName)
			}
		}
	}

	resp = &jobmgrsvc.GetThrottledPodsResponse{
		ThrottledPods: throttledPods,
	}
	return
}

func (h *serviceHandler) RefreshJob(
	ctx context.Context,
	req *jobmgrsvc.RefreshJobRequest) (resp *jobmgrsvc.RefreshJobResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		if err != nil {
			log.WithField("request", req).
				WithField("headers", headers).
				WithError(err).
				Warn("JobSVC.RefreshJob failed")
			err = yarpcutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
			WithField("headers", headers).
			Info("JobSVC.RefreshJob succeeded")
	}()

	if !h.candidate.IsLeader() {
		return nil,
			yarpcerrors.UnavailableErrorf("JobSVC.RefreshJob is not supported on non-leader")
	}

	pelotonJobID := &peloton.JobID{Value: req.GetJobId().GetValue()}

	jobRuntime, err := h.jobRuntimeOps.Get(ctx, pelotonJobID)
	if err != nil {
		return nil, errors.Wrap(err, "fail to get job runtime")
	}

	jobConfig, configAddOn, err := h.jobConfigOps.Get(
		ctx,
		&peloton.JobID{Value: req.GetJobId().GetValue()},
		jobRuntime.GetConfigurationVersion())
	if err != nil {
		return nil, errors.Wrap(err, "fail to get job config")
	}

	cachedJob := h.jobFactory.AddJob(pelotonJobID)
	cachedJob.Update(ctx, &pbjob.JobInfo{
		Config:  jobConfig,
		Runtime: jobRuntime,
	}, configAddOn,
		nil,
		cached.UpdateCacheOnly)
	h.goalStateDriver.EnqueueJob(pelotonJobID, time.Now())
	return &jobmgrsvc.RefreshJobResponse{}, nil
}

func (h *serviceHandler) GetJobCache(
	ctx context.Context,
	req *jobmgrsvc.GetJobCacheRequest) (resp *jobmgrsvc.GetJobCacheResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		if err != nil {
			log.WithField("request", req).
				WithField("headers", headers).
				WithError(err).
				Warn("JobSVC.GetJobCache failed")
			err = yarpcutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
			WithField("headers", headers).
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

	return &jobmgrsvc.GetJobCacheResponse{
		Spec:   convertCacheJobConfigToJobSpec(config),
		Status: status,
	}, nil
}

func (h *serviceHandler) QueryJobCache(
	ctx context.Context,
	req *jobmgrsvc.QueryJobCacheRequest,
) (resp *jobmgrsvc.QueryJobCacheResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		if err != nil {
			log.WithField("request", req).
				WithField("headers", headers).
				WithError(err).
				Warn("JobSVC.QueryJobCache failed")
			err = yarpcutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("num_of_results", len(resp.GetResult())).
			WithField("headers", headers).
			Debug("JobSVC.QueryJobCache succeeded")
	}()

	if !h.goalStateDriver.Started() {
		return nil, yarpcerrors.UnavailableErrorf(
			"QueryJobCache is not available until goal state driver finish start process")
	}

	var result []*jobmgrsvc.QueryJobCacheResponse_JobCache
	jobs := h.jobFactory.GetAllJobs()

	for _, job := range jobs {
		cachedConfig, err := job.GetConfig(ctx)

		// job may be removed from db but not yet cleaned
		// up from job factory
		if err != nil && yarpcerrors.IsNotFound(err) {
			continue
		}

		// for other kind of error, directly return from handler
		if err != nil {
			return nil, err
		}

		if nameMatch(cachedConfig.GetName(), req.GetSpec().GetName()) &&
			labelMatch(api.ConvertLabels(cachedConfig.GetLabels()), req.GetSpec().GetLabels()) {
			result = append(result, &jobmgrsvc.QueryJobCacheResponse_JobCache{
				JobId: &v1alphapeloton.JobID{Value: job.ID().GetValue()},
				Name:  cachedConfig.GetName(),
			})
		}
	}

	return &jobmgrsvc.QueryJobCacheResponse{Result: result}, nil
}

func (h *serviceHandler) GetInstanceAvailabilityInfoForJob(
	ctx context.Context,
	req *jobmgrsvc.GetInstanceAvailabilityInfoForJobRequest,
) (resp *jobmgrsvc.GetInstanceAvailabilityInfoForJobResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		if err != nil {
			log.WithField("request", req).
				WithField("headers", headers).
				WithError(err).
				Warn("JobSVC.GetInstanceAvailabilityInfoForJob failed")
			err = yarpcutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
			WithField("headers", headers).
			Debug("JobSVC.GetInstanceAvailabilityInfoForJob succeeded")
	}()

	job := h.jobFactory.GetJob(&peloton.JobID{Value: req.GetJobId().GetValue()})

	instanceAvailabilityMap := make(map[uint32]string)
	for i, t := range job.GetInstanceAvailabilityType(ctx, req.GetInstances()...) {
		instanceAvailabilityMap[i] = jobmgrcommon.InstanceAvailability_name[t]
	}
	return &jobmgrsvc.GetInstanceAvailabilityInfoForJobResponse{
		InstanceAvailabilityMap: instanceAvailabilityMap,
	}, nil
}

// nameMatch returns true if queryName not set, or jobName
// and queryName are the same
func nameMatch(jobName string, queryName string) bool {
	if len(queryName) == 0 {
		return true
	}

	return jobName == queryName
}

// labelMatch returns if jobLabels contains all elements in queryLables
func labelMatch(jobLabels []*v1alphapeloton.Label, queryLabels []*v1alphapeloton.Label) bool {
	if len(queryLabels) == 0 {
		return true
	}

	labelMap := constructLabelsMap(jobLabels)
	for _, l := range queryLabels {
		if v, ok := labelMap[l.GetKey()]; !ok {
			return false
		} else if v != l.GetValue() {
			return false
		}
	}

	return true
}

func constructLabelsMap(labels []*v1alphapeloton.Label) map[string]string {
	result := make(map[string]string)
	for _, label := range labels {
		result[label.GetKey()] = label.GetValue()
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
	result.PodStats = api.ConvertTaskStatsToPodStats(runtime.TaskStats)
	result.DesiredState = stateless.JobState(runtime.GetGoalState())
	result.Version = versionutil.GetJobEntityVersion(
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
	workflowStatus.PrevVersion = versionutil.GetPodEntityVersion(cachedWorkflow.GetState().JobVersion)
	workflowStatus.Version = versionutil.GetPodEntityVersion(cachedWorkflow.GetGoalState().JobVersion)
	return workflowStatus
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

// NewTestServiceHandler returns an empty new ServiceHandler ptr for testing.
func NewTestServiceHandler() *serviceHandler {
	return &serviceHandler{}
}
