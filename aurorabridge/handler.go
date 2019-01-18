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

package aurorabridge

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	statelesssvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	podsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/svc"
	"github.com/uber/peloton/.gen/thrift/aurora/api"
	"github.com/uber/peloton/common/async"

	"github.com/uber/peloton/aurorabridge/atop"
	"github.com/uber/peloton/aurorabridge/label"
	"github.com/uber/peloton/aurorabridge/ptoa"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/thriftrw/ptr"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	// number of workers to create in async pool, which defines number of
	// getJobUpdateDetails can be processed in parallel
	_defaultGetJobUpdateWorkers = 25
)

var errUnimplemented = errors.New("rpc is unimplemented")

// ServiceHandler implements a partial Aurora API. Various unneeded methods have
// been left intentionally unimplemented.
type ServiceHandler struct {
	metrics   *Metrics
	jobClient statelesssvc.JobServiceYARPCClient
	podClient podsvc.PodServiceYARPCClient
	respoolID *peloton.ResourcePoolID
}

// NewServiceHandler creates a new ServiceHandler.
func NewServiceHandler(
	parent tally.Scope,
	jobClient statelesssvc.JobServiceYARPCClient,
	podClient podsvc.PodServiceYARPCClient,
	respoolID *peloton.ResourcePoolID,
) *ServiceHandler {
	return &ServiceHandler{
		metrics:   NewMetrics(parent.SubScope("aurorabridge").SubScope("api")),
		jobClient: jobClient,
		podClient: podClient,
		respoolID: respoolID,
	}
}

// GetJobSummary returns a summary of jobs, optionally only those owned by a specific role.
func (h *ServiceHandler) GetJobSummary(
	ctx context.Context,
	role *string) (*api.Response, error) {
	return nil, errUnimplemented
}

// GetTasksWithoutConfigs is the same as getTasksStatus but without the TaskConfig.ExecutorConfig
// data set.
func (h *ServiceHandler) GetTasksWithoutConfigs(
	ctx context.Context,
	query *api.TaskQuery,
) (*api.Response, error) {

	result, err := h.getTasksWithoutConfigs(ctx, query)
	if err != nil {
		log.WithFields(log.Fields{
			"params": log.Fields{
				"query": query,
			},
			"code":  err.responseCode,
			"error": err.msg,
		}).Error("GetTasksWithoutConfigs error")
	}
	return newResponse(result, err), nil
}

func (h *ServiceHandler) getTasksWithoutConfigs(
	ctx context.Context,
	query *api.TaskQuery,
) (*api.Result, *auroraError) {

	var scheduledTasks []*api.ScheduledTask
	var podStates []pod.PodState

	for s := range query.GetStatuses() {
		p, err := atop.NewPodState(s)
		if err != nil {
			return nil, auroraErrorf("new pod state: %s", err)
		}
		podStates = append(podStates, p)
	}

	jobIDs, err := h.getJobIDsFromTaskQuery(ctx, query)
	if err != nil {
		return nil, auroraErrorf("get job ids from task query: %s", err)
	}

	for _, jobID := range jobIDs {
		jobInfo, err := h.getJobInfo(ctx, jobID)
		if err != nil {
			return nil, auroraErrorf("get job info for job id %q: %s",
				jobID.GetValue(), err)
		}

		// TODO(kevinxu): QueryPods api only returns pods from latest run,
		// while aurora returns tasks from all previous runs. Do we need
		// to make it consistent with aurora's behavior?
		pods, err := h.queryPods(ctx, jobID, podStates)
		if err != nil {
			return nil, auroraErrorf(
				"query pods for job id %q with pod states %q: %s",
				jobID.GetValue(), podStates, err)
		}

		// TODO(kevinxu): make the calls to query pods parallel
		for _, podInfo := range pods {
			n := podInfo.GetSpec().GetPodName()
			podEvents, err := h.getPodEvents(ctx, n)
			if err != nil {
				return nil, auroraErrorf("get pod events for pod %q: %s",
					n.GetValue(), err)
			}

			t, err := ptoa.NewScheduledTask(jobInfo, podInfo, podEvents)
			if err != nil {
				return nil, auroraErrorf("new scheduled task: %s", err)
			}

			scheduledTasks = append(scheduledTasks, t)
		}
	}

	return &api.Result{
		ScheduleStatusResult: &api.ScheduleStatusResult{
			Tasks: scheduledTasks,
		},
	}, nil
}

// GetConfigSummary fetches the configuration summary of active tasks for the specified job.
func (h *ServiceHandler) GetConfigSummary(
	ctx context.Context,
	job *api.JobKey) (*api.Response, error) {

	result, err := h.getConfigSummary(ctx, job)
	if err != nil {
		log.WithFields(log.Fields{
			"params": log.Fields{
				"job": job,
			},
			"code":  err.responseCode,
			"error": err.msg,
		}).Error("GetConfigSummary error")
	}
	return newResponse(result, err), nil
}

func (h *ServiceHandler) getConfigSummary(
	ctx context.Context,
	jobKey *api.JobKey,
) (*api.Result, *auroraError) {
	jobID, err := h.getJobID(
		ctx,
		jobKey)
	if err != nil {
		return nil, auroraErrorf("unable to get jobID from jobKey: %s", err)
	}

	podInfos, err := h.queryPods(
		ctx,
		jobID,
		nil)
	if err != nil {
		return nil, auroraErrorf("unable to query pods using jobID: %s", err)
	}

	configSummary, err := ptoa.NewConfigSummary(
		jobKey,
		podInfos)
	if err != nil {
		return nil, auroraErrorf("unable to get config summary from podInfos: %s", err)
	}

	return &api.Result{
		ConfigSummaryResult: &api.ConfigSummaryResult{
			Summary: configSummary,
		},
	}, nil
}

// GetJobs fetches the status of jobs. ownerRole is optional, in which case all jobs are returned.
func (h *ServiceHandler) GetJobs(
	ctx context.Context,
	ownerRole *string) (*api.Response, error) {
	return nil, errUnimplemented
}

// GetJobUpdateSummaries gets job update summaries.
// This is the sequence in which jobUpdateQuery filters updates.
// - Get JobIDs using job key role and filter their updates
// - Get JobIDs using job key and filter their updates
// - If only update statuses are provided then filter all job updates
func (h *ServiceHandler) GetJobUpdateSummaries(
	ctx context.Context,
	jobUpdateQuery *api.JobUpdateQuery,
) (*api.Response, error) {

	jobUpdateDetails, err := h.getJobUpdateDetails(
		ctx,
		jobUpdateQuery,
		true, /* summary only */
	)
	if err != nil {
		log.WithFields(log.Fields{
			"params": log.Fields{
				"key": jobUpdateQuery,
			},
			"code":  err.responseCode,
			"error": err.msg,
		}).Error("GetJobUpdateSummaries error")
	}

	var jobUpdateSummaries []*api.JobUpdateSummary
	for _, jobUpdateDetail := range jobUpdateDetails {
		jobUpdateSummaries = append(jobUpdateSummaries, jobUpdateDetail.GetUpdate().GetSummary())
	}

	return newResponse(&api.Result{
		GetJobUpdateSummariesResult: &api.GetJobUpdateSummariesResult{
			UpdateSummaries: jobUpdateSummaries,
		},
	}, err), nil
}

// GetJobUpdateDetails gets job update details.
// jobUpdateKey is marked to be deprecated from Aurora, and not used Aggregator
// It will be ignored to get job update details
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
	key *api.JobUpdateKey,
) (*api.Response, error) {

	result, err := h.pulseJobUpdate(ctx, key)
	if err != nil {
		log.WithFields(log.Fields{
			"params": log.Fields{
				"key": key,
			},
			"code":  err.responseCode,
			"error": err.msg,
		}).Error("PulseJobUpdate error")
	}
	return newResponse(result, err), nil
}

func (h *ServiceHandler) pulseJobUpdate(
	ctx context.Context,
	key *api.JobUpdateKey,
) (*api.Result, *auroraError) {

	// TODO(codyg): Leverage opaque data to replicate pulsed state. If
	// blockIfNoPulsesAfterMs is set, then the update will start in an AWAITING_PULSE
	// state. Updates in a PAUSED state cannot be pulsed, however updates in an
	// AWAITING_PULSE state can be pulsed, even though internally, both updates
	// are "paused".

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
	return &api.Result{
		PulseJobUpdateResult: &api.PulseJobUpdateResult{
			Status: api.JobUpdatePulseStatusOk.Ptr(),
		},
	}, nil
}

// Result represents output value returned on channel
// to get job update detail if not filtered
// and error if occured
type Result struct {
	detail     *api.JobUpdateDetails
	isFiltered bool /* set true, if job update state not in expected update statuses */
	err        error
}

// getJobUpdateDetails gets job detils concurrently
func (h *ServiceHandler) getJobUpdateDetails(
	ctx context.Context,
	jobUpdateQuery *api.JobUpdateQuery,
	summaryOnly bool,
) ([]*api.JobUpdateDetails, *auroraError) {
	var jobUpdateDetails []*api.JobUpdateDetails

	jobSummaries, err := h.getJobSummariesFromJobUpdateQuery(
		ctx,
		jobUpdateQuery)
	if err != nil {
		return jobUpdateDetails, nil
	}

	outchan := make(chan *Result)
	pool := async.NewPool(async.PoolOptions{
		MaxWorkers: _defaultGetJobUpdateWorkers,
	}, nil)
	pool.Start()

	getJobDetailsErrors := uint32(0)
	var wg sync.WaitGroup
	for _, jobSummary := range jobSummaries {
		wg.Add(1)
		go func(jobSummary *stateless.JobSummary) {
			defer wg.Done()
			pool.Enqueue(async.JobFunc(func(context.Context) {
				// stop fetching more getJobUpdateDetails on first error.
				if atomic.LoadUint32(&getJobDetailsErrors) > 0 {
					outchan <- &Result{}
					return
				}
				jobUpdateDetail, isFiltered, err := h.getJobUpdateDetail(
					ctx,
					jobUpdateQuery,
					jobSummary,
					summaryOnly)

				outchan <- &Result{
					detail:     jobUpdateDetail,
					isFiltered: isFiltered,
					err:        err,
				}
			}))
		}(jobSummary)
	}
	wg.Wait()

	for i := 0; i < len(jobSummaries); i++ {
		result := <-outchan

		if result.err != nil {
			log.WithError(result.err).Error("failed to get jobUpdateDetail from jobSummary")
			atomic.AddUint32(&getJobDetailsErrors, 1)
			continue
		}

		// isFiltered set to true, if workflow is in INITIALIZED state
		// or workflow state is filtered on provided update query
		if result.isFiltered == true {
			continue
		}

		jobUpdateDetails = append(jobUpdateDetails, result.detail)
	}

	if getJobDetailsErrors > 0 {
		return nil, auroraErrorf("failed to get jobUpdateDetails for %d jobs", getJobDetailsErrors)
	}

	return jobUpdateDetails, nil
}

// getJobUpdateDetail get details of most recent workflow of the job
func (h *ServiceHandler) getJobUpdateDetail(
	ctx context.Context,
	jobUpdateQuery *api.JobUpdateQuery,
	jobSummary *stateless.JobSummary,
	summaryOnly bool,
) (*api.JobUpdateDetails, bool, error) {

	// Get job update
	resp, err := h.jobClient.GetJobUpdate(
		ctx,
		&statelesssvc.GetJobUpdateRequest{
			JobId: jobSummary.GetJobId(),
		})
	if err != nil {
		return nil, false, fmt.Errorf("getJobUpdate failed: %s", err)
	}

	// INITIALIZED workflows are ignored to take action upon because Aurora
	// does not support this state
	// Assumption is that jobmgr's goal state engine will process these
	// workflows and take appropriate action to make progress on
	// workflow operations
	if resp.GetUpdateInfo().GetInfo().GetStatus().GetState() ==
		stateless.WorkflowState_WORKFLOW_STATE_INITIALIZED {
		return nil, true, nil /* update is filtered */
	}

	// Filter by job update state
	ok, err := isUpdateInfoInStatuses(
		resp.GetUpdateInfo(),
		jobUpdateQuery.GetUpdateStatuses())
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, true, nil /* update is filtered */
	}

	// Get Job Update Summary
	jobKey, err := ptoa.NewJobKey(jobSummary.GetName())
	if err != nil {
		return nil, false, err
	}
	jobUpdateSummary, err := ptoa.NewJobUpdateSummary(
		jobKey,
		resp.GetUpdateInfo())
	if err != nil {
		return nil, false, err
	}

	// Get job update details if summaryOnly is set false
	// TODO:
	// 1. instance workflow events
	// 2. job update events
	// 3. update spec
	// 4. job specs
	if !summaryOnly {
	}

	return &api.JobUpdateDetails{
		Update: &api.JobUpdate{
			Summary:      jobUpdateSummary,
			Instructions: nil,
		},
		UpdateEvents:   nil,
		InstanceEvents: nil,
	}, false, nil
}

// getJobSummariesFromJobUpdateQuery queries peloton jobs based on
// Aurora's JobUpdateQuery.
// 1. Query Peloton Jobs using only jobkey's Role. This can return a
// list of jobs.
// 2. If jobkey's Role is not set in Aurora's jobUpdateQuery, but entire
// jobkey is provided as input, then fetch corresponding job.
// JobSummary contains job information such as unique indentifier, name,
// instances, state and more. This information is populated for client
// for further filtering if required.
func (h *ServiceHandler) getJobSummariesFromJobUpdateQuery(
	ctx context.Context,
	jobUpdateQuery *api.JobUpdateQuery,
) ([]*stateless.JobSummary, error) {
	var jobKey *api.JobKey

	// TODO: remove partial key scenario for fetching jobIDs
	if jobUpdateQuery.IsSetRole() {
		jobKey = &api.JobKey{
			Role:        ptr.String(jobUpdateQuery.GetRole()),
			Environment: nil,
			Name:        nil,
		}
	}

	if jobKey == nil && jobUpdateQuery.IsSetJobKey() {
		jobKey = jobUpdateQuery.GetJobKey()
	}

	jobSummaries, err := h.getJobSummaries(ctx, jobKey)
	if err != nil {
		return nil, err
	}

	return jobSummaries, nil
}

// isUpdateInfoInStatuses checks if job update state is present
// in expected list of job update states
func isUpdateInfoInStatuses(
	updateInfo *stateless.UpdateInfo,
	updateStatus map[api.JobUpdateStatus]struct{},
) (bool, error) {

	pelotonUpdateState := updateInfo.GetInfo().GetStatus().GetState()
	auroraUpdateState, err := ptoa.NewJobUpdateStatus(pelotonUpdateState)
	if err != nil {
		return false, err
	}

	_, ok := updateStatus[auroraUpdateState]
	return ok, nil
}

// getJobIDs return Peloton JobID based on Aurora JobKey passed in.
//
// Currently, two querying mechanisms are implemented:
// 1. If all parameters in JobKey (role, environemnt, name) are present,
//    it uses GetJobIDFromJobName api directly. Expect one JobID to be
//    returned. If the job id cannot be found, then an error will return.
// 2. If only partial parameters are present, it uses QueryJobs api with
//    labels. Expect zero or many JobIDs to be returned.
//
// Option 1 will be dropped, if option 2 is proved to be stable and
// with minimal performance impact.
// TODO: To be deprecated in favor of getJobSummaries.
// Aggregator expects job key environment to be set in response of
// GetJobUpdateDetails to filter by deployment_id. On filtering via job key
// role, original peloton job name is not known to set job key environment.
func (h *ServiceHandler) getJobIDs(
	ctx context.Context,
	k *api.JobKey,
) ([]*peloton.JobID, error) {
	if k.IsSetRole() && k.IsSetEnvironment() && k.IsSetName() {
		req := &statelesssvc.GetJobIDFromJobNameRequest{
			JobName: atop.NewJobName(k),
		}
		resp, err := h.jobClient.GetJobIDFromJobName(ctx, req)
		if err != nil {
			return nil, err
		}
		// results are sorted chronologically, return the latest one
		return []*peloton.JobID{resp.GetJobId()[0]}, nil
	}

	labels := label.BuildPartialAuroraJobKeyLabels(k)
	if len(labels) == 0 {
		// TODO(kevinxu): do we need to return all job ids in this case?
		return nil, errors.New("empty job key")
	}

	req := &statelesssvc.QueryJobsRequest{
		Spec: &stateless.QuerySpec{
			Labels: labels,
		},
	}
	resp, err := h.jobClient.QueryJobs(ctx, req)
	if err != nil {
		return nil, err
	}
	jobIDs := make([]*peloton.JobID, 0, len(resp.GetRecords()))
	for _, record := range resp.GetRecords() {
		jobIDs = append(jobIDs, record.GetJobId())
	}
	return jobIDs, nil
}

// getJobSummaries returns Peloton JobSummary based on Aurora JobKey passed in.
//
// Currently, two querying mechanisms are implemented:
// 1. If all parameters in JobKey (role, environemnt, name) are present,
//    it uses GetJobIDFromJobName api directly. Expect one JobID to be
//    returned. If the job id cannot be found, then an error will return.
// 2. If only partial parameters are present, it uses QueryJobs api with
//    labels. Expect zero or many JobIDs to be returned.
// 3. If jobkey is not present, then QueryJobs will return all jobs.
func (h *ServiceHandler) getJobSummaries(
	ctx context.Context,
	k *api.JobKey,
) ([]*stateless.JobSummary, error) {
	if k.IsSetRole() && k.IsSetEnvironment() && k.IsSetName() {
		req := &statelesssvc.GetJobIDFromJobNameRequest{
			JobName: atop.NewJobName(k),
		}
		resp, err := h.jobClient.GetJobIDFromJobName(ctx, req)
		if err != nil {
			return nil, err
		}
		// results are sorted chronologically, return the latest one
		return []*stateless.JobSummary{
			{
				JobId: resp.GetJobId()[0],
				Name:  atop.NewJobName(k),
			},
		}, nil
	}

	req := &statelesssvc.QueryJobsRequest{
		Spec: &stateless.QuerySpec{
			Labels: label.BuildPartialAuroraJobKeyLabels(k),
		},
	}
	resp, err := h.jobClient.QueryJobs(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.GetRecords(), nil
}

// getJobID is a wrapper for getJobIDs when we expect only one peloton
// job id, e.g. when full job key is passed in. If the job id cannot be
// found, an error will return.
func (h *ServiceHandler) getJobID(
	ctx context.Context,
	k *api.JobKey,
) (*peloton.JobID, error) {
	ids, err := h.getJobIDs(ctx, k)
	if err != nil {
		return nil, err
	}
	return ids[0], nil
}

// getJobIDsFromTaskQuery queries peloton job ids based on aurora TaskQuery.
// Note that it will not throw error when no job is found. The current
// behavior for querying:
// 1. If TaskQuery.JobKeys is present, the job keys there to query job ids
// 2. Otherwise use TaskQuery.Role, TaskQuery.Environment and
//    TaskQuery.JobName to construct a job key (those 3 fields may not be
//    all present), and use it to query job ids.
func (h *ServiceHandler) getJobIDsFromTaskQuery(
	ctx context.Context,
	query *api.TaskQuery,
) ([]*peloton.JobID, error) {
	if query == nil {
		return nil, errors.New("task query is nil")
	}

	var jobIDs []*peloton.JobID

	// use job_keys to query if present
	if query.IsSetJobKeys() {
		for _, jobKey := range query.GetJobKeys() {
			ids, err := h.getJobIDs(ctx, jobKey)
			if err != nil {
				if yarpcerrors.IsNotFound(err) {
					continue
				}
				return nil, errors.Wrapf(err, "get job id for %q", jobKey)
			}
			jobIDs = append(jobIDs, ids[0])
		}
		return jobIDs, nil
	}

	// use job key parameters to query
	jobKey := &api.JobKey{
		Role:        query.Role,
		Environment: query.Environment,
		Name:        query.JobName,
	}
	jobIDs, err := h.getJobIDs(ctx, jobKey)
	if err != nil {
		if yarpcerrors.IsNotFound(err) {
			// ignore not found error and return empty job ids
			return nil, nil
		}
		return nil, errors.Wrapf(err, "get job ids for %q", jobKey)
	}
	return jobIDs, nil
}

func (h *ServiceHandler) getCurrentJobVersion(
	ctx context.Context,
	id *peloton.JobID,
) (*peloton.EntityVersion, error) {
	summary, err := h.getJobSummary(ctx, id)
	if err != nil {
		return nil, err
	}
	return summary.GetStatus().GetVersion(), nil
}

// getJobInfo calls jobmgr to get JobInfo based on JobID.
func (h *ServiceHandler) getJobInfo(
	ctx context.Context,
	jobID *peloton.JobID,
) (*stateless.JobInfo, error) {
	req := &statelesssvc.GetJobRequest{
		JobId:       jobID,
		SummaryOnly: false,
	}
	resp, err := h.jobClient.GetJob(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.GetJobInfo(), nil
}

// getJobSummary calls jobmgr to get JobSummary based on JobID.
func (h *ServiceHandler) getJobSummary(
	ctx context.Context,
	jobID *peloton.JobID,
) (*stateless.JobSummary, error) {
	req := &statelesssvc.GetJobRequest{
		JobId:       jobID,
		SummaryOnly: true,
	}
	resp, err := h.jobClient.GetJob(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.GetSummary(), nil
}

// queryPods calls jobmgr to query a list of PodInfo based on input JobID.
func (h *ServiceHandler) queryPods(
	ctx context.Context,
	jobID *peloton.JobID,
	states []pod.PodState,
) ([]*pod.PodInfo, error) {
	req := &statelesssvc.QueryPodsRequest{
		JobId: jobID,
		Spec: &pod.QuerySpec{
			PodStates: states,
		},
	}
	resp, err := h.jobClient.QueryPods(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.GetPods(), nil
}

// getPodEvents calls jobmgr to get a list of PodEvent based on PodName.
func (h *ServiceHandler) getPodEvents(
	ctx context.Context,
	podName *peloton.PodName,
) ([]*pod.PodEvent, error) {
	req := &podsvc.GetPodEventsRequest{
		PodName: podName,
	}
	resp, err := h.podClient.GetPodEvents(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.GetEvents(), nil
}
