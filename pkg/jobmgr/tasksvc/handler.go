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

package tasksvc

import (
	"context"
	"fmt"
	"math"
	"time"

	mesosv1 "github.com/uber/peloton/.gen/mesos/v1"
	pb_errors "github.com/uber/peloton/.gen/peloton/api/v0/errors"
	pb_job "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/query"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/common/leader"
	"github.com/uber/peloton/pkg/common/util"
	yarpcutil "github.com/uber/peloton/pkg/common/util/yarpc"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	"github.com/uber/peloton/pkg/jobmgr/goalstate"
	"github.com/uber/peloton/pkg/jobmgr/logmanager"
	jobmgr_task "github.com/uber/peloton/pkg/jobmgr/task"
	"github.com/uber/peloton/pkg/jobmgr/task/activermtask"
	"github.com/uber/peloton/pkg/jobmgr/task/lifecyclemgr"
	goalstateutil "github.com/uber/peloton/pkg/jobmgr/util/goalstate"
	handlerutil "github.com/uber/peloton/pkg/jobmgr/util/handler"
	taskutil "github.com/uber/peloton/pkg/jobmgr/util/task"
	"github.com/uber/peloton/pkg/storage"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	_rpcTimeout    = 15 * time.Second
	_frameworkName = "Peloton"
)

var (
	errEmptyFrameworkID = errors.New("framework id is empty")
)

// InitServiceHandler initializes the TaskManager
func InitServiceHandler(
	d *yarpc.Dispatcher,
	parent tally.Scope,
	ormStore *ormobjects.Store,
	taskStore storage.TaskStore,
	updateStore storage.UpdateStore,
	frameworkInfoStore storage.FrameworkInfoStore,
	jobFactory cached.JobFactory,
	goalStateDriver goalstate.Driver,
	candidate leader.Candidate,
	mesosAgentWorkDir string,
	hostMgrClientName string,
	logManager logmanager.LogManager,
	activeRMTasks activermtask.ActiveRMTasks,
	hmVersion api.Version,
) {

	handler := &serviceHandler{
		taskStore:          taskStore,
		jobConfigOps:       ormobjects.NewJobConfigOps(ormStore),
		jobRuntimeOps:      ormobjects.NewJobRuntimeOps(ormStore),
		updateStore:        updateStore,
		frameworkInfoStore: frameworkInfoStore,
		podEventsOps:       ormobjects.NewPodEventsOps(ormStore),
		metrics:            NewMetrics(parent.SubScope("jobmgr").SubScope("task")),
		resmgrClient:       resmgrsvc.NewResourceManagerServiceYARPCClient(d.ClientConfig(common.PelotonResourceManager)),
		lm:                 lifecyclemgr.New(hmVersion, d, parent),
		jobFactory:         jobFactory,
		goalStateDriver:    goalStateDriver,
		candidate:          candidate,
		mesosAgentWorkDir:  mesosAgentWorkDir,
		hostMgrClient:      hostsvc.NewInternalHostServiceYARPCClient(d.ClientConfig(hostMgrClientName)),
		logManager:         logManager,
		activeRMTasks:      activeRMTasks,
	}
	d.Register(task.BuildTaskManagerYARPCProcedures(handler))
}

// serviceHandler implements peloton.api.task.TaskManager
type serviceHandler struct {
	taskStore          storage.TaskStore
	jobConfigOps       ormobjects.JobConfigOps
	jobRuntimeOps      ormobjects.JobRuntimeOps
	updateStore        storage.UpdateStore
	frameworkInfoStore storage.FrameworkInfoStore
	podEventsOps       ormobjects.PodEventsOps
	metrics            *Metrics
	resmgrClient       resmgrsvc.ResourceManagerServiceYARPCClient
	lm                 lifecyclemgr.Manager
	jobFactory         cached.JobFactory
	goalStateDriver    goalstate.Driver
	candidate          leader.Candidate
	mesosAgentWorkDir  string
	hostMgrClient      hostsvc.InternalHostServiceYARPCClient
	logManager         logmanager.LogManager
	activeRMTasks      activermtask.ActiveRMTasks
}

func (m *serviceHandler) Get(
	ctx context.Context,
	body *task.GetRequest) (resp *task.GetResponse, err error) {

	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		if err != nil || resp.GetNotFound() != nil || resp.GetOutOfRange() == nil {
			entry := log.WithField("request", body).
				WithField("headers", headers)

			if err != nil {
				entry = entry.WithError(err)
			}
			if resp.GetNotFound() != nil {
				entry = entry.WithField("not_found_err", resp.GetNotFound().GetMessage())
			}
			if resp.GetOutOfRange() != nil {
				entry = entry.WithField("out_of_range_err", resp.GetOutOfRange().GetInstanceCount())
			}

			entry.Warn("TaskManager.Get failed")
			return
		}

		log.WithField("req", body).
			WithField("headers", headers).
			Debug("TaskManager.Get succeeded")
	}()

	m.metrics.TaskAPIGet.Inc(1)
	jobConfig, err := handlerutil.GetJobConfigWithoutFillingCache(
		ctx, body.JobId, m.jobFactory, m.jobConfigOps)
	if err != nil {
		log.WithField("job_id", body.JobId.Value).
			WithError(err).
			Debug("Failed to get job config")
		m.metrics.TaskGetFail.Inc(1)
		return &task.GetResponse{
			NotFound: &pb_errors.JobNotFound{
				Id:      body.JobId,
				Message: fmt.Sprintf("job %v not found, %v", body.JobId, err),
			},
		}, nil
	}

	var lastTaskInfo *task.TaskInfo
	result, err := m.taskStore.GetTaskForJob(
		ctx, body.JobId.GetValue(), body.InstanceId)
	if err != nil {
		return nil, err
	}

	for _, taskInfo := range result {
		lastTaskInfo = taskInfo
		break
	}

	eventList, err := m.getPodEvents(
		ctx,
		body.GetJobId(),
		body.GetInstanceId(),
		"") // get pod events for last run
	if err != nil {
		m.metrics.TaskGetFail.Inc(1)
		return &task.GetResponse{
			OutOfRange: &task.InstanceIdOutOfRange{
				JobId:         body.JobId,
				InstanceCount: jobConfig.GetInstanceCount(),
			},
		}, nil
	}

	taskInfos := m.getTerminalEvents(eventList, lastTaskInfo)

	m.metrics.TaskGet.Inc(1)
	return &task.GetResponse{
		Result:  lastTaskInfo,
		Results: taskInfos,
	}, nil
}

// GetPodEvents returns a chronological order of state transition events
// for a pod (a job's instance).
func (m *serviceHandler) GetPodEvents(
	ctx context.Context,
	body *task.GetPodEventsRequest) (resp *task.GetPodEventsResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		if err != nil || resp.GetError() != nil {
			entry := log.WithField("request", body).
				WithField("headers", headers)

			if err != nil {
				entry = entry.WithError(err)
			}
			if resp.GetError() != nil {
				entry = entry.WithField("resp_error", resp.GetError().GetMessage())
			}
			entry.Warn("TaskManager.GetPodEvents failed")
			return
		}

		log.WithField("request", body).
			WithField("headers", headers).
			Debug("TaskManager.GetPodEvents succeeded")
	}()

	// Limit defines the number of run id's to return, if the req is asking for
	// a specific run id then limit is 1.
	limit := body.GetLimit()
	if len(body.GetRunId()) != 0 {
		limit = 1
	}

	// Default to 10 run IDs
	if limit == 0 {
		limit = 10
	}

	mesosTaskID := body.GetRunId()
	var result []*task.PodEvent
	for i := uint64(0); i < limit; i++ {
		taskEvents, err := m.podEventsOps.GetAll(
			ctx,
			body.GetJobId().GetValue(),
			body.GetInstanceId(),
			mesosTaskID)
		if err != nil {
			return nil, errors.Wrap(err, "error getting task events from store")
		}

		if len(taskEvents) == 0 {
			break
		}

		result = append(result, taskEvents...)
		prevMesosTaskID := taskEvents[0].GetPrevTaskId().GetValue()

		prevRunID, err := util.ParseRunID(prevMesosTaskID)
		if err != nil {
			return nil, errors.Wrap(err, "error parsing prevMesosTaskID")
		}

		if prevRunID == uint64(0) {
			break
		}

		mesosTaskID = prevMesosTaskID
	}

	return &task.GetPodEventsResponse{
		Result: result,
	}, nil
}

// DeletePodEvents, deletes the pod events for provided request, which is for
// a jobID + instanceID + less than equal to runID.
// Response will be successful or error on unable to delete events for input.
func (m *serviceHandler) DeletePodEvents(
	ctx context.Context,
	body *task.DeletePodEventsRequest,
) (resp *task.DeletePodEventsResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		if err != nil {
			log.WithField("request", body).
				WithField("headers", headers).
				WithError(err).
				Warn("TaskManager.DeletePodEvents failed")
			return
		}

		log.WithField("request", body).
			WithField("headers", headers).
			Info("TaskManager.DeletePodEvents succeeded")
	}()

	if err := m.taskStore.DeletePodEvents(
		ctx,
		body.GetJobId().GetValue(),
		body.GetInstanceId(),
		1,
		body.GetRunId()+1,
	); err != nil {
		return nil, err
	}
	return &task.DeletePodEventsResponse{}, nil
}

// List/Query API should not use cachedJob
// because we would not clean up the cache for untracked job
func (m *serviceHandler) List(
	ctx context.Context,
	body *task.ListRequest) (resp *task.ListResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		if err != nil || resp.GetNotFound() != nil {
			entry := log.WithField("request", body).
				WithField("headers", headers)

			if err != nil {
				entry = entry.WithError(err)
			}

			if resp.GetNotFound() != nil {
				entry = entry.WithField("not_found_err", resp.GetNotFound().GetMessage())
			}
			entry.Warn("TaskManager.List failed")
			return
		}

		log.WithField("request", body).
			WithField("headers", headers).
			Debug("TaskManager.List succeeded")
	}()

	m.metrics.TaskAPIList.Inc(1)

	var instanceRanges []*task.InstanceRange
	if body.GetRange() != nil {
		instanceRanges = append(instanceRanges, body.GetRange())
	}
	result, err := m.getTaskInfosByRangesFromDB(
		ctx, body.GetJobId(), instanceRanges)
	if err != nil || len(result) == 0 {
		m.metrics.TaskListFail.Inc(1)
		return &task.ListResponse{
			NotFound: &pb_errors.JobNotFound{
				Id:      body.JobId,
				Message: fmt.Sprintf("err= %v", err),
			},
		}, nil
	}

	m.fillReasonForPendingTasksFromResMgr(ctx, body.GetJobId(), convertTaskMapToSlice(result))
	m.metrics.TaskList.Inc(1)
	resp = &task.ListResponse{
		Result: &task.ListResponse_Result{
			Value: result,
		},
	}
	log.WithField("response", resp).Debug("TaskSVC.List returned")
	return resp, nil
}

// Refresh loads the task runtime state from DB, updates the cache,
// and enqueues it to goal state for evaluation.
func (m *serviceHandler) Refresh(ctx context.Context, req *task.RefreshRequest) (resp *task.RefreshResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		if err != nil {
			log.WithField("request", req).
				WithField("headers", headers).
				WithError(err).
				Warn("TaskManager.Refresh failed")
			return
		}

		log.WithField("request", req).
			WithField("headers", headers).
			Info("TaskManager.Refresh succeeded")
	}()

	m.metrics.TaskAPIRefresh.Inc(1)

	if !m.candidate.IsLeader() {
		m.metrics.TaskRefreshFail.Inc(1)
		return nil, yarpcerrors.UnavailableErrorf("Task Refresh API not suppported on non-leader")
	}

	jobConfig, _, err := m.jobConfigOps.GetCurrentVersion(ctx, req.GetJobId())
	if err != nil {
		log.WithError(err).
			WithField("job_id", req.GetJobId().GetValue()).
			Error("failed to load job config in executing task")
		m.metrics.TaskRefreshFail.Inc(1)
		return &task.RefreshResponse{}, yarpcerrors.NotFoundErrorf("job not found")
	}

	reqRange := req.GetRange()
	if reqRange == nil {
		reqRange = &task.InstanceRange{
			From: 0,
			To:   jobConfig.InstanceCount,
		}
	}

	if reqRange.To > jobConfig.InstanceCount {
		reqRange.To = jobConfig.InstanceCount
	}

	taskInfos, err := m.taskStore.GetTasksForJobByRange(ctx, req.GetJobId(), reqRange)
	if err != nil {
		log.WithError(err).
			WithField("job_id", req.GetJobId().GetValue()).
			WithField("range", reqRange).
			Error("failed to load task infos in executing task")
		m.metrics.TaskRefreshFail.Inc(1)
		return &task.RefreshResponse{}, yarpcerrors.NotFoundErrorf("tasks not found")
	}
	if len(taskInfos) == 0 {
		log.WithError(err).
			WithField("job_id", req.GetJobId().GetValue()).
			WithField("range", reqRange).
			Error("no task info found while executing task")
		m.metrics.TaskRefreshFail.Inc(1)
		return &task.RefreshResponse{}, yarpcerrors.NotFoundErrorf("tasks not found")
	}

	cachedJob := m.jobFactory.AddJob(req.GetJobId())
	cachedJob.ReplaceTasks(taskInfos, true)
	for instID := range taskInfos {
		m.goalStateDriver.EnqueueTask(req.GetJobId(), instID, time.Now())
	}

	goalstate.EnqueueJobWithDefaultDelay(
		req.GetJobId(), m.goalStateDriver, cachedJob)

	m.metrics.TaskRefresh.Inc(1)
	return &task.RefreshResponse{}, nil
}

// getTaskInfosByRangesFromDB get all the tasks infos for given job and ranges.
func (m *serviceHandler) getTaskInfosByRangesFromDB(
	ctx context.Context,
	jobID *peloton.JobID,
	ranges []*task.InstanceRange) (map[uint32]*task.TaskInfo, error) {

	taskInfos := make(map[uint32]*task.TaskInfo)
	var err error
	if len(ranges) == 0 {
		// If no ranges specified, then start all instances in the given job.
		taskInfos, err = m.taskStore.GetTasksForJob(ctx, jobID)
	} else {
		var tmpTaskInfos map[uint32]*task.TaskInfo
		for _, taskRange := range ranges {
			// Need to do this check as instance_id is of type int32. When the
			// range goes beyond MaxInt32, C* would not return any result.
			// Theoretically, taskStore should return an error, but now
			// it is not the case. Remove this check, once figure out
			// what is going wrong in taskStore.
			if taskRange.GetTo() > math.MaxInt32 {
				taskRange.To = math.MaxInt32
			}
			tmpTaskInfos, err = m.taskStore.GetTasksForJobByRange(
				ctx, jobID, taskRange)
			if err != nil {
				return taskInfos, err
			}
			for inst := range tmpTaskInfos {
				taskInfos[inst] = tmpTaskInfos[inst]
			}
		}
	}

	return taskInfos, err
}

// Start implements TaskManager.Start, tries to start terminal tasks in a given job.
func (m *serviceHandler) Start(
	ctx context.Context,
	body *task.StartRequest) (resp *task.StartResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		if err != nil || resp.GetError() != nil {
			entry := log.WithField("request", body).
				WithField("headers", headers)

			if err != nil {
				entry = entry.WithError(err)
			}
			if resp.GetError() != nil {
				entry = entry.WithField("start_error", resp.GetError().String())
			}

			entry.Warn("TaskManager.Start failed")
			return
		}

		log.WithField("request", body).
			WithField("headers", headers).
			Info("TaskManager.Start succeeded")
	}()

	m.metrics.TaskAPIStart.Inc(1)
	ctx, cancelFunc := context.WithTimeout(
		ctx,
		_rpcTimeout,
	)
	defer cancelFunc()

	if !m.candidate.IsLeader() {
		m.metrics.TaskStartFail.Inc(1)
		return nil, yarpcerrors.UnavailableErrorf("Task Start API not suppported on non-leader")
	}

	cachedJob := m.jobFactory.AddJob(body.JobId)
	cachedConfig, err := cachedJob.GetConfig(ctx)

	if err != nil {
		log.WithField("job_id", body.JobId.Value).
			WithError(err).
			Error("Failed to get job config")
		m.metrics.TaskStartFail.Inc(1)
		return &task.StartResponse{
			Error: &task.StartResponse_Error{
				NotFound: &pb_errors.JobNotFound{
					Id:      body.JobId,
					Message: err.Error(),
				},
			},
		}, nil
	}

	count := 0
	for {
		jobRuntime, err := cachedJob.GetRuntime(ctx)
		if err != nil {
			log.WithField("job_id", body.JobId.Value).
				WithError(err).
				Info("failed to fetch job runtime while starting tasks")
			m.metrics.TaskStartFail.Inc(1)
			return nil, err
		}

		// batch jobs in terminated state cannot be restarted
		if cachedConfig.GetType() == pb_job.JobType_BATCH {
			if util.IsPelotonJobStateTerminal(jobRuntime.GetState()) {
				log.WithFields(log.Fields{
					"job_id": body.JobId.Value,
					"state":  jobRuntime.GetState().String(),
				}).Info("cannot start tasks in a terminal job")
				m.metrics.TaskStartFail.Inc(1)
				return nil, yarpcerrors.InvalidArgumentErrorf(
					"cannot start tasks in a terminated job")
			}
		}

		jobRuntime.State = pb_job.JobState_PENDING
		jobRuntime.GoalState = goalstateutil.GetDefaultJobGoalState(
			cachedConfig.GetType())

		// update the job runtime
		_, err = cachedJob.CompareAndSetRuntime(ctx, jobRuntime)
		if err == jobmgrcommon.UnexpectedVersionError {
			// concurrency error; retry MaxConcurrencyErrorRetry times
			count = count + 1
			if count < jobmgrcommon.MaxConcurrencyErrorRetry {
				continue
			}
		}

		if err != nil {
			log.WithField("job", body.JobId).
				WithError(err).
				Error("failed to set job runtime in db")
			m.metrics.TaskStartFail.Inc(1)
			return &task.StartResponse{
				Error: &task.StartResponse_Error{
					Failure: &task.TaskStartFailure{
						Message: fmt.Sprintf("task start failed while updating job status %v", err),
					},
				},
			}, nil
		}

		// job runtime is successfully updated, move on
		break
	}

	taskInfos, err := m.getTaskInfosByRangesFromDB(
		ctx, body.GetJobId(), body.GetRanges())
	if err != nil {
		log.WithField("job", body.JobId).
			WithError(err).
			Error("failed to get tasks for job in db")
		m.metrics.TaskStartFail.Inc(1)
		return &task.StartResponse{
			Error: &task.StartResponse_Error{
				OutOfRange: &task.InstanceIdOutOfRange{
					JobId:         body.JobId,
					InstanceCount: cachedConfig.GetInstanceCount(),
				},
			},
		}, nil
	}

	var startedInstanceIds []uint32
	var failedInstanceIds []uint32

	for _, taskInfo := range taskInfos {
		cachedTask, err := cachedJob.AddTask(ctx, taskInfo.GetInstanceId())
		if err != nil {
			log.WithFields(log.Fields{
				"job_id":      body.GetJobId().GetValue(),
				"instance_id": taskInfo.InstanceId,
			}).Info("failed to add task during task start")
			failedInstanceIds = append(failedInstanceIds, taskInfo.InstanceId)
			continue
		}

		count := 0
		for {
			taskRuntime, err := cachedTask.GetRuntime(ctx)
			if err != nil {
				log.WithFields(log.Fields{
					"job_id":      body.GetJobId().GetValue(),
					"instance_id": taskInfo.InstanceId,
				}).Info("failed to fetch runtime during task start")
				failedInstanceIds = append(failedInstanceIds, taskInfo.InstanceId)
				break
			}

			if taskRuntime.GetGoalState() != task.TaskState_KILLED {
				// ignore start request for tasks with non-killed goal state
				log.WithFields(log.Fields{
					"instance_id": taskInfo.InstanceId,
					"job_id":      body.GetJobId().GetValue(),
					"goal_state":  taskRuntime.GetGoalState().String(),
				}).Debug("task was not stopped")
				break
			}

			// Regenerate the task and change the goalstate
			healthState := taskutil.GetInitialHealthState(taskInfo.GetConfig())
			taskutil.RegenerateMesosTaskRuntime(
				body.GetJobId(),
				taskInfo.InstanceId,
				taskRuntime,
				healthState,
			)
			taskRuntime.GoalState =
				jobmgr_task.GetDefaultTaskGoalState(cachedConfig.GetType())
			taskRuntime.Message = "Task start API request"

			// Directly call task level APIs instead of calling job level API
			// as one transaction (like PatchTasks calls) because
			// compare and set calls cannot be batched as one transaction
			// as if task runtime of only one task has changed, then it should
			// not cause the entire transaction to fail and to be retried again.
			_, err = cachedJob.CompareAndSetTask(
				ctx,
				taskInfo.GetInstanceId(),
				taskRuntime,
				false,
			)

			if err == jobmgrcommon.UnexpectedVersionError {
				count = count + 1
				if count < jobmgrcommon.MaxConcurrencyErrorRetry {
					continue
				}
			}

			if err != nil {
				log.WithError(err).
					WithFields(log.Fields{
						"job_id":      body.GetJobId().GetValue(),
						"instance_id": taskInfo.InstanceId,
					}).Info("failed to write runtime during task start")
				failedInstanceIds = append(failedInstanceIds, taskInfo.InstanceId)
			} else {
				startedInstanceIds = append(startedInstanceIds, taskInfo.InstanceId)
			}
			break
		}
	}

	for _, instID := range startedInstanceIds {
		m.goalStateDriver.EnqueueTask(body.GetJobId(), instID, time.Now())
	}
	goalstate.EnqueueJobWithDefaultDelay(
		body.GetJobId(), m.goalStateDriver, cachedJob)

	m.metrics.TaskStart.Inc(1)
	return &task.StartResponse{
		StartedInstanceIds: startedInstanceIds,
		InvalidInstanceIds: failedInstanceIds,
	}, nil
}

func (m *serviceHandler) stopJob(
	ctx context.Context,
	jobID *peloton.JobID,
	instanceCount uint32) (*task.StopResponse, error) {
	var instanceList []uint32
	var count uint32

	for i := uint32(0); i < instanceCount; i++ {
		instanceList = append(instanceList, i)
	}

	cachedJob := m.jobFactory.AddJob(jobID)
	for {
		jobRuntime, err := cachedJob.GetRuntime(ctx)
		if err != nil {
			log.WithError(err).
				WithField("job_id", jobID.GetValue()).
				Error("failed to get job run time")
			m.metrics.TaskStopFail.Inc(int64(instanceCount))
			return &task.StopResponse{
				Error: &task.StopResponse_Error{
					UpdateError: &task.TaskUpdateError{
						Message: fmt.Sprintf("Job state fetch failed for %v", err),
					},
				},
				InvalidInstanceIds: instanceList,
			}, nil
		}

		if jobRuntime.GoalState == pb_job.JobState_KILLED {
			return &task.StopResponse{
				StoppedInstanceIds: instanceList,
			}, nil
		}

		jobRuntime.DesiredStateVersion++
		jobRuntime.GoalState = pb_job.JobState_KILLED

		_, err = cachedJob.CompareAndSetRuntime(ctx, jobRuntime)
		if err != nil {
			if err == jobmgrcommon.UnexpectedVersionError {
				// concurrency error; retry MaxConcurrencyErrorRetry times
				count = count + 1
				if count < jobmgrcommon.MaxConcurrencyErrorRetry {
					continue
				}
			}

			log.WithError(err).
				WithField("job_id", jobID.GetValue()).
				Error("failed to update job run time")
			m.metrics.TaskStopFail.Inc(int64(instanceCount))
			return &task.StopResponse{
				Error: &task.StopResponse_Error{
					UpdateError: &task.TaskUpdateError{
						Message: fmt.Sprintf("Job state update failed for %v", err),
					},
				},
				InvalidInstanceIds: instanceList,
			}, nil
		}

		break
	}

	m.goalStateDriver.EnqueueJob(jobID, time.Now())

	m.metrics.TaskStop.Inc(int64(instanceCount))
	return &task.StopResponse{
		StoppedInstanceIds: instanceList,
	}, nil
}

// Stop implements TaskManager.Stop, tries to stop tasks in a given job.
func (m *serviceHandler) Stop(
	ctx context.Context,
	body *task.StopRequest) (resp *task.StopResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		if err != nil || resp.GetError() != nil {
			entry := log.WithField("request", body).
				WithField("headers", headers)

			if err != nil {
				entry = entry.WithError(err)
			}
			if resp.GetError() != nil {
				entry = entry.WithField("stop_error", resp.GetError().String())
			}

			entry.Warn("TaskManager.Stop failed")
			return
		}

		log.WithField("request", body).
			WithField("headers", headers).
			Info("TaskManager.Stop succeeded")
	}()

	m.metrics.TaskAPIStop.Inc(1)
	ctx, cancelFunc := context.WithTimeout(
		ctx,
		_rpcTimeout,
	)
	defer cancelFunc()

	if !m.candidate.IsLeader() {
		m.metrics.TaskStopFail.Inc(1)
		return nil, yarpcerrors.UnavailableErrorf("Task Stop API not suppported on non-leader")
	}

	cachedJob := m.jobFactory.AddJob(body.JobId)
	cachedConfig, err := cachedJob.GetConfig(ctx)

	if err != nil {
		log.WithField("job_id", body.JobId).
			WithError(err).
			Error("Failed to get job config")
		m.metrics.TaskStopFail.Inc(1)
		return &task.StopResponse{
			Error: &task.StopResponse_Error{
				NotFound: &pb_errors.JobNotFound{
					Id:      body.JobId,
					Message: err.Error(),
				},
			},
		}, nil
	}

	taskRange := body.GetRanges()
	if len(taskRange) == 0 || (len(taskRange) == 1 && taskRange[0].From == 0 && taskRange[0].To >= cachedConfig.GetInstanceCount()) {
		// Stop all tasks in a job, stop entire job instead of task by task
		log.WithField("job_id", body.GetJobId().GetValue()).
			Info("stopping all tasks in the job")
		return m.stopJob(ctx, body.GetJobId(), cachedConfig.GetInstanceCount())
	}

	taskInfos, err := m.getTaskInfosByRangesFromDB(
		ctx, body.GetJobId(), taskRange)
	if err != nil {
		log.WithField("job", body.JobId).
			WithError(err).
			Error("failed to get tasks for job in db")
		m.metrics.TaskStopFail.Inc(1)
		return &task.StopResponse{
			Error: &task.StopResponse_Error{
				OutOfRange: &task.InstanceIdOutOfRange{
					JobId:         body.JobId,
					InstanceCount: cachedConfig.GetInstanceCount(),
				},
			},
		}, nil
	}

	// tasksToKill only includes task ids whose goal state update succeeds.
	var stoppedInstanceIds []uint32
	var failedInstanceIds []uint32
	var instanceIds []uint32
	runtimeDiffs := make(map[uint32]jobmgrcommon.RuntimeDiff)
	// Persist KILLED goalstate for tasks in db.
	for _, taskInfo := range taskInfos {
		// Skip update task goalstate if it is already KILLED.
		if taskInfo.GetRuntime().GoalState == task.TaskState_KILLED {
			continue
		}

		runtimeDiff := jobmgrcommon.RuntimeDiff{
			jobmgrcommon.GoalStateField: task.TaskState_KILLED,
			jobmgrcommon.MessageField:   "Task stop API request",
			jobmgrcommon.ReasonField:    "",
			jobmgrcommon.TerminationStatusField: &task.TerminationStatus{
				Reason: task.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_ON_REQUEST,
			},
		}
		runtimeDiffs[taskInfo.InstanceId] = runtimeDiff
		instanceIds = append(instanceIds, taskInfo.InstanceId)
	}

	_, _, err = cachedJob.PatchTasks(ctx, runtimeDiffs, false)
	if err != nil {
		log.WithError(err).
			WithField("instance_ids", instanceIds).
			WithField("job_id", body.GetJobId().GetValue()).
			Error("failed to updated killed goalstate")
		failedInstanceIds = instanceIds
		m.metrics.TaskStopFail.Inc(1)
	} else {
		stoppedInstanceIds = instanceIds
		m.metrics.TaskStop.Inc(1)
	}

	for _, instID := range stoppedInstanceIds {
		m.goalStateDriver.EnqueueTask(body.GetJobId(), instID, time.Now())
	}

	goalstate.EnqueueJobWithDefaultDelay(
		body.GetJobId(), m.goalStateDriver, cachedJob)

	if err != nil {
		return &task.StopResponse{
			Error: &task.StopResponse_Error{
				UpdateError: &task.TaskUpdateError{
					Message: fmt.Sprintf("Goalstate update failed for %v", err),
				},
			},
			StoppedInstanceIds: stoppedInstanceIds,
		}, nil
	}
	return &task.StopResponse{
		StoppedInstanceIds: stoppedInstanceIds,
		InvalidInstanceIds: failedInstanceIds,
	}, nil
}

func (m *serviceHandler) Restart(
	ctx context.Context,
	req *task.RestartRequest) (resp *task.RestartResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		if err != nil || resp.GetNotFound() != nil || resp.GetOutOfRange() != nil {
			entry := log.WithField("request", req).
				WithField("headers", headers)

			if err != nil {
				entry = entry.WithError(err)
			}
			if resp.GetNotFound() != nil {
				entry = entry.WithField("not_found_error", resp.GetNotFound().GetMessage())
			}
			if resp.GetOutOfRange() != nil {
				entry = entry.WithField("out_of_range_error", resp.GetOutOfRange().String())
			}

			entry.Warn("TaskManager.Restart failed")
			return
		}

		log.WithField("request", req).
			WithField("headers", headers).
			Info("TaskManager.Restart succeeded")
	}()

	m.metrics.TaskAPIRestart.Inc(1)

	if !m.candidate.IsLeader() {
		m.metrics.TaskRestartFail.Inc(1)
		return nil,
			yarpcerrors.UnavailableErrorf(
				"Task Restart API not supported on non-leader")
	}

	ctx, cancelFunc := context.WithTimeout(
		ctx,
		_rpcTimeout,
	)
	defer cancelFunc()

	cachedJob := m.jobFactory.AddJob(req.JobId)
	runtimeDiffs, err := m.getRuntimeDiffsForRestart(ctx,
		cachedJob,
		req.GetRanges())
	if err != nil {
		m.metrics.TaskRestartFail.Inc(1)
		return nil, err
	}
	if _, _, err := cachedJob.PatchTasks(ctx, runtimeDiffs, false); err != nil {
		m.metrics.TaskRestartFail.Inc(1)
		return nil, err
	}

	for instanceID := range runtimeDiffs {
		m.goalStateDriver.EnqueueTask(req.JobId, instanceID, time.Now())
	}
	m.metrics.TaskRestart.Inc(1)
	return &task.RestartResponse{}, nil
}

// getRuntimeDiffsForRestart returns runtimeDiffs to be applied to task to be
// restarted. It updates the DesiredMesosTaskID field of task runtime.
func (m *serviceHandler) getRuntimeDiffsForRestart(
	ctx context.Context,
	cachedJob cached.Job,
	instanceRanges []*task.InstanceRange) (map[uint32]jobmgrcommon.RuntimeDiff, error) {
	result := make(map[uint32]jobmgrcommon.RuntimeDiff)
	taskInfos, err := m.getTaskInfosByRangesFromDB(
		ctx, cachedJob.ID(), instanceRanges)
	if err != nil {
		return nil, err
	}

	for _, taskInfo := range taskInfos {
		runID, err :=
			util.ParseRunID(taskInfo.GetRuntime().GetMesosTaskId().GetValue())
		if err != nil {
			runID = 0
		}

		result[taskInfo.InstanceId] = jobmgrcommon.RuntimeDiff{
			jobmgrcommon.DesiredMesosTaskIDField: util.CreateMesosTaskID(
				cachedJob.ID(), taskInfo.InstanceId, runID+1),
		}
	}

	return result, nil
}

// List/Query API should not use cachedJob
// because we would not clean up the cache for untracked job
func (m *serviceHandler) Query(ctx context.Context, req *task.QueryRequest) (resp *task.QueryResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		if err != nil || resp.GetError() != nil {
			entry := log.WithField("request", req).
				WithField("headers", headers)

			if err != nil {
				entry = entry.WithError(err)
			}
			if resp.GetError() != nil {
				entry = entry.WithField("restart_error", resp.GetError().String())
			}

			entry.Warn("TaskManager.Query failed")
			return
		}

		log.WithField("request", req).
			WithField("headers", headers).
			Debug("TaskManager.Query succeeded")
	}()

	m.metrics.TaskAPIQuery.Inc(1)
	callStart := time.Now()

	_, err = handlerutil.GetJobRuntimeWithoutFillingCache(
		ctx, req.JobId, m.jobFactory, m.jobRuntimeOps)
	if err != nil {
		log.WithError(err).
			WithField("job_id", req.JobId.GetValue()).
			Debug("failed to find job")
		m.metrics.TaskQueryFail.Inc(1)
		return &task.QueryResponse{
			Error: &task.QueryResponse_Error{
				NotFound: &pb_errors.JobNotFound{
					Id:      req.JobId,
					Message: fmt.Sprintf("Failed to find job with id %v, err=%v", req.JobId, err),
				},
			},
		}, nil
	}

	result, total, err := m.taskStore.QueryTasks(ctx, req.GetJobId(), req.GetSpec())
	if err != nil {
		m.metrics.TaskQueryFail.Inc(1)
		return &task.QueryResponse{
			Error: &task.QueryResponse_Error{
				NotFound: &pb_errors.JobNotFound{
					Id:      req.JobId,
					Message: fmt.Sprintf("err= %v", err),
				},
			},
		}, nil
	}

	m.fillReasonForPendingTasksFromResMgr(ctx, req.GetJobId(), result)
	m.metrics.TaskQuery.Inc(1)
	resp = &task.QueryResponse{
		Records: result,
		Pagination: &query.Pagination{
			Offset: req.GetSpec().GetPagination().GetOffset(),
			Limit:  req.GetSpec().GetPagination().GetLimit(),
			Total:  total,
		},
	}
	callDuration := time.Since(callStart)
	m.metrics.TaskQueryHandlerDuration.Record(callDuration)
	log.WithField("response", resp).Debug("TaskSVC.Query returned")
	return resp, nil
}

func (m *serviceHandler) GetCache(
	ctx context.Context,
	req *task.GetCacheRequest) (resp *task.GetCacheResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		if err != nil {
			log.WithField("request", req).
				WithField("headers", headers).
				WithError(err).
				Warn("TaskManager.GetCache failed")
			return
		}

		log.WithField("request", req).
			WithField("headers", headers).
			Debug("TaskManager.GetCache succeeded")
	}()

	cachedJob := m.jobFactory.GetJob(req.JobId)
	if cachedJob == nil {
		return nil,
			yarpcerrors.NotFoundErrorf("Job not found in cache")
	}

	cachedTask := cachedJob.GetTask(req.InstanceId)
	if cachedTask == nil {
		return nil,
			yarpcerrors.NotFoundErrorf("Task not found in cache")
	}

	runtime, err := cachedTask.GetRuntime(ctx)
	if err != nil {
		return nil,
			yarpcerrors.InternalErrorf("Cannot get task cache with err: %v", err)
	}

	labels, err := cachedTask.GetLabels(ctx)
	if err != nil {
		return nil,
			yarpcerrors.InternalErrorf("Cannot get task labels in cache with err: %v", err)
	}

	return &task.GetCacheResponse{
		Runtime: runtime,
		Labels:  labels,
	}, nil
}

func (m *serviceHandler) getHostInfoWithTaskID(
	ctx context.Context,
	jobID *peloton.JobID,
	instanceID uint32,
	taskID string) (hostname string, agentID string, err error) {
	events, err := m.getPodEvents(ctx, jobID, instanceID, taskID)
	if err != nil {
		return "", "", err
	}

	if len(events) == 0 {
		return "", "",
			yarpcerrors.NotFoundErrorf("no pod events present for job_id: %s, instance_id: %d, run_id: %s",
				jobID.GetValue(), instanceID, taskID)
	}
	terminalEvent := events[0]

	for _, event := range events {
		taskState := task.TaskState(task.TaskState_value[event.GetActualState()])
		if util.IsPelotonStateTerminal(taskState) {
			terminalEvent = event
			break
		}
	}
	hostname = terminalEvent.GetHostname()
	agentID = terminalEvent.GetAgentID()
	return hostname, agentID, nil
}

func (m *serviceHandler) getHostInfoCurrentTask(
	ctx context.Context,
	jobID *peloton.JobID,
	instanceID uint32) (hostname string, agentID string, taskID string, err error) {
	result, err := m.taskStore.GetTaskForJob(ctx, jobID.GetValue(), instanceID)

	if err != nil {
		return "", "", "", err
	}

	if len(result) != 1 {
		return "", "", "", yarpcerrors.NotFoundErrorf("task not found")
	}

	var taskInfo *task.TaskInfo
	for _, t := range result {
		taskInfo = t
	}

	hostname, agentID = taskInfo.GetRuntime().GetHost(), taskInfo.GetRuntime().GetAgentID().GetValue()
	taskID = taskInfo.GetRuntime().GetMesosTaskId().GetValue()
	return hostname, agentID, taskID, nil
}

// getSandboxPathInfo - return details such as hostname, agentID, frameworkID and taskID to create sandbox path.
func (m *serviceHandler) getSandboxPathInfo(
	ctx context.Context,
	instanceCount uint32,
	req *task.BrowseSandboxRequest) (hostname, agentID, taskID, frameworkID string, resp *task.BrowseSandboxResponse) {
	var host string
	var agentid string
	taskid := req.GetTaskId()

	var err error
	if len(taskid) > 0 {
		host, agentid, err = m.getHostInfoWithTaskID(ctx,
			req.JobId,
			req.InstanceId,
			taskid,
		)
	} else {
		host, agentid, taskid, err = m.getHostInfoCurrentTask(
			ctx,
			req.JobId,
			req.InstanceId)
	}

	if err != nil {
		m.metrics.TaskListLogsFail.Inc(1)
		return "", "", "", "", &task.BrowseSandboxResponse{
			Error: &task.BrowseSandboxResponse_Error{
				OutOfRange: &task.InstanceIdOutOfRange{
					JobId:         req.JobId,
					InstanceCount: instanceCount,
				},
			},
		}
	}

	if len(host) == 0 || len(agentid) == 0 {
		m.metrics.TaskListLogsFail.Inc(1)
		return "", "", "", "", &task.BrowseSandboxResponse{
			Error: &task.BrowseSandboxResponse_Error{
				NotRunning: &task.TaskNotRunning{
					Message: "taskinfo does not have hostname or agentID",
				},
			},
		}
	}

	// get framework ID.
	frameworkid, err := m.getFrameworkID(ctx)
	if err != nil {
		m.metrics.TaskListLogsFail.Inc(1)
		log.WithError(err).WithFields(log.Fields{
			"req": req,
		}).Error("failed to get framework id")
		return "", "", "", "", &task.BrowseSandboxResponse{
			Error: &task.BrowseSandboxResponse_Error{
				Failure: &task.BrowseSandboxFailure{
					Message: err.Error(),
				},
			},
		}
	}
	return host, agentid, taskid, frameworkid, nil
}

// BrowseSandbox returns the list of sandbox files path, with agent name, agent id and mesos master name & port.
func (m *serviceHandler) BrowseSandbox(
	ctx context.Context,
	req *task.BrowseSandboxRequest) (resp *task.BrowseSandboxResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		if err != nil || resp.GetError() != nil {
			entry := log.WithField("request", req).
				WithField("headers", headers)

			if err != nil {
				entry = entry.WithError(err)
			}
			if resp.GetError() != nil {
				entry = entry.WithField("browse_sandbox_err", resp.GetError().String())
			}
			entry.Warn("TaskManager.BrowseSandbox failed")
			return
		}

		log.WithField("request", req).
			WithField("headers", headers).
			Debug("TaskManager.BrowseSandbox succeeded")
	}()

	m.metrics.TaskAPIListLogs.Inc(1)

	jobConfig, err := handlerutil.GetJobConfigWithoutFillingCache(
		ctx, req.JobId, m.jobFactory, m.jobConfigOps)
	if err != nil {
		log.WithField("job_id", req.JobId.Value).
			WithError(err).
			Debug("Failed to get job config")
		m.metrics.TaskListLogsFail.Inc(1)
		return &task.BrowseSandboxResponse{
			Error: &task.BrowseSandboxResponse_Error{
				NotFound: &pb_errors.JobNotFound{
					Id:      req.JobId,
					Message: fmt.Sprintf("job %v not found, %v", req.JobId, err),
				},
			},
		}, nil
	}

	hostname, agentID, taskID, frameworkID, resp := m.getSandboxPathInfo(ctx,
		jobConfig.GetInstanceCount(), req)
	if resp != nil {
		return resp, nil
	}

	// Extract the IP address + port of the agent, if possible,
	// because the hostname may not be resolvable on the network
	agentIP := hostname
	agentPort := "5051"
	agentResponse, err := m.hostMgrClient.GetMesosAgentInfo(ctx,
		&hostsvc.GetMesosAgentInfoRequest{Hostname: hostname})
	if err == nil && len(agentResponse.Agents) > 0 {
		ip, port, err := util.ExtractIPAndPortFromMesosAgentPID(
			agentResponse.Agents[0].GetPid())
		if err == nil {
			agentIP = ip
			if port != "" {
				agentPort = port
			}
		}
	} else {
		log.WithField("hostname", hostname).Info(
			"Could not get Mesos agent info")
	}

	log.WithFields(log.Fields{
		"hostname":     hostname,
		"ip_address":   agentIP,
		"port":         agentPort,
		"agent_id":     agentID,
		"task_id":      taskID,
		"framework_id": frameworkID,
	}).Debug("Listing sandbox files")

	var logPaths []string
	logPaths, err = m.logManager.ListSandboxFilesPaths(m.mesosAgentWorkDir,
		frameworkID, agentIP, agentPort, agentID, taskID)

	if err != nil {
		m.metrics.TaskListLogsFail.Inc(1)
		log.WithError(err).WithFields(log.Fields{
			"req":          req,
			"hostname":     hostname,
			"ip_address":   agentIP,
			"port":         agentPort,
			"framework_id": frameworkID,
			"agent_id":     agentID,
		}).Error("failed to list slave logs files paths")
		return &task.BrowseSandboxResponse{
			Error: &task.BrowseSandboxResponse_Error{
				Failure: &task.BrowseSandboxFailure{
					Message: fmt.Sprintf(
						"get slave log failed on host:%s due to: %v",
						hostname,
						err,
					),
				},
			},
		}, nil
	}

	mesosMasterHostPortRespose, err := m.hostMgrClient.GetMesosMasterHostPort(ctx, &hostsvc.MesosMasterHostPortRequest{})
	if err != nil {
		m.metrics.TaskListLogsFail.Inc(1)
		log.WithError(err).WithFields(log.Fields{
			"req":          req,
			"hostname":     hostname,
			"framework_id": frameworkID,
			"agent_id":     agentID,
		}).Error("failed to list slave logs files paths")
		return &task.BrowseSandboxResponse{
			Error: &task.BrowseSandboxResponse_Error{
				Failure: &task.BrowseSandboxFailure{
					Message: fmt.Sprintf(
						"%v",
						err,
					),
				},
			},
		}, nil
	}

	m.metrics.TaskListLogs.Inc(1)
	resp = &task.BrowseSandboxResponse{
		Hostname:            agentIP,
		Port:                agentPort,
		Paths:               logPaths,
		MesosMasterHostname: mesosMasterHostPortRespose.Hostname,
		MesosMasterPort:     mesosMasterHostPortRespose.Port,
	}
	log.WithField("response", resp).Debug("TaskSVC.BrowseSandbox returned")
	return resp, nil
}

// TODO: remove this function once eventstream is enabled in RM
// fillReasonForPendingTasksFromResMgr takes a list of taskinfo and
// fills in the reason for pending tasks from ResourceManager.
// All the tasks in `taskInfos` should belong to the same job
func (m *serviceHandler) fillReasonForPendingTasksFromResMgr(
	ctx context.Context,
	jobID *peloton.JobID,
	taskInfos []*task.TaskInfo,
) {
	// only need to consult ResourceManager for PENDING tasks,
	// because only tasks with PENDING states are being processed by ResourceManager
	for _, taskInfo := range taskInfos {
		if taskInfo.GetRuntime().GetState() == task.TaskState_PENDING {
			// attach the reason from the taskEntry in activeRMTasks
			taskEntry := m.activeRMTasks.GetTask(
				util.CreatePelotonTaskID(
					jobID.GetValue(),
					taskInfo.GetInstanceId(),
				),
			)
			if taskEntry != nil {
				taskInfo.GetRuntime().Reason = taskEntry.GetReason()
			}
		}
	}
}

// GetFrameworkID returns the frameworkID.
func (m *serviceHandler) getFrameworkID(ctx context.Context) (string, error) {
	frameworkIDVal, err := m.frameworkInfoStore.GetFrameworkID(ctx, _frameworkName)
	if err != nil {
		return frameworkIDVal, err
	}
	if frameworkIDVal == "" {
		return frameworkIDVal, errEmptyFrameworkID
	}
	return frameworkIDVal, nil
}

// getPodEvents returns all the pod events for given
// job_id + instance_id + optional (run_id)
func (m *serviceHandler) getPodEvents(
	ctx context.Context,
	id *peloton.JobID,
	instanceID uint32,
	runID string) ([]*task.PodEvent, error) {
	var events []*task.PodEvent
	for {
		taskEvents, err := m.podEventsOps.GetAll(ctx, id.GetValue(), instanceID,
			runID)
		if err != nil {
			return nil, err
		}

		if len(taskEvents) == 0 {
			break
		}

		events = append(events, taskEvents...)

		prevRunID, err := util.ParseRunID(taskEvents[0].GetPrevTaskId().GetValue())
		if err != nil {
			return nil, err
		}
		// Reached last run for this task
		if prevRunID == 0 {
			break
		}
		runID = taskEvents[0].GetPrevTaskId().GetValue()
	}

	return events, nil
}

// getTerminalEvents filters input pod events and return on terminal ones
func (m *serviceHandler) getTerminalEvents(
	eventList []*task.PodEvent,
	lastTaskInfo *task.TaskInfo) []*task.TaskInfo {
	var taskInfos []*task.TaskInfo

	for _, event := range eventList {
		taskState := task.TaskState(task.TaskState_value[event.GetActualState()])
		if !util.IsPelotonStateTerminal(taskState) {
			continue
		}

		mesosID := event.GetTaskId().GetValue()
		prevMesosID := event.GetPrevTaskId().GetValue()
		agentID := event.GetAgentID()
		taskInfos = append(taskInfos, &task.TaskInfo{
			InstanceId: lastTaskInfo.GetInstanceId(),
			JobId:      lastTaskInfo.GetJobId(),
			Config:     lastTaskInfo.GetConfig(),
			Runtime: &task.RuntimeInfo{
				State: taskState,
				MesosTaskId: &mesosv1.TaskID{
					Value: &mesosID,
				},
				Host: event.GetHostname(),
				AgentID: &mesosv1.AgentID{
					Value: &agentID,
				},
				Message: event.GetMessage(),
				Reason:  event.GetReason(),
				PrevMesosTaskId: &mesosv1.TaskID{
					Value: &prevMesosID,
				},
			},
		})
	}

	return taskInfos
}

// TODO: remove this function once eventstream is enabled in RM
func convertTaskMapToSlice(taskMaps map[uint32]*task.TaskInfo) []*task.TaskInfo {
	result := make([]*task.TaskInfo, 0, len(taskMaps))
	for _, taskInfo := range taskMaps {
		result = append(result, taskInfo)
	}
	return result
}

// NewTestServiceHandler returns an empty new ServiceHandler ptr for testing.
func NewTestServiceHandler() *serviceHandler {
	return &serviceHandler{}
}
