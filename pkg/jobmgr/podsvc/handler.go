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

package podsvc

import (
	"context"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	v0peloton "github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	v1alphapeloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/svc"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/common/leader"
	"github.com/uber/peloton/pkg/common/util"
	versionutil "github.com/uber/peloton/pkg/common/util/entityversion"
	yarpcutil "github.com/uber/peloton/pkg/common/util/yarpc"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	"github.com/uber/peloton/pkg/jobmgr/goalstate"
	"github.com/uber/peloton/pkg/jobmgr/logmanager"
	jobmgrtask "github.com/uber/peloton/pkg/jobmgr/task"
	goalstateutil "github.com/uber/peloton/pkg/jobmgr/util/goalstate"
	taskutil "github.com/uber/peloton/pkg/jobmgr/util/task"
	"github.com/uber/peloton/pkg/storage"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	_frameworkName = "Peloton"
)

var _errPodNotInCache = yarpcerrors.InternalErrorf("pod not present in cache, please retry action")

type serviceHandler struct {
	jobStore           storage.JobStore
	podStore           storage.TaskStore
	frameworkInfoStore storage.FrameworkInfoStore
	podEventsOps       ormobjects.PodEventsOps
	taskConfigV2Ops    ormobjects.TaskConfigV2Ops
	jobFactory         cached.JobFactory
	goalStateDriver    goalstate.Driver
	candidate          leader.Candidate
	logManager         logmanager.LogManager
	mesosAgentWorkDir  string
	hostMgrClient      hostsvc.InternalHostServiceYARPCClient
}

// InitV1AlphaPodServiceHandler initializes the Pod Service Handler
func InitV1AlphaPodServiceHandler(
	d *yarpc.Dispatcher,
	jobStore storage.JobStore,
	podStore storage.TaskStore,
	frameworkInfoStore storage.FrameworkInfoStore,
	ormStore *ormobjects.Store,
	jobFactory cached.JobFactory,
	goalStateDriver goalstate.Driver,
	candidate leader.Candidate,
	logManager logmanager.LogManager,
	mesosAgentWorkDir string,
	hostMgrClient hostsvc.InternalHostServiceYARPCClient,
) {
	handler := &serviceHandler{
		jobStore:           jobStore,
		podStore:           podStore,
		frameworkInfoStore: frameworkInfoStore,
		podEventsOps:       ormobjects.NewPodEventsOps(ormStore),
		taskConfigV2Ops:    ormobjects.NewTaskConfigV2Ops(ormStore),
		jobFactory:         jobFactory,
		goalStateDriver:    goalStateDriver,
		candidate:          candidate,
		logManager:         logManager,
		mesosAgentWorkDir:  mesosAgentWorkDir,
		hostMgrClient:      hostMgrClient,
	}
	d.Register(svc.BuildPodServiceYARPCProcedures(handler))
}

func (h *serviceHandler) StartPod(
	ctx context.Context,
	req *svc.StartPodRequest,
) (resp *svc.StartPodResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		if err != nil {
			log.WithField("request", req).
				WithField("headers", headers).
				WithError(err).
				Warn("PodSVC.StartPod failed")
			err = yarpcutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
			WithField("headers", headers).
			Info("PodSVC.StartPod succeeded")
	}()

	if !h.candidate.IsLeader() {
		return nil,
			yarpcerrors.UnavailableErrorf("PodSVC.StartPod is not supported on non-leader")
	}

	jobID, instanceID, err := util.ParseTaskID(req.GetPodName().GetValue())
	if err != nil {
		return nil, err
	}

	cachedJob := h.jobFactory.AddJob(&v0peloton.JobID{Value: jobID})
	cachedConfig, err := cachedJob.GetConfig(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "fail to get job config")
	}

	// change the state of job first
	if err := h.startJob(ctx, cachedJob, cachedConfig); err != nil {
		// enqueue job state to goal state engine and let goal state engine
		// decide if the job state needs to be changed
		goalstate.EnqueueJobWithDefaultDelay(
			&v0peloton.JobID{Value: jobID}, h.goalStateDriver, cachedJob)
		return nil, err
	}

	// then change task state
	cachedTask, err := cachedJob.AddTask(ctx, instanceID)
	if err != nil {
		return nil, errors.Wrap(err, "fail to add pod in job cache")
	}

	err = h.startPod(ctx, cachedJob, cachedTask, cachedConfig.GetType())
	// enqueue the pod/job into goal state engine even in failure case.
	// Because the state may be updated, let goal state engine decide what to do
	h.goalStateDriver.EnqueueTask(&v0peloton.JobID{Value: jobID}, instanceID, time.Now())
	goalstate.EnqueueJobWithDefaultDelay(
		&v0peloton.JobID{Value: jobID}, h.goalStateDriver, cachedJob)
	if err != nil {
		return nil, err
	}
	return &svc.StartPodResponse{}, nil
}

// startJob sets the job state to PENDING, and set the goal state to
// RUNNING/SUCCEEDED based on job config
func (h *serviceHandler) startJob(
	ctx context.Context,
	cachedJob cached.Job,
	cachedConfig jobmgrcommon.JobConfig,
) error {
	count := 0

	for {
		jobRuntime, err := cachedJob.GetRuntime(ctx)
		if err != nil {
			return errors.Wrap(err, "fail to fetch job runtime")
		}

		// batch jobs in terminated state cannot be restarted
		if cachedConfig.GetType() == pbjob.JobType_BATCH &&
			util.IsPelotonJobStateTerminal(jobRuntime.GetState()) {
			return yarpcerrors.InvalidArgumentErrorf("cannot start pod in terminated job")
		}

		// job already in expected state, skip the runtime update
		if jobRuntime.State == pbjob.JobState_PENDING &&
			jobRuntime.GoalState == goalstateutil.GetDefaultJobGoalState(cachedConfig.GetType()) {
			return nil
		}

		jobRuntime.State = pbjob.JobState_PENDING
		jobRuntime.GoalState = goalstateutil.GetDefaultJobGoalState(cachedConfig.GetType())

		// update the job runtime
		if _, err = cachedJob.CompareAndSetRuntime(ctx, jobRuntime); err == nil {
			return nil
		}
		if err == jobmgrcommon.UnexpectedVersionError {
			// concurrency error; retry MaxConcurrencyErrorRetry times
			count = count + 1
			if count < jobmgrcommon.MaxConcurrencyErrorRetry {
				continue
			}
		}
		return errors.Wrap(err, "fail to update job runtime")
	}
}

func (h *serviceHandler) startPod(
	ctx context.Context,
	cachedJob cached.Job,
	cachedTask cached.Task,
	jobType pbjob.JobType,
) error {
	count := 0

	for {
		taskRuntime, err := cachedTask.GetRuntime(ctx)
		if err != nil {
			return errors.Wrap(err, "fail to get pod runtime")
		}

		// for pod that goal state is running, ignore the kill request
		if taskRuntime.GetGoalState() == jobmgrtask.GetDefaultTaskGoalState(jobType) {
			return nil
		}

		if taskRuntime.GetGoalState() == pbtask.TaskState_DELETED {
			return yarpcerrors.InvalidArgumentErrorf(
				"cannot start a pod going to be deleted")
		}

		taskConfig, _, err := h.taskConfigV2Ops.GetTaskConfig(
			ctx,
			cachedJob.ID(),
			cachedTask.ID(),
			taskRuntime.GetConfigVersion(),
		)
		if err != nil {
			return errors.Wrap(err, "fail to get pod config")
		}

		healthState := taskutil.GetInitialHealthState(taskConfig)
		taskutil.RegenerateMesosTaskRuntime(
			cachedJob.ID(),
			cachedTask.ID(),
			taskRuntime,
			healthState,
		)
		taskRuntime.GoalState =
			jobmgrtask.GetDefaultTaskGoalState(jobType)
		taskRuntime.Message = "PodSVC.StartPod request"

		if _, err = cachedJob.CompareAndSetTask(
			ctx,
			cachedTask.ID(),
			taskRuntime,
			false,
		); err == nil {
			return nil
		}
		if err == jobmgrcommon.UnexpectedVersionError {
			count = count + 1
			if count < jobmgrcommon.MaxConcurrencyErrorRetry {
				continue
			}
		}
		return errors.Wrap(err, "fail to update pod runtime")
	}
}

func (h *serviceHandler) StopPod(
	ctx context.Context,
	req *svc.StopPodRequest,
) (resp *svc.StopPodResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		if err != nil {
			log.WithField("request", req).
				WithField("headers", headers).
				WithError(err).
				Warn("PodSVC.StopPod failed")
			err = yarpcutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
			WithField("headers", headers).
			Info("PodSVC.StopPod succeeded")
	}()

	if !h.candidate.IsLeader() {
		return nil,
			yarpcerrors.UnavailableErrorf("PodSVC.StopPod is not supported on non-leader")
	}

	jobID, instanceID, err := util.ParseTaskID(req.GetPodName().GetValue())
	if err != nil {
		return nil, err
	}

	cachedJob := h.jobFactory.AddJob(&v0peloton.JobID{Value: jobID})

	runtimeInfo, err := h.podStore.GetTaskRuntime(
		ctx, cachedJob.ID(), instanceID)
	if err != nil {
		return nil, err
	}

	if runtimeInfo.GetGoalState() == pbtask.TaskState_KILLED {
		// No-op if the pod is already KILLED
		return &svc.StopPodResponse{}, nil
	}

	runtimeDiff := make(map[uint32]jobmgrcommon.RuntimeDiff)
	runtimeDiff[instanceID] = jobmgrcommon.RuntimeDiff{
		jobmgrcommon.GoalStateField: pbtask.TaskState_KILLED,
		jobmgrcommon.MessageField:   "Task stop API request",
		jobmgrcommon.ReasonField:    "",
		jobmgrcommon.TerminationStatusField: &pbtask.TerminationStatus{
			Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_ON_REQUEST,
		},
		jobmgrcommon.DesiredHostField: "",
	}
	_, instancesToRetry, err := cachedJob.PatchTasks(ctx, runtimeDiff, false)

	// We should enqueue the tasks even if PatchTasks fail,
	// because some tasks may get updated successfully in db.
	// We can let goal state engine to decide whether or not to stop.
	h.goalStateDriver.EnqueueTask(
		&v0peloton.JobID{Value: jobID},
		instanceID,
		time.Now(),
	)

	if err == nil && len(instancesToRetry) != 0 {
		return nil, _errPodNotInCache
	}

	return &svc.StopPodResponse{}, err
}

func (h *serviceHandler) RestartPod(
	ctx context.Context,
	req *svc.RestartPodRequest,
) (resp *svc.RestartPodResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		if err != nil {
			log.WithField("request", req).
				WithField("headers", headers).
				WithError(err).
				Warn("PodSVC.RestartPod failed")
			err = yarpcutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
			WithField("headers", headers).
			Info("PodSVC.RestartPod succeeded")
	}()

	if !h.candidate.IsLeader() {
		return nil,
			yarpcerrors.UnavailableErrorf("PodSVC.RestartPod is not supported on non-leader")
	}

	jobID, instanceID, err := util.ParseTaskID(req.GetPodName().GetValue())
	if err != nil {
		return nil, yarpcerrors.InvalidArgumentErrorf("invalid pod name")
	}

	cachedJob := h.jobFactory.AddJob(&v0peloton.JobID{Value: jobID})

	newPodID, err := h.getPodIDForRestart(ctx,
		cachedJob,
		instanceID)
	if err != nil {
		return nil, err
	}

	runtimeDiff := make(map[uint32]jobmgrcommon.RuntimeDiff)
	runtimeDiff[instanceID] = jobmgrcommon.RuntimeDiff{
		jobmgrcommon.DesiredMesosTaskIDField: newPodID,
	}

	if req.GetCheckSla() {
		runtimeDiff[instanceID][jobmgrcommon.TerminationStatusField] = &pbtask.TerminationStatus{
			Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_SLA_AWARE_RESTART,
		}
	} else {
		runtimeDiff[instanceID][jobmgrcommon.TerminationStatusField] = &pbtask.TerminationStatus{
			Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_RESTART,
		}
	}

	instancesSucceeded, instancesToRetry, err := cachedJob.PatchTasks(ctx, runtimeDiff, false)

	// We should enqueue the tasks even if PatchTasks fail,
	// because some tasks may get updated successfully in db.
	// We can let goal state engine to decide whether or not to restart.
	h.goalStateDriver.EnqueueTask(
		&v0peloton.JobID{Value: jobID},
		instanceID,
		time.Now(),
	)

	if err == nil && len(instancesToRetry) != 0 {
		return nil, _errPodNotInCache
	}

	// the restart would violate SLA
	if req.GetCheckSla() && len(instancesSucceeded) == 0 {
		return nil, yarpcerrors.AbortedErrorf("pod restart would violate SLA")
	}

	return &svc.RestartPodResponse{}, err
}

func (h *serviceHandler) GetPod(
	ctx context.Context,
	req *svc.GetPodRequest,
) (resp *svc.GetPodResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		if err != nil {
			log.WithField("request", req).
				WithField("headers", headers).
				WithError(err).
				Warn("PodSVC.GetPod failed")
			err = yarpcutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
			WithField("headers", headers).
			Debug("PodSVC.GetPod succeeded")
	}()

	jobID, instanceID, err := util.ParseTaskID(req.GetPodName().GetValue())
	if err != nil {
		return nil, err
	}

	pelotonJobID := &v0peloton.JobID{Value: jobID}
	taskRuntime, err := h.podStore.GetTaskRuntime(ctx, pelotonJobID, instanceID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get task runtime")
	}

	podStatus := api.ConvertTaskRuntimeToPodStatus(taskRuntime)

	var podSpec *pbpod.PodSpec
	if !req.GetStatusOnly() {
		taskConfig, _, err := h.taskConfigV2Ops.GetTaskConfig(
			ctx,
			pelotonJobID,
			instanceID,
			taskRuntime.GetConfigVersion(),
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get task config")
		}

		podSpec = api.ConvertTaskConfigToPodSpec(
			taskConfig,
			jobID,
			instanceID,
		)
	}

	currentPodInfo := &pbpod.PodInfo{
		Spec:   podSpec,
		Status: podStatus,
	}

	var prevPodInfos []*pbpod.PodInfo
	if req.GetLimit() != 1 {
		prevPodInfos, err = h.getPodInfoForAllPodRuns(
			ctx,
			jobID,
			instanceID,
			podStatus.GetPrevPodId(),
			req.GetStatusOnly(),
			req.GetLimit(),
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get pod info for previous runs")
		}
	}

	return &svc.GetPodResponse{
		Current:  currentPodInfo,
		Previous: prevPodInfos,
	}, nil
}

func (h *serviceHandler) GetPodEvents(
	ctx context.Context,
	req *svc.GetPodEventsRequest,
) (resp *svc.GetPodEventsResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		if err != nil {
			log.WithField("request", req).
				WithField("headers", headers).
				WithError(err).
				Warn("PodSVC.GetPodEvents failed")
			err = yarpcutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
			WithField("headers", headers).
			Debug("PodSVC.GetPodEvents succeeded")
	}()
	jobID, instanceID, err := util.ParseTaskID(req.GetPodName().GetValue())
	if err != nil {
		return nil, err
	}

	podEvents, err := h.podStore.GetPodEvents(
		ctx,
		jobID,
		instanceID,
		req.GetPodId().GetValue())
	if err != nil {
		return nil, errors.Wrap(err, "failed to get pod events from store")
	}

	return &svc.GetPodEventsResponse{
		Events: podEvents,
	}, nil
}

func (h *serviceHandler) BrowsePodSandbox(
	ctx context.Context,
	req *svc.BrowsePodSandboxRequest,
) (resp *svc.BrowsePodSandboxResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		if err != nil {
			log.WithField("request", req).
				WithField("headers", headers).
				WithError(err).
				Warn("PodSVC.BrowsePodSandbox failed")
			err = yarpcutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
			WithField("headers", headers).
			Debug("PodSVC.BrowsePodSandbox succeeded")
	}()

	jobID, instanceID, err := util.ParseTaskID(req.GetPodName().GetValue())
	if err != nil {
		return nil, err
	}

	hostname, agentID, podID, frameworkID, err :=
		h.getSandboxPathInfo(
			ctx,
			jobID,
			instanceID,
			req.GetPodId().GetValue(),
		)
	if err != nil {
		return nil, err
	}

	// Extract the IP address + port of the agent, if possible,
	// because the hostname may not be resolvable on the network
	agentIP := hostname
	agentPort := "5051"
	agentResponse, err := h.hostMgrClient.GetMesosAgentInfo(ctx,
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
		log.WithField("hostname", hostname).
			Info("Could not get Mesos agent info")
	}

	var logPaths []string
	logPaths, err = h.logManager.ListSandboxFilesPaths(
		h.mesosAgentWorkDir,
		frameworkID,
		agentIP,
		agentPort,
		agentID,
		podID,
	)

	if err != nil {
		return nil, err
	}

	mesosMasterHostPortResponse, err := h.hostMgrClient.GetMesosMasterHostPort(
		ctx,
		&hostsvc.MesosMasterHostPortRequest{},
	)
	if err != nil {
		return nil, err
	}

	resp = &svc.BrowsePodSandboxResponse{
		Hostname:            agentIP,
		Port:                agentPort,
		Paths:               logPaths,
		MesosMasterHostname: mesosMasterHostPortResponse.GetHostname(),
		MesosMasterPort:     mesosMasterHostPortResponse.GetPort(),
	}
	return resp, nil
}

func (h *serviceHandler) RefreshPod(
	ctx context.Context,
	req *svc.RefreshPodRequest,
) (resp *svc.RefreshPodResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		if err != nil {
			log.WithField("request", req).
				WithField("headers", headers).
				WithError(err).
				Warn("PodSVC.RefreshPod failed")
			err = yarpcutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
			WithField("headers", headers).
			Info("PodSVC.RefreshPod succeeded")
	}()

	if !h.candidate.IsLeader() {
		return nil,
			yarpcerrors.UnavailableErrorf("PodSVC.RefreshPod is not supported on non-leader")
	}

	jobID, instanceID, err := util.ParseTaskID(req.GetPodName().GetValue())
	if err != nil {
		return nil, err
	}

	pelotonJobID := &v0peloton.JobID{Value: jobID}
	taskInfo, err := h.podStore.GetTaskForJob(ctx, jobID, instanceID)

	if err != nil {
		return nil, errors.Wrap(err, "fail to get task info")
	}

	cachedJob := h.jobFactory.AddJob(pelotonJobID)
	if err := cachedJob.ReplaceTasks(taskInfo, true); err != nil {
		return nil, errors.Wrap(err, "fail to replace task runtime")
	}

	h.goalStateDriver.EnqueueTask(pelotonJobID, instanceID, time.Now())
	goalstate.EnqueueJobWithDefaultDelay(
		pelotonJobID, h.goalStateDriver, cachedJob)

	return &svc.RefreshPodResponse{}, nil
}

func (h *serviceHandler) GetPodCache(
	ctx context.Context,
	req *svc.GetPodCacheRequest,
) (resp *svc.GetPodCacheResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		if err != nil {
			log.WithField("request", req).
				WithField("headers", headers).
				WithError(err).
				Warn("PodSVC.GetPodCache failed")
			err = yarpcutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
			WithField("headers", headers).
			Debug("PodSVC.GetPodCache succeeded")
	}()

	jobID, instanceID, err := util.ParseTaskID(req.GetPodName().GetValue())
	if err != nil {
		return nil, err
	}

	cachedJob := h.jobFactory.GetJob(&v0peloton.JobID{Value: jobID})
	if cachedJob == nil {
		return nil,
			yarpcerrors.NotFoundErrorf("job not found in cache")
	}

	cachedTask := cachedJob.GetTask(instanceID)
	if cachedTask == nil {
		return nil,
			yarpcerrors.NotFoundErrorf("task not found in cache")
	}

	runtime, err := cachedTask.GetRuntime(ctx)
	if err != nil {
		return nil,
			errors.Wrap(err, "fail to get task runtime")
	}

	labels, err := cachedTask.GetLabels(ctx)
	if err != nil {
		return nil,
			errors.Wrap(err, "fail to get task labels")
	}

	return &svc.GetPodCacheResponse{
		Status: api.ConvertTaskRuntimeToPodStatus(runtime),
		Labels: api.ConvertLabels(labels),
	}, nil
}

func (h *serviceHandler) DeletePodEvents(
	ctx context.Context,
	req *svc.DeletePodEventsRequest,
) (resp *svc.DeletePodEventsResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)
		if err != nil {
			log.WithField("request", req).
				WithField("headers", headers).
				WithError(err).
				Warn("PodSVC.DeletePodEvents failed")
			err = yarpcutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
			WithField("headers", headers).
			Info("PodSVC.DeletePodEvents succeeded")
	}()

	jobID, instanceID, err := util.ParseTaskID(req.GetPodName().GetValue())
	if err != nil {
		return nil, err
	}

	runID, err := util.ParseRunID(req.GetPodId().GetValue())
	if err != nil {
		return nil, err
	}

	if err = h.podStore.DeletePodEvents(
		ctx,
		jobID,
		instanceID,
		runID,
		runID+1,
	); err != nil {
		return nil, err
	}

	return &svc.DeletePodEventsResponse{}, nil
}

func (h *serviceHandler) getHostInfo(
	ctx context.Context,
	jobID string,
	instanceID uint32,
	podID string,
) (hostname, podid, agentID string, err error) {
	podEvents, err := h.podStore.GetPodEvents(ctx, jobID, instanceID, podID)
	if err != nil {
		return "", "", "", errors.Wrap(err, "failed to get pod events")
	}

	hostname = ""
	agentID = ""
	for _, event := range podEvents {
		podid = event.GetPodId().GetValue()
		if event.GetActualState() == jobmgrtask.GetDefaultPodGoalState(pbjob.JobType_SERVICE).String() {
			hostname = event.GetHostname()
			agentID = event.GetAgentId()
			break
		}
	}

	return hostname, podid, agentID, nil
}

// getSandboxPathInfo - return details such as hostname, agentID,
// frameworkID and podName to create sandbox path.
func (h *serviceHandler) getSandboxPathInfo(ctx context.Context,
	jobID string,
	instanceID uint32,
	podID string,
) (hostname, agentID, podid, frameworkID string, err error) {
	hostname, podid, agentID, err = h.getHostInfo(
		ctx,
		jobID,
		instanceID,
		podID,
	)

	if err != nil {
		return "", "", "", "", err
	}

	if len(hostname) == 0 || len(agentID) == 0 {
		return "", "", "", "", yarpcerrors.AbortedErrorf("pod has not been run")
	}

	// get framework ID.
	frameworkid, err := h.getFrameworkID(ctx)
	if err != nil {
		return "", "", "", "", err
	}
	return hostname, agentID, podid, frameworkid, nil
}

// GetFrameworkID returns the frameworkID.
func (h *serviceHandler) getFrameworkID(ctx context.Context) (string, error) {
	frameworkIDVal, err := h.frameworkInfoStore.GetFrameworkID(ctx, _frameworkName)
	if err != nil {
		return frameworkIDVal, err
	}
	if frameworkIDVal == "" {
		return frameworkIDVal, yarpcerrors.InternalErrorf("framework id is empty")
	}
	return frameworkIDVal, nil
}

// getPodIDForRestart returns the new pod id for restart
func (h *serviceHandler) getPodIDForRestart(
	ctx context.Context,
	cachedJob cached.Job,
	instanceID uint32) (*mesos.TaskID, error) {
	runtimeInfo, err := h.podStore.GetTaskRuntime(
		ctx, cachedJob.ID(), instanceID)
	if err != nil {
		return nil, err
	}

	runID, err :=
		util.ParseRunID(runtimeInfo.GetMesosTaskId().GetValue())
	if err != nil {
		runID = 0
	}

	return util.CreateMesosTaskID(
		cachedJob.ID(), instanceID, runID+1), nil
}

func (h *serviceHandler) getPodInfoForAllPodRuns(
	ctx context.Context,
	jobID string,
	instanceID uint32,
	podID *v1alphapeloton.PodID,
	statusOnly bool,
	limit uint32,
) ([]*pbpod.PodInfo, error) {
	var podInfos []*pbpod.PodInfo

	pID := podID.GetValue()
	for {
		podEvents, err := h.podStore.GetPodEvents(ctx, jobID, instanceID, pID)
		if err != nil {
			return nil, err
		}

		if len(podEvents) == 0 {
			break
		}

		if limit > 0 && uint32(len(podInfos)) >= (limit-1) {
			break
		}

		prevPodID := podEvents[0].GetPrevPodId().GetValue()
		agentID := podEvents[0].GetAgentId()
		podInfo := &pbpod.PodInfo{
			Status: &pbpod.PodStatus{
				State: pbpod.PodState(
					pbpod.PodState_value[podEvents[0].GetActualState()],
				),
				DesiredState: pbpod.PodState(
					pbpod.PodState_value[podEvents[0].GetDesiredState()],
				),
				PodId: &v1alphapeloton.PodID{
					Value: podEvents[0].GetPodId().GetValue(),
				},
				Host: podEvents[0].GetHostname(),
				AgentId: &mesos.AgentID{
					Value: &agentID,
				},
				Version:        podEvents[0].GetVersion(),
				DesiredVersion: podEvents[0].GetDesiredVersion(),
				Message:        podEvents[0].GetMessage(),
				Reason:         podEvents[0].GetReason(),
				PrevPodId: &v1alphapeloton.PodID{
					Value: prevPodID,
				},
				DesiredPodId: podEvents[0].GetDesiredPodId(),
			},
		}
		podInfos = append(podInfos, podInfo)
		pID = prevPodID

		if !statusOnly {
			configVersion, err := versionutil.GetConfigVersion(
				podInfo.GetStatus().GetVersion(),
			)
			if err != nil {
				return nil,
					errors.Wrap(err, "failed to get config version for pod run")
			}

			taskConfig, _, err := h.taskConfigV2Ops.GetTaskConfig(
				ctx,
				&v0peloton.JobID{Value: jobID},
				instanceID,
				configVersion,
			)
			if err != nil {
				// If we aren't able to get pod spec for a particular run,
				// then we should just continue and fill up whatever
				// we can instead of throwing an error.
				if yarpcerrors.IsNotFound(err) {
					log.WithFields(
						log.Fields{
							"job_id":      jobID,
							"instance_id": instanceID,
						}).WithError(err).
						Info("failed to get task config")
					continue
				}
				return nil, errors.Wrap(err, "failed to get task config")
			}

			spec := api.ConvertTaskConfigToPodSpec(
				taskConfig,
				jobID,
				instanceID,
			)
			spec.PodName = &v1alphapeloton.PodName{
				Value: util.CreatePelotonTaskID(jobID, instanceID),
			}
			podInfo.Spec = spec
		}
	}

	return podInfos, nil
}

// NewTestServiceHandler returns an empty new ServiceHandler ptr for testing.
func NewTestServiceHandler() *serviceHandler {
	return &serviceHandler{}
}
