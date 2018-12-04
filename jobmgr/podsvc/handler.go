package podsvc

import (
	"context"
	"time"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	v0peloton "code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	v1alphapeloton "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"
	pbpod "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/pod"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/pod/svc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"code.uber.internal/infra/peloton/jobmgr/cached"
	jobmgrcommon "code.uber.internal/infra/peloton/jobmgr/common"
	"code.uber.internal/infra/peloton/jobmgr/goalstate"
	"code.uber.internal/infra/peloton/jobmgr/logmanager"
	jobmgrtask "code.uber.internal/infra/peloton/jobmgr/task"
	goalstateutil "code.uber.internal/infra/peloton/jobmgr/util/goalstate"
	handlerutil "code.uber.internal/infra/peloton/jobmgr/util/handler"
	jobutil "code.uber.internal/infra/peloton/jobmgr/util/job"
	taskutil "code.uber.internal/infra/peloton/jobmgr/util/task"
	"code.uber.internal/infra/peloton/leader"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	_frameworkName = "Peloton"
)

type serviceHandler struct {
	jobStore           storage.JobStore
	podStore           storage.TaskStore
	frameworkInfoStore storage.FrameworkInfoStore
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
		if err != nil {
			log.WithField("request", req).
				WithError(err).
				Warn("PodSVC.StartPod failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
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
		taskRuntime, err := cachedTask.GetRunTime(ctx)
		if err != nil {
			return errors.Wrap(err, "fail to get pod runtime")
		}

		// for pod not going to be killed, ignore the request.
		if taskRuntime.GetGoalState() != pbtask.TaskState_KILLED {
			return yarpcerrors.InvalidArgumentErrorf(
				"pod goal state is not killed, ignore the start request")
		}

		taskConfig, _, err := h.podStore.GetTaskConfig(
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
		taskRuntime.Reason = ""

		if _, err = cachedTask.CompareAndSetRuntime(
			ctx, taskRuntime, jobType); err == nil {
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
		if err != nil {
			log.WithField("request", req).
				WithError(err).
				Warn("PodSVC.StopPod failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
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
	}
	err = cachedJob.PatchTasks(ctx, runtimeDiff)

	// We should enqueue the tasks even if PatchTasks fail,
	// because some tasks may get updated successfully in db.
	// We can let goal state engine to decide whether or not to stop.
	h.goalStateDriver.EnqueueTask(
		&v0peloton.JobID{Value: jobID},
		instanceID,
		time.Now(),
	)

	return &svc.StopPodResponse{}, err
}

func (h *serviceHandler) RestartPod(
	ctx context.Context,
	req *svc.RestartPodRequest,
) (resp *svc.RestartPodResponse, err error) {
	defer func() {
		if err != nil {
			log.WithField("request", req).
				WithError(err).
				Warn("PodSVC.RestartPod failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
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
	err = cachedJob.PatchTasks(ctx, runtimeDiff)

	// We should enqueue the tasks even if PatchTasks fail,
	// because some tasks may get updated successfully in db.
	// We can let goal state engine to decide whether or not to restart.
	h.goalStateDriver.EnqueueTask(
		&v0peloton.JobID{Value: jobID},
		instanceID,
		time.Now(),
	)

	return &svc.RestartPodResponse{}, err
}

func (h *serviceHandler) GetPod(
	ctx context.Context,
	req *svc.GetPodRequest,
) (resp *svc.GetPodResponse, err error) {
	defer func() {
		if err != nil {
			log.WithField("request", req).
				WithError(err).
				Warn("PodSVC.GetPod failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
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

	podStatus := convertTaskRuntimeToPodStatus(taskRuntime)

	var podSpec *pbpod.PodSpec
	if !req.GetStatusOnly() {
		taskConfig, _, err := h.podStore.GetTaskConfig(
			ctx,
			pelotonJobID,
			instanceID,
			taskRuntime.GetConfigVersion(),
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get task config")
		}

		podSpec = convertTaskConfigToPodSpec(taskConfig)
	}

	currentPodInfo := &pbpod.PodInfo{
		Spec:   podSpec,
		Status: podStatus,
	}

	events, err := h.podStore.GetPodEvents(ctx, jobID, instanceID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get current pod events")
	}

	var prevPodInfos []*pbpod.PodInfo
	if len(events) != 0 {
		prevPodInfos, err = h.getPodInfoForAllPodRuns(
			ctx,
			jobID,
			instanceID,
			events[0].GetPrevPodId(),
			currentPodInfo,
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
		if err != nil {
			log.WithField("request", req).
				WithError(err).
				Warn("PodSVC.GetPodEvents failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
			Debug("PodSVC.GetPodEvents succeeded")
	}()
	jobID, instanceID, err := util.ParseTaskID(req.GetPodName().GetValue())
	if err != nil {
		return nil, err
	}

	events, err := h.podStore.GetPodEvents(
		ctx,
		jobID,
		instanceID,
		req.GetPodId().GetValue())
	if err != nil {
		return nil, err
	}
	return &svc.GetPodEventsResponse{
		Events: events,
	}, nil
}

func (h *serviceHandler) BrowsePodSandbox(
	ctx context.Context,
	req *svc.BrowsePodSandboxRequest,
) (resp *svc.BrowsePodSandboxResponse, err error) {
	defer func() {
		if err != nil {
			log.WithField("request", req).
				WithError(err).
				Warn("PodSVC.BrowsePodSandbox failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
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
		if err != nil {
			log.WithField("request", req).
				WithError(err).
				Warn("PodSVC.RefreshPod failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
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
	runtime, err := h.podStore.GetTaskRuntime(ctx, pelotonJobID, instanceID)

	if err != nil {
		return nil, errors.Wrap(err, "fail to get task runtime")
	}

	cachedJob := h.jobFactory.AddJob(pelotonJobID)
	if err := cachedJob.ReplaceTasks(map[uint32]*pbtask.RuntimeInfo{
		instanceID: runtime,
	}, true); err != nil {
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
		if err != nil {
			log.WithField("request", req).
				WithError(err).
				Warn("PodSVC.GetPodCache failed")
			err = handlerutil.ConvertToYARPCError(err)
			return
		}

		log.WithField("request", req).
			WithField("response", resp).
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

	runtime, err := cachedTask.GetRunTime(ctx)
	if err != nil {
		return nil,
			errors.Wrap(err, "fail to get task runtime")
	}

	return &svc.GetPodCacheResponse{
		Status: convertTaskRuntimeToPodStatus(runtime),
	}, nil
}

func (h *serviceHandler) DeletePodEvents(
	ctx context.Context,
	req *svc.DeletePodEventsRequest,
) (resp *svc.DeletePodEventsResponse, err error) {
	return &svc.DeletePodEventsResponse{}, nil
}

func (h *serviceHandler) getHostInfo(
	ctx context.Context,
	jobID string,
	instanceID uint32,
	podID string,
) (hostname, podid, agentID string, err error) {
	events, err := h.podStore.GetPodEvents(ctx, jobID, instanceID, podID)
	if err != nil {
		return "", "", "", errors.Wrap(err, "failed to get pod events")
	}

	hostname = ""
	agentID = ""
	for _, event := range events {
		podid = event.GetPodId().GetValue()
		if event.GetActualState() == jobmgrtask.GetDefaultTaskGoalState(pbjob.JobType_SERVICE).String() {
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
	latestPodInfo *pbpod.PodInfo,
) ([]*pbpod.PodInfo, error) {
	var podInfos []*pbpod.PodInfo

	pID := podID.GetValue()
	for {
		events, err := h.podStore.GetPodEvents(ctx, jobID, instanceID, pID)
		if err != nil {
			return nil, err
		}

		if len(events) == 0 {
			break
		}

		prevPodID := events[0].GetPrevPodId().GetValue()
		agentID := events[0].GetAgentId()
		podInfos = append(podInfos, &pbpod.PodInfo{
			Spec: latestPodInfo.GetSpec(),
			Status: &pbpod.PodStatus{
				State: convertTaskStateToPodState(
					pbtask.TaskState(pbtask.TaskState_value[events[0].GetActualState()]),
				),
				DesiredState: latestPodInfo.GetStatus().GetDesiredState(),
				PodId: &v1alphapeloton.PodID{
					Value: events[0].GetPodId().GetValue(),
				},
				Host: events[0].GetHostname(),
				AgentId: &mesos.AgentID{
					Value: &agentID,
				},
				Version:        events[0].GetVersion(),
				DesiredVersion: events[0].GetDesiredVersion(),
				Message:        events[0].GetMessage(),
				Reason:         events[0].GetReason(),
				PrevPodId: &v1alphapeloton.PodID{
					Value: prevPodID,
				},
				DesiredPodId: events[0].GetDesiredPodId(),
			},
		})

		pID = prevPodID
	}

	return podInfos, nil
}

func convertTaskStateToPodState(state pbtask.TaskState) pbpod.PodState {
	switch state {
	case pbtask.TaskState_UNKNOWN:
		return pbpod.PodState_POD_STATE_INVALID
	case pbtask.TaskState_INITIALIZED:
		return pbpod.PodState_POD_STATE_INITIALIZED
	case pbtask.TaskState_PENDING:
		return pbpod.PodState_POD_STATE_PENDING
	case pbtask.TaskState_READY:
		return pbpod.PodState_POD_STATE_READY
	case pbtask.TaskState_PLACING:
		return pbpod.PodState_POD_STATE_PLACING
	case pbtask.TaskState_PLACED:
		return pbpod.PodState_POD_STATE_PLACED
	case pbtask.TaskState_LAUNCHING:
		return pbpod.PodState_POD_STATE_LAUNCHING
	case pbtask.TaskState_LAUNCHED:
		return pbpod.PodState_POD_STATE_LAUNCHED
	case pbtask.TaskState_STARTING:
		return pbpod.PodState_POD_STATE_STARTING
	case pbtask.TaskState_RUNNING:
		return pbpod.PodState_POD_STATE_RUNNING
	case pbtask.TaskState_SUCCEEDED:
		return pbpod.PodState_POD_STATE_SUCCEEDED
	case pbtask.TaskState_FAILED:
		return pbpod.PodState_POD_STATE_FAILED
	case pbtask.TaskState_LOST:
		return pbpod.PodState_POD_STATE_LOST
	case pbtask.TaskState_PREEMPTING:
		return pbpod.PodState_POD_STATE_PREEMPTING
	case pbtask.TaskState_KILLING:
		return pbpod.PodState_POD_STATE_KILLING
	case pbtask.TaskState_KILLED:
		return pbpod.PodState_POD_STATE_KILLED
	case pbtask.TaskState_DELETED:
		return pbpod.PodState_POD_STATE_DELETED
	}
	return pbpod.PodState_POD_STATE_INVALID
}

func convertTaskRuntimeToPodStatus(runtime *pbtask.RuntimeInfo) *pbpod.PodStatus {
	return &pbpod.PodStatus{
		State:          convertTaskStateToPodState(runtime.GetState()),
		PodId:          &v1alphapeloton.PodID{Value: runtime.GetMesosTaskId().GetValue()},
		StartTime:      runtime.GetStartTime(),
		CompletionTime: runtime.GetCompletionTime(),
		Host:           runtime.GetHost(),
		ContainersStatus: []*pbpod.ContainerStatus{
			{
				Ports: runtime.GetPorts(),
				Healthy: &pbpod.HealthStatus{
					State: pbpod.HealthState(runtime.GetHealthy()),
				},
			},
		},
		DesiredState:   convertTaskStateToPodState(runtime.GetGoalState()),
		Message:        runtime.GetMessage(),
		Reason:         runtime.GetReason(),
		FailureCount:   runtime.GetFailureCount(),
		VolumeId:       &v1alphapeloton.VolumeID{Value: runtime.GetVolumeID().GetValue()},
		Version:        jobutil.GetPodEntityVersion(runtime.GetConfigVersion()),
		DesiredVersion: jobutil.GetPodEntityVersion(runtime.GetDesiredConfigVersion()),
		AgentId:        runtime.GetAgentID(),
		Revision: &v1alphapeloton.Revision{
			Version:   runtime.GetRevision().GetVersion(),
			CreatedAt: runtime.GetRevision().GetCreatedAt(),
			UpdatedAt: runtime.GetRevision().GetUpdatedAt(),
			UpdatedBy: runtime.GetRevision().GetUpdatedBy(),
		},
		PrevPodId:     &v1alphapeloton.PodID{Value: runtime.GetPrevMesosTaskId().GetValue()},
		ResourceUsage: runtime.GetResourceUsage(),
		DesiredPodId:  &v1alphapeloton.PodID{Value: runtime.GetDesiredMesosTaskId().GetValue()},
	}
}

func convertTaskConfigToPodSpec(taskConfig *pbtask.TaskConfig) *pbpod.PodSpec {
	var constraint *pbpod.Constraint
	if taskConfig.GetConstraint() != nil {
		constraint = getPodConstraints([]*pbtask.Constraint{taskConfig.GetConstraint()})[0]
	}

	return &pbpod.PodSpec{
		PodName: &v1alphapeloton.PodName{Value: taskConfig.GetName()},
		Labels:  getPodLabels(taskConfig.GetLabels()),
		Containers: []*pbpod.ContainerSpec{
			{
				Name: taskConfig.GetName(),
				Resource: &pbpod.ResourceSpec{
					CpuLimit:    taskConfig.GetResource().GetCpuLimit(),
					MemLimitMb:  taskConfig.GetResource().GetMemLimitMb(),
					DiskLimitMb: taskConfig.GetResource().GetDiskLimitMb(),
					FdLimit:     taskConfig.GetResource().GetFdLimit(),
					GpuLimit:    taskConfig.GetResource().GetGpuLimit(),
				},
				Container: taskConfig.GetContainer(),
				Command:   taskConfig.GetCommand(),
				LivenessCheck: &pbpod.HealthCheckSpec{
					Enabled:                taskConfig.GetHealthCheck().GetEnabled(),
					InitialIntervalSecs:    taskConfig.GetHealthCheck().GetInitialIntervalSecs(),
					IntervalSecs:           taskConfig.GetHealthCheck().GetIntervalSecs(),
					MaxConsecutiveFailures: taskConfig.GetHealthCheck().GetMaxConsecutiveFailures(),
					TimeoutSecs:            taskConfig.GetHealthCheck().GetTimeoutSecs(),
					Type:                   pbpod.HealthCheckSpec_HealthCheckType(taskConfig.GetHealthCheck().GetType()),
					CommandCheck: &pbpod.HealthCheckSpec_CommandCheck{
						Command:             taskConfig.GetHealthCheck().GetCommandCheck().GetCommand(),
						UnshareEnvironments: taskConfig.GetHealthCheck().GetCommandCheck().GetUnshareEnvironments(),
					},
				},
				Ports: getContainerPorts(taskConfig.GetPorts()),
			},
		},
		Constraint: constraint,
		RestartPolicy: &pbpod.RestartPolicy{
			MaxFailures: taskConfig.GetRestartPolicy().GetMaxFailures(),
		},
		Volume: &pbpod.PersistentVolumeSpec{
			ContainerPath: taskConfig.GetVolume().GetContainerPath(),
			SizeMb:        taskConfig.GetVolume().GetSizeMB(),
		},
		PreemptionPolicy: &pbpod.PreemptionPolicy{
			KillOnPreempt: taskConfig.GetPreemptionPolicy().GetKillOnPreempt(),
		},
		Controller:             taskConfig.GetController(),
		KillGracePeriodSeconds: taskConfig.GetKillGracePeriodSeconds(),
		Revocable:              taskConfig.GetRevocable(),
	}
}

func getPodLabels(labels []*v0peloton.Label) []*v1alphapeloton.Label {
	var podLabels []*v1alphapeloton.Label
	for _, l := range labels {
		podLabels = append(podLabels, &v1alphapeloton.Label{
			Key:   l.GetKey(),
			Value: l.GetValue(),
		})
	}
	return podLabels
}

func getPodConstraints(constraints []*pbtask.Constraint) []*pbpod.Constraint {
	var podConstraints []*pbpod.Constraint
	for _, constraint := range constraints {
		podConstraints = append(podConstraints, &pbpod.Constraint{
			Type: pbpod.Constraint_Type(constraint.GetType()),
			LabelConstraint: &pbpod.LabelConstraint{
				Kind: pbpod.LabelConstraint_Kind(
					constraint.GetLabelConstraint().GetKind(),
				),
				Condition: pbpod.LabelConstraint_Condition(
					constraint.GetLabelConstraint().GetCondition(),
				),
				Label: &v1alphapeloton.Label{
					Value: constraint.GetLabelConstraint().GetLabel().GetValue(),
				},
				Requirement: constraint.GetLabelConstraint().GetRequirement(),
			},
			AndConstraint: &pbpod.AndConstraint{
				Constraints: getPodConstraints(constraint.GetAndConstraint().GetConstraints()),
			},
			OrConstraint: &pbpod.OrConstraint{
				Constraints: getPodConstraints(constraint.GetOrConstraint().GetConstraints()),
			},
		})
	}
	return podConstraints
}

func getContainerPorts(ports []*pbtask.PortConfig) []*pbpod.PortSpec {
	var containerPorts []*pbpod.PortSpec
	for _, p := range ports {
		containerPorts = append(
			containerPorts,
			&pbpod.PortSpec{
				Name:    p.GetName(),
				Value:   p.GetValue(),
				EnvName: p.GetEnvName(),
			},
		)
	}
	return containerPorts
}
