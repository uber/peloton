package podsvc

import (
	"context"
	"fmt"
	"time"

	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	v1alphapeloton "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"
	pbpod "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/pod"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/pod/svc"

	jobmgrcommon "code.uber.internal/infra/peloton/jobmgr/common"
	"code.uber.internal/infra/peloton/jobmgr/goalstate"
	jobmgrtask "code.uber.internal/infra/peloton/jobmgr/task"
	goalstateutil "code.uber.internal/infra/peloton/jobmgr/util/goalstate"
	handlerutil "code.uber.internal/infra/peloton/jobmgr/util/handler"
	taskutil "code.uber.internal/infra/peloton/jobmgr/util/task"

	"code.uber.internal/infra/peloton/jobmgr/cached"
	"code.uber.internal/infra/peloton/leader"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
)

type serviceHandler struct {
	podStore        storage.TaskStore
	jobFactory      cached.JobFactory
	goalStateDriver goalstate.Driver
	candidate       leader.Candidate
}

// InitV1AlphaPodServiceHandler initializes the Pod Service Handler
func InitV1AlphaPodServiceHandler(
	d *yarpc.Dispatcher,
	podStore storage.TaskStore,
	jobFactory cached.JobFactory,
	goalStateDriver goalstate.Driver,
	candidate leader.Candidate,
) {
	handler := &serviceHandler{
		jobFactory:      jobFactory,
		podStore:        podStore,
		goalStateDriver: goalStateDriver,
		candidate:       candidate,
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

	cachedJob := h.jobFactory.AddJob(&peloton.JobID{Value: jobID})
	cachedConfig, err := cachedJob.GetConfig(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "fail to get job config")
	}

	// change the state of job first
	if err := h.startJob(ctx, cachedJob, cachedConfig); err != nil {
		// enqueue job state to goal state engine and let goal state engine
		// decide if the job state needs to be changed
		goalstate.EnqueueJobWithDefaultDelay(
			&peloton.JobID{Value: jobID}, h.goalStateDriver, cachedJob)
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
	h.goalStateDriver.EnqueueTask(&peloton.JobID{Value: jobID}, instanceID, time.Now())
	goalstate.EnqueueJobWithDefaultDelay(
		&peloton.JobID{Value: jobID}, h.goalStateDriver, cachedJob)
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
	return &svc.StopPodResponse{}, nil
}

func (h *serviceHandler) RestartPod(
	ctx context.Context,
	req *svc.RestartPodRequest,
) (resp *svc.RestartPodResponse, err error) {
	return &svc.RestartPodResponse{}, nil
}

func (h *serviceHandler) GetPod(
	ctx context.Context,
	req *svc.GetPodRequest,
) (*svc.GetPodResponse, error) {
	return &svc.GetPodResponse{}, nil
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
	return &svc.BrowsePodSandboxResponse{}, nil
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

	pelotonJobID := &peloton.JobID{Value: jobID}
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

	cachedJob := h.jobFactory.GetJob(&peloton.JobID{Value: jobID})
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
		Status: convertToPodStatus(runtime),
	}, nil
}

func (h *serviceHandler) DeletePodEvents(
	ctx context.Context,
	req *svc.DeletePodEventsRequest,
) (resp *svc.DeletePodEventsResponse, err error) {
	return &svc.DeletePodEventsResponse{}, nil
}

func convertToPodStatus(runtime *pbtask.RuntimeInfo) *pbpod.PodStatus {
	return &pbpod.PodStatus{
		State:          pbpod.PodState(runtime.GetState()),
		PodId:          &v1alphapeloton.PodID{Value: runtime.GetMesosTaskId().GetValue()},
		StartTime:      runtime.GetStartTime(),
		CompletionTime: runtime.GetCompletionTime(),
		Host:           runtime.GetHost(),
		Ports:          runtime.GetPorts(),
		DesiredState:   pbpod.PodState(runtime.GetGoalState()),
		Message:        runtime.GetMessage(),
		Reason:         runtime.GetReason(),
		FailureCount:   runtime.GetFailureCount(),
		VolumeId:       &v1alphapeloton.VolumeID{Value: runtime.GetVolumeID().GetValue()},
		JobVersion: &v1alphapeloton.EntityVersion{
			Value: fmt.Sprintf("%d", runtime.GetConfigVersion())},
		DesiredJobVersion: &v1alphapeloton.EntityVersion{
			Value: fmt.Sprintf("%d", runtime.GetDesiredConfigVersion())},
		AgentId: runtime.GetAgentID(),
		Revision: &v1alphapeloton.Revision{
			Version:   runtime.GetRevision().GetVersion(),
			CreatedAt: runtime.GetRevision().GetCreatedAt(),
			UpdatedAt: runtime.GetRevision().GetUpdatedAt(),
			UpdatedBy: runtime.GetRevision().GetUpdatedBy(),
		},
		PrevPodId:     &v1alphapeloton.PodID{Value: runtime.GetPrevMesosTaskId().GetValue()},
		ResourceUsage: runtime.GetResourceUsage(),
		Healthy:       pbpod.HealthState(runtime.GetHealthy()),
		DesiredPodId:  &v1alphapeloton.PodID{Value: runtime.GetDesiredMesosTaskId().GetValue()},
	}
}
