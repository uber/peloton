package podsvc

import (
	"context"
	"fmt"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	v1alphapeloton "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"
	pbpod "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/pod"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/pod/svc"

	"code.uber.internal/infra/peloton/jobmgr/cached"
	handlerutil "code.uber.internal/infra/peloton/jobmgr/util/handler"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
)

type serviceHandler struct {
	jobFactory cached.JobFactory
	podStore   storage.TaskStore
}

// InitV1AlphaPodServiceHandler initializes the Pod Service Handler
func InitV1AlphaPodServiceHandler(
	d *yarpc.Dispatcher,
	jobFactory cached.JobFactory,
	podStore storage.TaskStore,
) {
	handler := &serviceHandler{
		jobFactory: jobFactory,
		podStore:   podStore,
	}
	d.Register(svc.BuildPodServiceYARPCProcedures(handler))
}

func (h *serviceHandler) StartPod(
	ctx context.Context,
	req *svc.StartPodRequest,
) (resp *svc.StartPodResponse, err error) {
	return &svc.StartPodResponse{}, nil
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
		return nil, yarpcerrors.InvalidArgumentErrorf(err.Error())
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
		return nil, yarpcerrors.InvalidArgumentErrorf(err.Error())
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
