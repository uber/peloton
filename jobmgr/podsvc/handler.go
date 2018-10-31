package podsvc

import (
	"context"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/pod/svc"

	"go.uber.org/yarpc"
)

type serviceHandler struct {
}

// InitV1AlphaPodServiceHandler initializes the Pod Service Handler
func InitV1AlphaPodServiceHandler(
	d *yarpc.Dispatcher,
) {
	handler := &serviceHandler{}
	d.Register(svc.BuildPodServiceYARPCProcedures(handler))
}

func (h *serviceHandler) StartPod(
	ctx context.Context,
	req *svc.StartPodRequest,
) (*svc.StartPodResponse, error) {
	return &svc.StartPodResponse{}, nil
}

func (h *serviceHandler) StopPod(
	ctx context.Context,
	req *svc.StopPodRequest,
) (*svc.StopPodResponse, error) {
	return &svc.StopPodResponse{}, nil
}

func (h *serviceHandler) RestartPod(
	ctx context.Context,
	req *svc.RestartPodRequest,
) (*svc.RestartPodResponse, error) {
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
) (*svc.GetPodEventsResponse, error) {
	return &svc.GetPodEventsResponse{}, nil
}

func (h *serviceHandler) BrowsePodSandbox(
	ctx context.Context,
	req *svc.BrowsePodSandboxRequest,
) (*svc.BrowsePodSandboxResponse, error) {
	return &svc.BrowsePodSandboxResponse{}, nil
}

func (h *serviceHandler) RefreshPod(
	ctx context.Context,
	req *svc.RefreshPodRequest,
) (*svc.RefreshPodResponse, error) {
	return &svc.RefreshPodResponse{}, nil
}

func (h *serviceHandler) GetPodCache(
	ctx context.Context,
	req *svc.GetPodCacheRequest,
) (*svc.GetPodCacheResponse, error) {
	return &svc.GetPodCacheResponse{}, nil
}

func (h *serviceHandler) DeletePodEvents(
	ctx context.Context,
	req *svc.DeletePodEventsRequest,
) (*svc.DeletePodEventsResponse, error) {
	return &svc.DeletePodEventsResponse{}, nil
}
