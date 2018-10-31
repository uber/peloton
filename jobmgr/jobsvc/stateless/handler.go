package stateless

import (
	"context"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"

	"go.uber.org/yarpc"
)

type serviceHandler struct {
}

// InitV1AlphaJobServiceHandler initializes the Job Manager V1Alpha Service Handler
func InitV1AlphaJobServiceHandler(
	d *yarpc.Dispatcher,
) {
	handler := &serviceHandler{}
	d.Register(svc.BuildJobServiceYARPCProcedures(handler))
}

func (h *serviceHandler) CreateJob(
	ctx context.Context,
	req *svc.CreateJobRequest) (*svc.CreateJobResponse, error) {
	return &svc.CreateJobResponse{}, nil
}

func (h *serviceHandler) ReplaceJob(
	ctx context.Context,
	req *svc.ReplaceJobRequest) (*svc.ReplaceJobResponse, error) {
	return &svc.ReplaceJobResponse{}, nil
}

func (h *serviceHandler) PatchJob(
	ctx context.Context,
	req *svc.PatchJobRequest) (*svc.PatchJobResponse, error) {
	return &svc.PatchJobResponse{}, nil
}

func (h *serviceHandler) RestartJob(
	ctx context.Context,
	req *svc.RestartJobRequest) (*svc.RestartJobResponse, error) {
	return &svc.RestartJobResponse{}, nil
}

func (h *serviceHandler) PauseJobWorkflow(
	ctx context.Context,
	req *svc.PauseJobWorkflowRequest) (*svc.PauseJobWorkflowResponse, error) {
	return &svc.PauseJobWorkflowResponse{}, nil
}

func (h *serviceHandler) ResumeJobWorkflow(
	ctx context.Context,
	req *svc.ResumeJobWorkflowRequest) (*svc.ResumeJobWorkflowResponse, error) {
	return &svc.ResumeJobWorkflowResponse{}, nil
}

func (h *serviceHandler) AbortJobWorkflow(
	ctx context.Context,
	req *svc.AbortJobWorkflowRequest) (*svc.AbortJobWorkflowResponse, error) {
	return &svc.AbortJobWorkflowResponse{}, nil
}

func (h *serviceHandler) StartJob(
	ctx context.Context,
	req *svc.StartJobRequest) (*svc.StartJobResponse, error) {
	return &svc.StartJobResponse{}, nil
}
func (h *serviceHandler) StopJob(
	ctx context.Context,
	req *svc.StopJobRequest) (*svc.StopJobResponse, error) {
	return &svc.StopJobResponse{}, nil
}
func (h *serviceHandler) DeleteJob(
	ctx context.Context,
	req *svc.DeleteJobRequest) (*svc.DeleteJobResponse, error) {
	return &svc.DeleteJobResponse{}, nil
}
func (h *serviceHandler) GetJob(
	ctx context.Context,
	req *svc.GetJobRequest) (*svc.GetJobResponse, error) {
	return &svc.GetJobResponse{}, nil
}
func (h *serviceHandler) ListPods(
	ctx context.Context,
	req *svc.ListPodsRequest) (*svc.ListPodsResponse, error) {
	return &svc.ListPodsResponse{}, nil
}
func (h *serviceHandler) QueryPods(
	ctx context.Context,
	req *svc.QueryPodsRequest) (*svc.QueryPodsResponse, error) {
	return &svc.QueryPodsResponse{}, nil
}
func (h *serviceHandler) QueryJobs(
	ctx context.Context,
	req *svc.QueryJobsRequest) (*svc.QueryJobsResponse, error) {
	return &svc.QueryJobsResponse{}, nil
}
func (h *serviceHandler) ListJobUpdates(
	ctx context.Context,
	req *svc.ListJobUpdatesRequest) (*svc.ListJobUpdatesResponse, error) {
	return &svc.ListJobUpdatesResponse{}, nil
}
func (h *serviceHandler) RefreshJob(
	ctx context.Context,
	req *svc.RefreshJobRequest) (*svc.RefreshJobResponse, error) {
	return &svc.RefreshJobResponse{}, nil
}
func (h *serviceHandler) GetJobCache(
	ctx context.Context,
	req *svc.GetJobCacheRequest) (*svc.GetJobCacheResponse, error) {
	return &svc.GetJobCacheResponse{}, nil
}
