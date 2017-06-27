package upgrade

import (
	"context"
	"fmt"

	"code.uber.internal/infra/peloton/.gen/peloton/api/errors"
	"code.uber.internal/infra/peloton/.gen/peloton/api/upgrade"
	"code.uber.internal/infra/peloton/jobmgr/job"
	"code.uber.internal/infra/peloton/storage"
	"go.uber.org/yarpc"
)

// InitServiceHandler initalizes the upgrade manager.
func InitServiceHandler(d *yarpc.Dispatcher, jobStore storage.JobStore) {
	handler := &serviceHandler{
		jobStore: jobStore,
	}

	d.Register(upgrade.BuildUpgradeManagerYarpcProcedures(handler))
}

// serviceHandler implements peloton.api.upgrade.UpgradeManager
type serviceHandler struct {
	jobStore storage.JobStore
}

// Create creates a upgrade workflow for a given job ID.
func (h *serviceHandler) Create(ctx context.Context, req *upgrade.CreateRequest) (*upgrade.CreateResponse, error) {
	jr, err := h.jobStore.GetJobRuntime(ctx, req.GetSpec().GetJobId())
	if err != nil {
		return &upgrade.CreateResponse{
			Response: &upgrade.CreateResponse_NotFound{
				NotFound: &errors.JobNotFound{
					Message: err.Error(),
				},
			},
		}, nil
	}

	if !job.NonTerminatedStates[jr.GetState()] {
		return &upgrade.CreateResponse{
			Response: &upgrade.CreateResponse_NotFound{
				NotFound: &errors.JobNotFound{
					Message: "cannot upgrade terminated job",
				},
			},
		}, nil
	}

	return nil, fmt.Errorf("UpgradeManager.Create is not implemented")
}

func (h *serviceHandler) Get(ctx context.Context, req *upgrade.GetRequest) (*upgrade.GetResponse, error) {
	return nil, fmt.Errorf("UpgradeManager.Get is not implemented")
}

func (h *serviceHandler) Pause(ctx context.Context, req *upgrade.PauseRequest) (*upgrade.PauseResponse, error) {
	return nil, fmt.Errorf("UpgradeManager.Pause is not implemented")
}

func (h *serviceHandler) Resume(ctx context.Context, req *upgrade.ResumeRequest) (*upgrade.ResumeResponse, error) {
	return nil, fmt.Errorf("UpgradeManager.Resume is not implemented")
}

func (h *serviceHandler) List(ctx context.Context, req *upgrade.ListRequest) (*upgrade.ListResponse, error) {
	return nil, fmt.Errorf("UpgradeManager.List is not implemented")
}

func (h *serviceHandler) Abort(ctx context.Context, req *upgrade.AbortRequest) (*upgrade.AbortResponse, error) {
	return nil, fmt.Errorf("UpgradeManager.Abort is not implemented")
}

func (h *serviceHandler) Rollback(ctx context.Context, req *upgrade.RollbackRequest) (*upgrade.RollbackResponse, error) {
	return nil, fmt.Errorf("UpgradeManager.Rollback is not implemented")
}
