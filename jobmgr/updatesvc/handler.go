package updatesvc

import (
	"context"

	"github.com/pborman/uuid"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/update/svc"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
)

// InitServiceHandler initalizes the update service.
func InitServiceHandler(
	d *yarpc.Dispatcher,
	jobStore storage.JobStore,
	updateStore storage.UpdateStore) {
	handler := &serviceHandler{
		jobStore:    jobStore,
		updateStore: updateStore,
	}

	d.Register(svc.BuildUpdateServiceYARPCProcedures(handler))
}

// serviceHandler implements peloton.api.update.svc
type serviceHandler struct {
	jobStore    storage.JobStore
	updateStore storage.UpdateStore
}

// Create creates an update for a given job ID.
func (h *serviceHandler) CreateUpdate(ctx context.Context, req *svc.CreateUpdateRequest) (*svc.CreateUpdateResponse, error) {
	jobID := req.GetJobId()
	jr, err := h.jobStore.GetJobRuntime(ctx, jobID)
	if err != nil {
		return nil, yarpcerrors.NotFoundErrorf("job not found")
	}

	if util.IsPelotonJobStateTerminal(jr.GetState()) {
		return nil, yarpcerrors.InvalidArgumentErrorf("cannot update terminated job")
	}

	jobUUID := uuid.Parse(jobID.GetValue())
	if jobUUID == nil {
		return nil, yarpcerrors.InvalidArgumentErrorf("JobID must be of UUID format")
	}

	id := &update.UpdateID{
		Value: uuid.NewSHA1(jobUUID, []byte("update")).String(),
	}

	if err := h.updateStore.CreateUpdate(ctx, id, jobID, req.GetJobConfig(), req.GetUpdateConfig()); err != nil {
		return nil, yarpcerrors.AlreadyExistsErrorf("failed to create update in DB")
	}

	return &svc.CreateUpdateResponse{
		Result: id,
	}, nil
}

func (h *serviceHandler) GetUpdate(ctx context.Context, req *svc.GetUpdateRequest) (*svc.GetUpdateResponse, error) {
	return nil, yarpcerrors.UnimplementedErrorf("UpdateService.GetUpdate is not implemented")
}

func (h *serviceHandler) PauseUpdate(ctx context.Context, req *svc.PauseUpdateRequest) (*svc.PauseUpdateResponse, error) {
	return nil, yarpcerrors.UnimplementedErrorf("UpdateService.PauseUpdate is not implemented")
}

func (h *serviceHandler) ResumeUpdate(ctx context.Context, req *svc.ResumeUpdateRequest) (*svc.ResumeUpdateResponse, error) {
	return nil, yarpcerrors.UnimplementedErrorf("UpdateService.ResumeUpdate is not implemented")
}

func (h *serviceHandler) ListUpdates(ctx context.Context, req *svc.ListUpdatesRequest) (*svc.ListUpdatesResponse, error) {
	return nil, yarpcerrors.UnimplementedErrorf("UpdateService.ListUpdate is not implemented")
}

func (h *serviceHandler) AbortUpdate(ctx context.Context, req *svc.AbortUpdateRequest) (*svc.AbortUpdateResponse, error) {
	return nil, yarpcerrors.UnimplementedErrorf("UpdateService.AbortUpdate is not implemented")
}

func (h *serviceHandler) RollbackUpdate(ctx context.Context, req *svc.RollbackUpdateRequest) (*svc.RollbackUpdateResponse, error) {
	return nil, yarpcerrors.UnimplementedErrorf("UpdateService.RollbackUpdate is not implemented")
}
