package upgrade

import (
	"context"
	"fmt"

	"github.com/pborman/uuid"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/upgrade"
	"code.uber.internal/infra/peloton/.gen/peloton/api/upgrade/svc"
	"code.uber.internal/infra/peloton/jobmgr/job"
	"code.uber.internal/infra/peloton/storage"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
)

// InitServiceHandler initalizes the upgrade manager.
func InitServiceHandler(
	d *yarpc.Dispatcher,
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	upgradeStore storage.UpgradeStore,
	manager Manager) {
	handler := &serviceHandler{
		jobStore:     jobStore,
		taskStore:    taskStore,
		upgradeStore: upgradeStore,
		manager:      manager,
	}

	d.Register(svc.BuildUpgradeServiceYARPCProcedures(handler))
}

// serviceHandler implements peloton.api.upgrade.UpgradeManager
type serviceHandler struct {
	jobStore     storage.JobStore
	taskStore    storage.TaskStore
	upgradeStore storage.UpgradeStore
	manager      Manager
}

// Create creates a upgrade workflow for a given job ID.
func (h *serviceHandler) Create(ctx context.Context, req *svc.CreateRequest) (*svc.CreateResponse, error) {
	jobID := req.GetJobId()
	runtime, err := h.jobStore.GetJobRuntime(ctx, jobID)
	if err != nil {
		return nil, err
	}

	if !job.NonTerminatedStates[runtime.GetState()] {
		return nil, yarpcerrors.InvalidArgumentErrorf("cannot upgrade terminated job")
	}

	newConfig := req.GetJobConfig()
	if err := h.jobStore.CreateJobConfig(ctx, jobID, newConfig); err != nil {
		return nil, err
	}

	if err := h.taskStore.CreateTaskConfigs(ctx, jobID, newConfig); err != nil {
		return nil, err
	}

	jobUUID := uuid.Parse(jobID.GetValue())
	if jobUUID == nil {
		return nil, yarpcerrors.InvalidArgumentErrorf("JobID must be of UUID format")
	}

	id := &peloton.UpgradeID{
		Value: uuid.NewSHA1(jobUUID, []byte("upgrade")).String(),
	}

	status := &upgrade.Status{
		Id:           id,
		JobId:        jobID,
		State:        upgrade.State_ROLLING_FORWARD,
		NumTasksDone: 0,
	}

	if err := h.upgradeStore.CreateUpgrade(ctx, id, status, req.Options, uint64(runtime.ConfigVersion), newConfig.GetRevision().GetVersion()); err != nil {
		return nil, err
	}

	h.manager.TrackUpgrade(status)

	return &svc.CreateResponse{Id: id}, nil
}

func (h *serviceHandler) Get(ctx context.Context, req *svc.GetRequest) (*svc.GetResponse, error) {
	return nil, fmt.Errorf("UpgradeManager.Get is not implemented")
}

func (h *serviceHandler) Pause(ctx context.Context, req *svc.PauseRequest) (*svc.PauseResponse, error) {
	return nil, fmt.Errorf("UpgradeManager.Pause is not implemented")
}

func (h *serviceHandler) Resume(ctx context.Context, req *svc.ResumeRequest) (*svc.ResumeResponse, error) {
	return nil, fmt.Errorf("UpgradeManager.Resume is not implemented")
}

func (h *serviceHandler) List(ctx context.Context, req *svc.ListRequest) (*svc.ListResponse, error) {
	return nil, fmt.Errorf("UpgradeManager.List is not implemented")
}

func (h *serviceHandler) Abort(ctx context.Context, req *svc.AbortRequest) (*svc.AbortResponse, error) {
	return nil, fmt.Errorf("UpgradeManager.Abort is not implemented")
}

func (h *serviceHandler) Rollback(ctx context.Context, req *svc.RollbackRequest) (*svc.RollbackResponse, error) {
	return nil, fmt.Errorf("UpgradeManager.Rollback is not implemented")
}

func (h *serviceHandler) Delete(ctx context.Context, req *svc.DeleteRequest) (*svc.DeleteResponse, error) {
	return nil, fmt.Errorf("UpgradeManager.Delete is not implemented")
}
