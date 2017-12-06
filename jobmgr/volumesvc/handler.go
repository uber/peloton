package volumesvc

import (
	"context"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	volume_svc "code.uber.internal/infra/peloton/.gen/peloton/api/volume/svc"

	"code.uber.internal/infra/peloton/.gen/peloton/api/volume"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
)

var (
	errVolumeNotFound = yarpcerrors.NotFoundErrorf("volume not found")
	errTaskNotFound   = yarpcerrors.NotFoundErrorf("task not found")
	errJobNotFound    = yarpcerrors.NotFoundErrorf("job not found")
	errVolumeInUse    = yarpcerrors.InternalErrorf("volume is being used")
	errVolumeUpdate   = yarpcerrors.InternalErrorf("failed to update volume goalstate")
)

// serviceHandler implements peloton.api.volume.VolumeService
type serviceHandler struct {
	metrics     *Metrics
	jobStore    storage.JobStore
	taskStore   storage.TaskStore
	volumeStore storage.PersistentVolumeStore
}

// InitServiceHandler initialize serviceHandler.
func InitServiceHandler(
	d *yarpc.Dispatcher,
	parent tally.Scope,
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	volumeStore storage.PersistentVolumeStore,
) {

	handler := &serviceHandler{
		metrics:     NewMetrics(parent),
		jobStore:    jobStore,
		taskStore:   taskStore,
		volumeStore: volumeStore,
	}

	d.Register(volume_svc.BuildVolumeServiceYARPCProcedures(handler))
}

// DeleteVolume implements VolumeService.DeleteVolume.
func (h *serviceHandler) DeleteVolume(
	ctx context.Context,
	req *volume_svc.DeleteVolumeRequest,
) (*volume_svc.DeleteVolumeResponse, error) {
	log.WithField("request", req).Info("DeleteVolume called.")
	h.metrics.DeleteVolumeAPI.Inc(1)

	if len(req.GetId().GetValue()) == 0 {
		h.metrics.DeleteVolumeFail.Inc(1)
		return &volume_svc.DeleteVolumeResponse{}, errVolumeNotFound
	}

	pv, err := h.getVolumeInfo(ctx, req.GetId())
	if err != nil {
		log.WithError(err).WithField("volume_id", req.GetId().GetValue()).
			Error("Failed to get persistent volume")
		h.metrics.DeleteVolumeFail.Inc(1)
		return &volume_svc.DeleteVolumeResponse{}, errVolumeNotFound
	}

	taskRuntime, err := h.taskStore.GetTaskRuntime(ctx, pv.GetJobId(), pv.GetInstanceId())
	if err != nil {
		log.WithError(err).WithField("volume_info", pv).
			Error("Failed to get task runtime")
		h.metrics.DeleteVolumeFail.Inc(1)
		return &volume_svc.DeleteVolumeResponse{}, errTaskNotFound
	}

	if taskRuntime.GetVolumeID().GetValue() == req.GetId().GetValue() &&
		(!util.IsPelotonStateTerminal(taskRuntime.GetState()) ||
			!util.IsPelotonStateTerminal(taskRuntime.GetGoalState())) {
		log.WithError(err).WithField("volume_info", pv).
			WithField("task_runtime", taskRuntime).
			Error("Cannot delete volume that is being used")
		h.metrics.DeleteVolumeFail.Inc(1)
		return &volume_svc.DeleteVolumeResponse{}, errVolumeInUse
	}

	pv.GoalState = volume.VolumeState_DELETED
	err = h.volumeStore.UpdatePersistentVolume(ctx, pv)
	if err != nil {
		log.WithError(err).WithField("volume_info", pv).
			Error("Failed to update volume goalstate")
		h.metrics.DeleteVolumeFail.Inc(1)
		return &volume_svc.DeleteVolumeResponse{}, errVolumeUpdate
	}

	log.WithField("request", req).Info("DeleteVolume returned.")
	h.metrics.DeleteVolume.Inc(1)
	return &volume_svc.DeleteVolumeResponse{}, nil
}

// ListVolumes implements VolumeService.ListVolumes.
func (h *serviceHandler) ListVolumes(
	ctx context.Context,
	req *volume_svc.ListVolumesRequest,
) (*volume_svc.ListVolumesResponse, error) {

	log.WithField("request", req).Debug("ListVolumes called")
	h.metrics.ListVolumeAPI.Inc(1)

	if len(req.GetJobId().GetValue()) == 0 {
		h.metrics.ListVolumeFail.Inc(1)
		return &volume_svc.ListVolumesResponse{}, errJobNotFound
	}

	taskInfos, err := h.taskStore.GetTasksForJob(ctx, req.GetJobId())
	if err != nil {
		log.WithError(err).WithField("req", req).Error("Failed to get tasks for job")
		h.metrics.ListVolumeFail.Inc(1)
		return &volume_svc.ListVolumesResponse{}, errTaskNotFound
	}

	result := make(map[string]*volume.PersistentVolumeInfo)
	var volumeInfo *volume.PersistentVolumeInfo
	for _, info := range taskInfos {
		if info.GetRuntime().GetVolumeID() == nil {
			continue
		}
		volumeInfo, err = h.getVolumeInfo(ctx, info.GetRuntime().GetVolumeID())
		if err != nil {
			if err == errVolumeNotFound {
				log.WithField("task_info", info).
					Warn("Failed to get persistent volume for task")
				err = nil
				continue
			}
			log.WithError(err).
				WithField("volume_id", info.GetRuntime().GetVolumeID().GetValue()).
				Error("Failed to get persistent volume")
			h.metrics.GetVolumeFail.Inc(1)
			return &volume_svc.ListVolumesResponse{}, err
		}
		result[volumeInfo.GetId().GetValue()] = volumeInfo
	}

	log.WithField("result", result).Debug("ListVolumes returned")
	h.metrics.ListVolume.Inc(1)
	return &volume_svc.ListVolumesResponse{
		Volumes: result,
	}, nil
}

func (h *serviceHandler) getVolumeInfo(
	ctx context.Context,
	volumeID *peloton.VolumeID) (*volume.PersistentVolumeInfo, error) {

	pv, err := h.volumeStore.GetPersistentVolume(ctx, volumeID)
	if err != nil {
		_, ok := err.(*storage.VolumeNotFoundError)
		if !ok {
			// volume store db read error.
			return nil, yarpcerrors.InternalErrorf("peloton storage read error: " + err.Error())
		}
		return nil, errVolumeNotFound
	}

	return pv, nil
}

// GetVolume implements VolumeService.GetVolume.
func (h *serviceHandler) GetVolume(
	ctx context.Context,
	req *volume_svc.GetVolumeRequest,
) (*volume_svc.GetVolumeResponse, error) {
	log.WithField("request", req).Debug("GetVolume called.")
	h.metrics.GetVolumeAPI.Inc(1)

	pv, err := h.getVolumeInfo(ctx, req.GetId())
	if err != nil {
		log.WithError(err).WithField("volume_id", req.GetId().GetValue()).
			Error("Failed to get persistent volume")
		h.metrics.GetVolumeFail.Inc(1)
		return nil, err
	}

	h.metrics.GetVolume.Inc(1)
	log.WithField("request", req).WithField("resp", pv).Debug("GetVolume returned.")
	return &volume_svc.GetVolumeResponse{
		Result: pv,
	}, nil
}
