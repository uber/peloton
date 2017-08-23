package volume

import (
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"

	volume_svc "code.uber.internal/infra/peloton/.gen/peloton/api/volume/svc"

	"code.uber.internal/infra/peloton/storage"
)

var (
	errVolumeNotFound = yarpcerrors.NotFoundErrorf("volume not found")
)

// serviceHandler implements peloton.api.volume.VolumeService
type serviceHandler struct {
	metrics     *Metrics
	taskStore   storage.TaskStore
	volumeStore storage.PersistentVolumeStore
}

// InitServiceHandler initialize serviceHandler.
func InitServiceHandler(
	d *yarpc.Dispatcher,
	parent tally.Scope,
	taskStore storage.TaskStore,
	volumeStore storage.PersistentVolumeStore,
) {

	handler := &serviceHandler{
		metrics:     NewMetrics(parent),
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
	return nil, fmt.Errorf("VolumeService.DeleteVolume is not implemented")
}

// ListVolumes implements VolumeService.ListVolumes.
func (h *serviceHandler) ListVolumes(
	ctx context.Context,
	req *volume_svc.ListVolumesRequest,
) (*volume_svc.ListVolumesResponse, error) {
	return nil, fmt.Errorf("VolumeService.ListVolumes is not implemented")
}

// GetVolume implements VolumeService.GetVolume.
func (h *serviceHandler) GetVolume(
	ctx context.Context,
	req *volume_svc.GetVolumeRequest,
) (*volume_svc.GetVolumeResponse, error) {
	log.WithField("request", req).Debug("GetVolume called.")
	h.metrics.GetVolumeAPI.Inc(1)

	volumeID := req.GetId()
	pv, err := h.volumeStore.GetPersistentVolume(ctx, volumeID)
	if err != nil {
		log.WithError(err).WithField("volume_id", volumeID).
			Error("Failed to get persistent volume")
		h.metrics.GetVolumeFail.Inc(1)
		_, ok := err.(*storage.VolumeNotFoundError)
		if !ok {
			// volume store db read error.
			return nil, yarpcerrors.InternalErrorf("peloton storage read error: " + err.Error())
		}
		return nil, errVolumeNotFound
	}

	h.metrics.GetVolume.Inc(1)
	return &volume_svc.GetVolumeResponse{
		Result: pv,
	}, nil
}
