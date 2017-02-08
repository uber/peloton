package hostmgr

import (
	"context"
	"errors"
	"fmt"

	mesos_v1 "mesos/v1"
	sched "mesos/v1/scheduler"

	hostmgr_mesos "code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/pbgen/src/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/util"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"

	log "github.com/Sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"
)

// serviceHandler implements peloton.private.hostmgr.InternalHostService.
type serviceHandler struct {
	client  mpb.Client
	metrics *Metrics

	// TODO(zhitao): Add offer pool batching and implement OfferManager API.

	// TODO(zhitao): Add host batching.
}

// InitServiceHandler initialize serviceHandler.
func InitServiceHandler(
	d yarpc.Dispatcher,
	client mpb.Client,
	metrics *Metrics) {
	handler := serviceHandler{
		client:  client,
		metrics: metrics,
	}

	json.Register(d, json.Procedure("InternalHostService.GetHostOffers", handler.GetHostOffers))
	json.Register(d, json.Procedure("InternalHostService.LaunchTasks", handler.LaunchTasks))
	json.Register(d, json.Procedure("InternalHostService.KillTasks", handler.KillTasks))
	json.Register(d, json.Procedure("InternalHostService.ReserveResources", handler.ReserveResources))
	json.Register(d, json.Procedure("InternalHostService.UnreserveResources", handler.UnreserveResources))
	json.Register(d, json.Procedure("InternalHostService.CreateVolumes", handler.CreateVolumes))
	json.Register(d, json.Procedure("InternalHostService.DestroyVolumes", handler.DestroyVolumes))
}

// GetHostOffers implements InternalHostService.GetHostOffers.
func (h *serviceHandler) GetHostOffers(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *hostsvc.GetHostOffersRequest) (*hostsvc.GetHostOffersResponse, yarpc.ResMeta, error) {
	log.Info("GetHostOffers called.")
	return nil, nil, fmt.Errorf("Unimplemented")
}

// LaunchTasks implements InternalHostService.LaunchTasks.
func (h *serviceHandler) LaunchTasks(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *hostsvc.LaunchTasksRequest) (*hostsvc.LaunchTasksResponse, yarpc.ResMeta, error) {
	log.Info("LaunchTasks called.")

	if err := validateLaunchTasks(body); err != nil {
		h.metrics.LaunchTasksInvalid.Inc(1)
		return &hostsvc.LaunchTasksResponse{
			InvalidArgument: &hostsvc.InvalidArgument{
				Message: err.Error(),
			},
		}, nil, nil
	}

	var mesosTasks []*mesos_v1.TaskInfo
	var mesosTaskIds []string

	for _, t := range body.Tasks {
		mesosTask, err := util.GetMesosTaskInfo(t.TaskId, t.Config)
		if err != nil {
			log.WithFields(log.Fields{
				"error":   err,
				"task_id": t.TaskId,
			}).Error("Fail to get correct Mesos TaskInfo")
			h.metrics.LaunchTasksInvalid.Inc(1)

			return &hostsvc.LaunchTasksResponse{
				InvalidArgument: &hostsvc.InvalidArgument{
					Message: "Cannot get Mesos task info: " + err.Error(),
					InvalidTasks: []*hostsvc.LaunchableTask{
						t,
					},
				},
			}, nil, nil
		}

		mesosTask.AgentId = body.GetAgentId()
		mesosTasks = append(mesosTasks, mesosTask)
		mesosTaskIds = append(mesosTaskIds, *mesosTask.TaskId.Value)
	}

	callType := sched.Call_ACCEPT
	opType := mesos_v1.Offer_Operation_LAUNCH
	msg := &sched.Call{
		FrameworkId: hostmgr_mesos.GetSchedulerDriver().GetFrameworkID(),
		Type:        &callType,
		Accept: &sched.Call_Accept{
			OfferIds: body.OfferIds,
			Operations: []*mesos_v1.Offer_Operation{
				{
					Type: &opType,
					Launch: &mesos_v1.Offer_Operation_Launch{
						TaskInfos: mesosTasks,
					},
				},
			},
		},
	}

	// TODO: add retry / put back offer and tasks in failure scenarios
	msid := hostmgr_mesos.GetSchedulerDriver().GetMesosStreamID()
	err := h.client.Call(msid, msg)
	if err != nil {
		h.metrics.LaunchTasksFail.Inc(int64(len(mesosTasks)))
		log.WithFields(log.Fields{
			"tasks":  mesosTasks,
			"offers": body.OfferIds,
			"error":  err,
		}).Error("Tasks launch failure")

		return &hostsvc.LaunchTasksResponse{
			LaunchFailure: &hostsvc.LaunchFailure{
				Message: err.Error(),
			},
		}, nil, nil
	}

	h.metrics.LaunchTasks.Inc(int64(len(mesosTasks)))
	log.WithFields(log.Fields{
		"tasks":  len(mesosTasks),
		"offers": len(body.OfferIds),
	}).Debug("Tasks launched.")

	return &hostsvc.LaunchTasksResponse{}, nil, nil
}

func validateLaunchTasks(request *hostsvc.LaunchTasksRequest) error {
	if len(request.Tasks) <= 0 {
		return errors.New("Empty task list in LaunchTasksRequest")
	}

	if len(request.OfferIds) <= 0 {
		return errors.New("Empty offer id in LaunchTasksRequest")
	}

	if len(request.GetAgentId().GetValue()) <= 0 {
		return errors.New("Empty agent id in LaunchTasksRequest")
	}

	if len(request.Hostname) <= 0 {
		return errors.New("Empty hostname in LaunchTasksRequest")
	}

	return nil
}

// KillTasks implements InternalHostService.KillTasks.
func (h *serviceHandler) KillTasks(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *hostsvc.KillTasksRequest) (*hostsvc.KillTasksResponse, yarpc.ResMeta, error) {
	log.Info("KillTasks called.")
	return nil, nil, fmt.Errorf("Unimplemented")
}

// ReserveResources implements InternalHostService.ReserveResources.
func (h *serviceHandler) ReserveResources(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *hostsvc.ReserveResourcesRequest) (*hostsvc.ReserveResourcesResponse, yarpc.ResMeta, error) {
	log.Info("ReserveResources called.")
	return nil, nil, fmt.Errorf("Unimplemented")
}

// UnreserveResources implements InternalHostService.UnreserveResources.
func (h *serviceHandler) UnreserveResources(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *hostsvc.UnreserveResourcesRequest) (*hostsvc.UnreserveResourcesResponse, yarpc.ResMeta, error) {
	log.Info("UnreserveResources called.")
	return nil, nil, fmt.Errorf("Unimplemented")
}

// CreateVolumes implements InternalHostService.CreateVolumes.
func (h *serviceHandler) CreateVolumes(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *hostsvc.CreateVolumesRequest) (*hostsvc.CreateVolumesResponse, yarpc.ResMeta, error) {
	log.Info("CreateVolumes called.")
	return nil, nil, fmt.Errorf("Unimplemented")
}

// DestroyVolumes implements InternalHostService.DestroyVolumes.
func (h *serviceHandler) DestroyVolumes(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *hostsvc.DestroyVolumesRequest) (*hostsvc.DestroyVolumesResponse, yarpc.ResMeta, error) {
	log.Info("DestroyVolumes called.")
	return nil, nil, fmt.Errorf("Unimplemented")
}
