package hostmgr

import (
	"context"
	"errors"
	"fmt"

	mesos "mesos/v1"
	sched "mesos/v1/scheduler"

	"peloton/private/hostmgr/hostsvc"

	hostmgr_mesos "code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/hostmgr/offer"
	"code.uber.internal/infra/peloton/util"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"

	log "github.com/Sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"
)

// serviceHandler implements peloton.private.hostmgr.InternalHostService.
type serviceHandler struct {
	client    mpb.Client
	metrics   *Metrics
	offerPool offer.Pool
}

// InitServiceHandler initialize serviceHandler.
func InitServiceHandler(
	d yarpc.Dispatcher,
	client mpb.Client,
	metrics *Metrics,
	offerPool offer.Pool) {
	handler := serviceHandler{
		client:    client,
		metrics:   metrics,
		offerPool: offerPool,
	}

	json.Register(d, json.Procedure("InternalHostService.AcquireHostOffers", handler.AcquireHostOffers))
	json.Register(d, json.Procedure("InternalHostService.ReleaseHostOffers", handler.ReleaseHostOffers))
	json.Register(d, json.Procedure("InternalHostService.LaunchTasks", handler.LaunchTasks))
	json.Register(d, json.Procedure("InternalHostService.KillTasks", handler.KillTasks))
	json.Register(d, json.Procedure("InternalHostService.ReserveResources", handler.ReserveResources))
	json.Register(d, json.Procedure("InternalHostService.UnreserveResources", handler.UnreserveResources))
	json.Register(d, json.Procedure("InternalHostService.CreateVolumes", handler.CreateVolumes))
	json.Register(d, json.Procedure("InternalHostService.DestroyVolumes", handler.DestroyVolumes))
}

func validateConstraints(req *hostsvc.AcquireHostOffersRequest) *hostsvc.InvalidConstraints {
	constraints := req.GetConstraints()
	if len(constraints) <= 0 {
		log.Error("Empty constraints")
		return &hostsvc.InvalidConstraints{
			Message: "Empty constraints",
		}
	}

	return nil
}

// AcquireHostOffers implements InternalHostService.AcquireHostOffers.
func (h *serviceHandler) AcquireHostOffers(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *hostsvc.AcquireHostOffersRequest) (*hostsvc.AcquireHostOffersResponse, yarpc.ResMeta, error) {
	log.WithField("request", body).Debug("AcquireHostOffers called.")

	if invalidConstraints := validateConstraints(body); invalidConstraints != nil {
		h.metrics.AcquireHostOffersInvalid.Inc(1)
		return &hostsvc.AcquireHostOffersResponse{
			Error: &hostsvc.AcquireHostOffersResponse_Error{
				InvalidConstraints: invalidConstraints,
			},
		}, nil, nil
	}

	var constraints []*offer.Constraint
	for _, c := range body.GetConstraints() {
		constraints = append(constraints, &offer.Constraint{*c})
	}

	hostOffers, err := h.offerPool.ClaimForPlace(constraints)
	if err != nil {
		log.WithField("error", err).Error("ClaimForPlace failed")
		return &hostsvc.AcquireHostOffersResponse{
			Error: &hostsvc.AcquireHostOffersResponse_Error{
				Failure: &hostsvc.AcquireHostOffersFailure{
					Message: err.Error(),
				},
			},
		}, nil, nil
	}

	response := hostsvc.AcquireHostOffersResponse{
		HostOffers: []*hostsvc.HostOffer{},
	}

	for hostname, offers := range hostOffers {
		if len(offers) <= 0 {
			log.WithField("host", hostname).Warn("Empty offer slice from host")
			continue
		}

		var resources []*mesos.Resource
		for _, offer := range offers {
			resources = append(resources, offer.GetResources()...)
		}

		hostOffer := hostsvc.HostOffer{
			Hostname:   hostname,
			AgentId:    offers[0].GetAgentId(),
			Attributes: offers[0].GetAttributes(),
			Resources:  resources,
		}

		response.HostOffers = append(response.HostOffers, &hostOffer)
	}

	h.metrics.AcquireHostOffers.Inc(1)

	log.WithField("response", response).Debug("AcquireHostOffers returned")
	return &response, nil, nil
}

// ReleaseHostOffers implements InternalHostService.ReleaseHostOffers.
func (h *serviceHandler) ReleaseHostOffers(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *hostsvc.ReleaseHostOffersRequest) (*hostsvc.ReleaseHostOffersResponse, yarpc.ResMeta, error) {
	log.WithField("request", body).Debug("ReleaseHostOffers called.")
	response := hostsvc.ReleaseHostOffersResponse{}

	for _, hostOffer := range body.GetHostOffers() {
		hostname := hostOffer.GetHostname()
		if err := h.offerPool.ReturnUnusedOffers(hostname); err != nil {
			log.WithField("host", hostname).Error("Cannot return unused offer on host.")
		}
	}

	h.metrics.ReleaseHostOffers.Inc(1)
	return &response, nil, nil
}

// LaunchTasks implements InternalHostService.LaunchTasks.
func (h *serviceHandler) LaunchTasks(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *hostsvc.LaunchTasksRequest) (
	*hostsvc.LaunchTasksResponse,
	yarpc.ResMeta,
	error) {
	log.WithField("request", body).Debug("LaunchTasks called.")

	if err := validateLaunchTasks(body); err != nil {
		h.metrics.LaunchTasksInvalid.Inc(1)
		return &hostsvc.LaunchTasksResponse{
			Error: &hostsvc.LaunchTasksResponse_Error{
				InvalidArgument: &hostsvc.InvalidArgument{
					Message: err.Error(),
				},
			},
		}, nil, nil
	}

	offers, err := h.offerPool.ClaimForLaunch(body.GetHostname())
	if err != nil {
		h.metrics.LaunchTasksInvalidOffers.Inc(1)
		return &hostsvc.LaunchTasksResponse{
			Error: &hostsvc.LaunchTasksResponse_Error{
				InvalidOffers: &hostsvc.InvalidOffers{
					Message: err.Error(),
				},
			},
		}, nil, nil
	}

	var offerIds []*mesos.OfferID
	for _, offer := range offers {
		offerIds = append(offerIds, offer.GetId())
	}

	// TODO: Use `offers` so we can support reservation, port picking, etc.
	log.WithField("offers", offers).Debug("Offers found for launch")

	var mesosTasks []*mesos.TaskInfo
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
				Error: &hostsvc.LaunchTasksResponse_Error{
					InvalidArgument: &hostsvc.InvalidArgument{
						Message: "Cannot get Mesos task info: " + err.Error(),
						InvalidTasks: []*hostsvc.LaunchableTask{
							t,
						},
					},
				},
			}, nil, nil
		}

		mesosTask.AgentId = body.GetAgentId()
		mesosTasks = append(mesosTasks, mesosTask)
		mesosTaskIds = append(mesosTaskIds, *mesosTask.TaskId.Value)
	}

	callType := sched.Call_ACCEPT
	opType := mesos.Offer_Operation_LAUNCH
	msg := &sched.Call{
		FrameworkId: hostmgr_mesos.GetSchedulerDriver().GetFrameworkID(),
		Type:        &callType,
		Accept: &sched.Call_Accept{
			OfferIds: offerIds,
			Operations: []*mesos.Offer_Operation{
				{
					Type: &opType,
					Launch: &mesos.Offer_Operation_Launch{
						TaskInfos: mesosTasks,
					},
				},
			},
		},
	}

	// TODO: add retry / put back offer and tasks in failure scenarios
	msid := hostmgr_mesos.GetSchedulerDriver().GetMesosStreamID()
	err = h.client.Call(msid, msg)
	if err != nil {
		h.metrics.LaunchTasksFail.Inc(int64(len(mesosTasks)))
		log.WithFields(log.Fields{
			"tasks":  mesosTasks,
			"offers": offerIds,
			"error":  err,
		}).Error("Tasks launch failure")

		return &hostsvc.LaunchTasksResponse{
			Error: &hostsvc.LaunchTasksResponse_Error{
				LaunchFailure: &hostsvc.LaunchFailure{
					Message: err.Error(),
				},
			},
		}, nil, nil
	}

	h.metrics.LaunchTasks.Inc(int64(len(mesosTasks)))
	log.WithFields(log.Fields{
		"tasks":  len(mesosTasks),
		"offers": len(offerIds),
	}).Debug("Tasks launched.")

	return &hostsvc.LaunchTasksResponse{}, nil, nil
}

func validateLaunchTasks(request *hostsvc.LaunchTasksRequest) error {
	if len(request.Tasks) <= 0 {
		return errors.New("Empty task list in LaunchTasksRequest")
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
	log.WithField("request", body).Debug("KillTasks called.")
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
