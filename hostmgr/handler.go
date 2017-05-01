package hostmgr

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"

	mesos "mesos/v1"
	sched "mesos/v1/scheduler"
	"peloton/private/hostmgr/hostsvc"

	"code.uber.internal/infra/peloton/common"
	hostmgr_mesos "code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/hostmgr/offer"
	"code.uber.internal/infra/peloton/hostmgr/scalar"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
)

// serviceHandler implements peloton.private.hostmgr.InternalHostService.
type serviceHandler struct {
	schedulerClient       mpb.Client
	mOperatorClient       mpb.MasterOperatorClient
	metrics               *Metrics
	offerPool             offer.Pool
	frameworkInfoProvider hostmgr_mesos.FrameworkInfoProvider
}

// InitServiceHandler initialize serviceHandler.
func InitServiceHandler(
	d yarpc.Dispatcher,
	parent tally.Scope,
	schedulerClient mpb.Client,
	operatorMClient mpb.MasterOperatorClient,
	frameworkInfoProvider hostmgr_mesos.FrameworkInfoProvider,
) {

	handler := serviceHandler{
		schedulerClient:       schedulerClient,
		mOperatorClient:       operatorMClient,
		metrics:               NewMetrics(parent.SubScope("hostmgr")),
		offerPool:             offer.GetEventHandler().GetOfferPool(),
		frameworkInfoProvider: frameworkInfoProvider,
	}

	d.Register(json.Procedure(
		"InternalHostService.AcquireHostOffers", handler.AcquireHostOffers))
	d.Register(json.Procedure(
		"InternalHostService.ReleaseHostOffers", handler.ReleaseHostOffers))
	d.Register(json.Procedure(
		"InternalHostService.LaunchTasks", handler.LaunchTasks))
	d.Register(json.Procedure(
		"InternalHostService.KillTasks", handler.KillTasks))
	d.Register(json.Procedure(
		"InternalHostService.ReserveResources", handler.ReserveResources))
	d.Register(json.Procedure(
		"InternalHostService.UnreserveResources", handler.UnreserveResources))
	d.Register(json.Procedure(
		"InternalHostService.CreateVolumes", handler.CreateVolumes))
	d.Register(json.Procedure(
		"InternalHostService.DestroyVolumes", handler.DestroyVolumes))
	d.Register(json.Procedure(
		"InternalHostService.ClusterCapacity",
		handler.ClusterCapacity))
}

func validateConstraint(
	req *hostsvc.AcquireHostOffersRequest) *hostsvc.InvalidConstraint {

	if req.GetConstraint() == nil {
		log.WithField("request", req).Warn("Empty constraint")
		return &hostsvc.InvalidConstraint{
			Message: "Empty constraint",
		}
	}

	return nil
}

// AcquireHostOffers implements InternalHostService.AcquireHostOffers.
func (h *serviceHandler) AcquireHostOffers(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *hostsvc.AcquireHostOffersRequest,
) (*hostsvc.AcquireHostOffersResponse, yarpc.ResMeta, error) {

	log.WithField("request", body).Debug("AcquireHostOffers called.")

	if invalid := validateConstraint(body); invalid != nil {
		h.metrics.AcquireHostOffersInvalid.Inc(1)
		return &hostsvc.AcquireHostOffersResponse{
			Error: &hostsvc.AcquireHostOffersResponse_Error{
				InvalidConstraint: invalid,
			},
		}, nil, nil
	}

	hostOffers, err := h.offerPool.ClaimForPlace(body.GetConstraint())
	if err != nil {
		log.WithError(err).Warn("ClaimForPlace failed")
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
			log.WithField("host", hostname).
				Warn("Empty offer slice from host")
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
	body *hostsvc.ReleaseHostOffersRequest) (
	*hostsvc.ReleaseHostOffersResponse, yarpc.ResMeta, error) {

	log.WithField("request", body).Debug("ReleaseHostOffers called.")
	response := hostsvc.ReleaseHostOffersResponse{}

	for _, hostOffer := range body.GetHostOffers() {
		hostname := hostOffer.GetHostname()
		if err := h.offerPool.ReturnUnusedOffers(hostname); err != nil {
			log.WithField("host", hostname).
				Warn("Cannot return unused offer on host.")
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
	var mesosResources []*mesos.Resource
	for _, offer := range offers {
		offerIds = append(offerIds, offer.GetId())
		mesosResources = append(mesosResources, offer.GetResources()...)
	}

	// TODO: Use `offers` so we can support reservation, port picking, etc.
	log.WithField("offers", offers).Debug("Offers found for launch")

	var mesosTasks []*mesos.TaskInfo
	var mesosTaskIds []string

	builder := newTaskBuilder(mesosResources)

	for _, t := range body.Tasks {
		mesosTask, err := builder.build(t.TaskId, t.Config)
		if err != nil {
			log.WithFields(log.Fields{
				"error":   err,
				"task_id": t.TaskId,
			}).Warn("Fail to get correct Mesos TaskInfo")
			h.metrics.LaunchTasksInvalid.Inc(1)

			// For now, decline all offers to Mesos in the hope that next
			// call to pool will select some different host.
			// An alternative is to mark offers on the host as ready.
			if err := h.offerPool.DeclineOffers(offers); err != nil {
				log.WithError(err).
					WithField("offers", offers).
					Warn("Cannot decline offers task building error")
			}

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
		FrameworkId: h.frameworkInfoProvider.GetFrameworkID(),
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

	log.WithFields(log.Fields{
		"call": msg,
	}).Debug("Launching tasks to Mesos.")

	// TODO: add retry / put back offer and tasks in failure scenarios
	msid := h.frameworkInfoProvider.GetMesosStreamID()
	err = h.schedulerClient.Call(msid, msg)
	if err != nil {
		h.metrics.LaunchTasksFail.Inc(int64(len(mesosTasks)))
		log.WithFields(log.Fields{
			"tasks":  mesosTasks,
			"offers": offerIds,
			"error":  err,
		}).Warn("Tasks launch failure")

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
	body *hostsvc.KillTasksRequest) (
	*hostsvc.KillTasksResponse, yarpc.ResMeta, error) {

	log.WithField("request", body).Debug("KillTasks called.")
	taskIds := body.GetTaskIds()
	if len(taskIds) == 0 {
		return &hostsvc.KillTasksResponse{
			Error: &hostsvc.KillTasksResponse_Error{
				InvalidTaskIDs: &hostsvc.InvalidTaskIDs{
					Message: "Empty task ids",
				},
			},
		}, nil, nil
	}

	var wg sync.WaitGroup
	failedMutex := &sync.Mutex{}
	var failedTaskIds []*mesos.TaskID
	var errs []string
	for _, taskID := range taskIds {
		wg.Add(1)
		go func(taskID *mesos.TaskID) {
			defer wg.Done()
			callType := sched.Call_KILL
			msg := &sched.Call{
				FrameworkId: h.frameworkInfoProvider.GetFrameworkID(),
				Type:        &callType,
				Kill: &sched.Call_Kill{
					TaskId: taskID,
				},
			}

			msid := h.frameworkInfoProvider.GetMesosStreamID()
			err := h.schedulerClient.Call(msid, msg)
			if err != nil {
				h.metrics.KillTasksFail.Inc(1)
				log.WithFields(log.Fields{
					"task_id": taskID,
					"error":   err,
				}).Error("Kill task failure")

				failedMutex.Lock()
				defer failedMutex.Unlock()
				failedTaskIds = append(failedTaskIds, taskID)
				errs = append(errs, err.Error())
				return
			}

			h.metrics.KillTasks.Inc(1)
			log.WithField("task", taskID).Info("Task kill request sent")
		}(taskID)
	}

	wg.Wait()

	if len(failedTaskIds) > 0 {
		return &hostsvc.KillTasksResponse{
			Error: &hostsvc.KillTasksResponse_Error{
				KillFailure: &hostsvc.KillFailure{
					Message: strings.Join(errs, ";"),
					TaskIds: failedTaskIds,
				},
			},
		}, nil, nil
	}

	return &hostsvc.KillTasksResponse{}, nil, nil
}

// ReserveResources implements InternalHostService.ReserveResources.
func (h *serviceHandler) ReserveResources(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *hostsvc.ReserveResourcesRequest) (
	*hostsvc.ReserveResourcesResponse, yarpc.ResMeta, error) {

	log.Debug("ReserveResources called.")
	return nil, nil, fmt.Errorf("Unimplemented")
}

// UnreserveResources implements InternalHostService.UnreserveResources.
func (h *serviceHandler) UnreserveResources(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *hostsvc.UnreserveResourcesRequest) (
	*hostsvc.UnreserveResourcesResponse, yarpc.ResMeta, error) {

	log.Debug("UnreserveResources called.")
	return nil, nil, fmt.Errorf("Unimplemented")
}

// CreateVolumes implements InternalHostService.CreateVolumes.
func (h *serviceHandler) CreateVolumes(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *hostsvc.CreateVolumesRequest) (
	*hostsvc.CreateVolumesResponse, yarpc.ResMeta, error) {

	log.Debug("CreateVolumes called.")
	return nil, nil, fmt.Errorf("Unimplemented")
}

// DestroyVolumes implements InternalHostService.DestroyVolumes.
func (h *serviceHandler) DestroyVolumes(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *hostsvc.DestroyVolumesRequest) (
	*hostsvc.DestroyVolumesResponse, yarpc.ResMeta, error) {

	log.Debug("DestroyVolumes called.")
	return nil, nil, fmt.Errorf("Unimplemented")
}

// ClusterCapacity fetches the allocated resources to the framework
func (h *serviceHandler) ClusterCapacity(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *hostsvc.ClusterCapacityRequest) (
	*hostsvc.ClusterCapacityResponse, yarpc.ResMeta, error) {

	log.WithField("request", body).Debug("ClusterCapacity called.")

	// Get frameworkID
	frameWorkID := h.frameworkInfoProvider.GetFrameworkID()

	// Validate FrameworkID
	if len(frameWorkID.GetValue()) == 0 {
		return &hostsvc.ClusterCapacityResponse{
			Error: &hostsvc.ClusterCapacityResponse_Error{
				ClusterUnavailable: &hostsvc.ClusterUnavailable{
					Message: "unable to fetch framework ID",
				},
			},
		}, nil, nil
	}

	// Fetch allocated resources
	allocatedResources, err := h.mOperatorClient.AllocatedResources(frameWorkID.GetValue())

	if err != nil {
		h.metrics.ClusterCapacityFail.Inc(1)
		log.WithError(err).Error("error making cluster capacity request")
		return &hostsvc.ClusterCapacityResponse{
			Error: &hostsvc.ClusterCapacityResponse_Error{
				ClusterUnavailable: &hostsvc.ClusterUnavailable{
					Message: err.Error(),
				},
			},
		}, nil, nil
	}

	// Get scalar resource from Mesos resources
	tAllocatedResources := scalar.FromMesosResources(allocatedResources)

	h.metrics.ClusterCapacity.Inc(1)
	return &hostsvc.ClusterCapacityResponse{
		Resources: []*hostsvc.Resource{
			{
				Kind:     common.CPU,
				Capacity: tAllocatedResources.CPU,
			}, {
				Kind:     common.DISK,
				Capacity: tAllocatedResources.Disk,
			}, {
				Kind:     common.GPU,
				Capacity: tAllocatedResources.GPU,
			}, {
				Kind:     common.MEMORY,
				Capacity: tAllocatedResources.Mem,
			},
		},
	}, nil, nil
}
