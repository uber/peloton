package hostmgr

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	sched "code.uber.internal/infra/peloton/.gen/mesos/v1/scheduler"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/volume"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/constraints"
	"code.uber.internal/infra/peloton/common/queue"
	"code.uber.internal/infra/peloton/common/reservation"
	"code.uber.internal/infra/peloton/hostmgr/config"
	"code.uber.internal/infra/peloton/hostmgr/factory/operation"
	"code.uber.internal/infra/peloton/hostmgr/factory/task"
	"code.uber.internal/infra/peloton/hostmgr/host"
	hostmgr_mesos "code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/hostmgr/metrics"
	"code.uber.internal/infra/peloton/hostmgr/offer"
	"code.uber.internal/infra/peloton/hostmgr/offer/offerpool"
	mqueue "code.uber.internal/infra/peloton/hostmgr/queue"
	"code.uber.internal/infra/peloton/hostmgr/reserver"
	"code.uber.internal/infra/peloton/hostmgr/scalar"
	"code.uber.internal/infra/peloton/hostmgr/summary"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
)

const (
	// This is the number of completed reservations which
	// will be fetched in one call from the reserver.
	_completedReservationLimit = 10
)

// serviceHandler implements peloton.private.hostmgr.InternalHostService.
type serviceHandler struct {
	schedulerClient       mpb.SchedulerClient
	operatorMasterClient  mpb.MasterOperatorClient
	metrics               *metrics.Metrics
	offerPool             offerpool.Pool
	frameworkInfoProvider hostmgr_mesos.FrameworkInfoProvider
	volumeStore           storage.PersistentVolumeStore
	roleName              string
	mesosDetector         hostmgr_mesos.MasterDetector
	reserver              reserver.Reserver
	hmConfig              config.Config
	maintenanceQueue      mqueue.MaintenanceQueue // queue containing the machineIDs of the machines to be put into maintenance
}

// InitServiceHandler initialize serviceHandler.
func InitServiceHandler(
	d *yarpc.Dispatcher,
	parent tally.Scope,
	schedulerClient mpb.SchedulerClient,
	masterOperatorClient mpb.MasterOperatorClient,
	frameworkInfoProvider hostmgr_mesos.FrameworkInfoProvider,
	volumeStore storage.PersistentVolumeStore,
	mesosConfig hostmgr_mesos.Config,
	mesosDetector hostmgr_mesos.MasterDetector,
	hmConfig *config.Config,
	maintenanceQueue mqueue.MaintenanceQueue) {

	handler := &serviceHandler{
		schedulerClient:       schedulerClient,
		operatorMasterClient:  masterOperatorClient,
		metrics:               metrics.NewMetrics(parent),
		offerPool:             offer.GetEventHandler().GetOfferPool(),
		frameworkInfoProvider: frameworkInfoProvider,
		volumeStore:           volumeStore,
		roleName:              mesosConfig.Framework.Role,
		mesosDetector:         mesosDetector,
		maintenanceQueue:      maintenanceQueue,
	}
	// Creating Reserver object for handler
	handler.reserver = reserver.NewReserver(
		handler.metrics,
		hmConfig,
		handler.offerPool)
	d.Register(hostsvc.BuildInternalHostServiceYARPCProcedures(handler))
}

// validateHostFilter validates the host filter passed to
// AcquireHostOffers or GetHosts request.
func validateHostFilter(
	filter *hostsvc.HostFilter) *hostsvc.InvalidHostFilter {
	if filter == nil {
		return &hostsvc.InvalidHostFilter{
			Message: "Empty host filter",
		}
	}
	return nil
}

// GetOutstandingOffers returns all the offers present in offer pool.
func (h *serviceHandler) GetOutstandingOffers(
	ctx context.Context,
	body *hostsvc.GetOutstandingOffersRequest) (*hostsvc.GetOutstandingOffersResponse, error) {

	hostOffers, count := h.offerPool.GetOffers(summary.All)
	if count == 0 {
		return &hostsvc.GetOutstandingOffersResponse{
			Error: &hostsvc.GetOutstandingOffersResponse_Error{
				NoOffers: &hostsvc.NoOffersError{
					Message: "no offers present in offer pool",
				},
			},
		}, nil
	}

	outstandingOffers := make([]*mesos.Offer, 0, count)

	for _, hostOffer := range hostOffers {
		for _, offer := range hostOffer {
			outstandingOffers = append(outstandingOffers, offer)
		}
	}

	return &hostsvc.GetOutstandingOffersResponse{
		Offers: outstandingOffers,
	}, nil
}

// AcquireHostOffers implements InternalHostService.AcquireHostOffers.
func (h *serviceHandler) AcquireHostOffers(
	ctx context.Context,
	body *hostsvc.AcquireHostOffersRequest) (*hostsvc.AcquireHostOffersResponse, error) {

	log.WithField("request", body).Debug("AcquireHostOffers called.")

	if invalid := validateHostFilter(body.GetFilter()); invalid != nil {
		log.WithField("filter", body.GetFilter()).Warn("Invalid Filter")
		h.metrics.AcquireHostOffersInvalid.Inc(1)
		return &hostsvc.AcquireHostOffersResponse{
			Error: &hostsvc.AcquireHostOffersResponse_Error{
				InvalidHostFilter: invalid,
			},
		}, nil
	}

	hostOffers, resultCount, err := h.offerPool.ClaimForPlace(body.GetFilter())
	if err != nil {
		log.WithError(err).Warn("ClaimForPlace failed")
		return &hostsvc.AcquireHostOffersResponse{
			Error: &hostsvc.AcquireHostOffersResponse_Error{
				Failure: &hostsvc.AcquireHostOffersFailure{
					Message: err.Error(),
				},
			},
		}, nil
	}

	response := hostsvc.AcquireHostOffersResponse{
		HostOffers:         []*hostsvc.HostOffer{},
		FilterResultCounts: resultCount,
	}

	for hostname, offers := range hostOffers {
		if len(offers) <= 0 {
			log.WithField("host", hostname).
				Warn("Empty offer slice from host")
			continue
		}

		var resources []*mesos.Resource
		var attributes []*mesos.Attribute
		for _, offer := range offers {
			resources = append(resources, offer.GetResources()...)
			attributes = append(attributes, offer.GetAttributes()...)
		}

		hostOffer := hostsvc.HostOffer{
			Hostname:   hostname,
			AgentId:    offers[0].GetAgentId(),
			Attributes: attributes,
			Resources:  resources,
		}

		response.HostOffers = append(response.HostOffers, &hostOffer)
	}

	h.metrics.AcquireHostOffers.Inc(1)
	h.metrics.AcquireHostOffersCount.Inc(int64(len(response.HostOffers)))

	log.WithField("response", response).Debug("AcquireHostOffers returned")
	return &response, nil
}

// GetHosts implements InternalHostService.GetHosts.
// This function gets the hosts based on resource requirements
// and constraints passed in the request through hostsvc.HostFilter
func (h *serviceHandler) GetHosts(
	ctx context.Context,
	body *hostsvc.GetHostsRequest) (*hostsvc.GetHostsResponse, error) {
	log.WithField("request", body).Debug("GetHosts called.")

	// validating the Host Filter, in case of invalid filter
	// return InvalidHostFilter error
	if invalid := validateHostFilter(body.GetFilter()); invalid != nil {
		return h.processGetHostsFailure(invalid), nil
	}

	// Creating the Matcher instance for particular HostFilter and Evaluator
	matcher := host.NewMatcher(body.GetFilter(),
		constraints.NewEvaluator(pb_task.LabelConstraint_HOST))

	// Calling the GetMatchingHosts to get the matched hosts for the matcher
	result, err := matcher.GetMatchingHosts()
	if err != nil {
		return h.processGetHostsFailure(err), nil
	}

	hosts := make([]*hostsvc.HostInfo, 0, len(result))

	// Filling agentInfo from result to GetHostsResponse
	for hostname, agentInfo := range result {
		hosts = append(hosts, util.CreateHostInfo(hostname, agentInfo))
	}

	//Updating metrics for GetHosts
	h.metrics.GetHosts.Inc(1)
	h.metrics.GetHostsCount.Inc(int64(len(hosts)))

	log.WithField("hosts", hosts).Debug("GetHosts returned")
	return &hostsvc.GetHostsResponse{
		Hosts: hosts,
	}, nil
}

// processGetHostsFailure process the GetHostsFailure and returns the
// error in respose otherwise with empty error
func (h *serviceHandler) processGetHostsFailure(err interface{}) *hostsvc.GetHostsResponse {
	resp := &hostsvc.GetHostsResponse{
		Error: &hostsvc.GetHostsResponse_Error{},
	}
	h.metrics.GetHostsInvalid.Inc(1)
	if filter, ok := err.(*hostsvc.InvalidHostFilter); ok {
		log.WithField("error", filter.Message).Warn("no matching hosts")
		resp.Error.InvalidHostFilter = filter
	}
	if hostFailure, ok := err.(*hostsvc.GetHostsFailure); ok {
		log.WithField("error", hostFailure.Message).Warn("no matching hosts")
		resp.Error.Failure = hostFailure
	}
	return resp
}

// ReleaseHostOffers implements InternalHostService.ReleaseHostOffers.
func (h *serviceHandler) ReleaseHostOffers(
	ctx context.Context,
	body *hostsvc.ReleaseHostOffersRequest) (
	*hostsvc.ReleaseHostOffersResponse, error) {

	log.WithField("request", body).Debug("ReleaseHostOffers called.")
	response := hostsvc.ReleaseHostOffersResponse{}

	for _, hostOffer := range body.GetHostOffers() {
		hostname := hostOffer.GetHostname()
		if err := h.offerPool.ReturnUnusedOffers(hostname); err != nil {
			log.WithError(err).WithField("hostoffer", hostOffer).
				Warn("Cannot return unused offer on host.")
		}
	}

	h.metrics.ReleaseHostOffers.Inc(1)
	h.metrics.ReleaseHostsCount.Inc(int64(len(body.GetHostOffers())))

	return &response, nil
}

var (
	errEmptyOfferOperations              = errors.New("empty operations in OfferOperationsRequest")
	errLaunchOperationIsNotLastOperation = errors.New("launch operation is not the last operation")
	errLaunchOperationWithEmptyTasks     = errors.New("launch operation with empty task list")
	errOfferOperationNotSupported        = errors.New("offer operation not supported")
	errInvalidOfferOperation             = errors.New("invalid offer operation")
	errHostnameMissing                   = errors.New("hostname is required")
	errReservationNotFound               = errors.New("reservation could not be made")
)

// validateOfferOperation ensures offer operations sequences are valid.
func validateOfferOperationsRequest(
	request *hostsvc.OfferOperationsRequest) error {

	operations := request.GetOperations()
	if len(operations) == 0 {
		return errEmptyOfferOperations
	}

	for index, op := range operations {
		if op.GetType() == hostsvc.OfferOperation_LAUNCH {
			if index != len(operations)-1 {
				return errLaunchOperationIsNotLastOperation
			} else if len(op.GetLaunch().GetTasks()) == 0 {
				return errLaunchOperationWithEmptyTasks
			}
		} else if op.GetType() != hostsvc.OfferOperation_CREATE &&
			op.GetType() != hostsvc.OfferOperation_RESERVE {
			return errOfferOperationNotSupported
		} else {
			// Reservation label must be specified for RESERVE/CREATE operation.
			if op.GetReservationLabels() == nil {
				return errInvalidOfferOperation
			}
		}
	}

	if len(request.GetHostname()) == 0 {
		return errHostnameMissing
	}

	return nil
}

// extractReserveationLabels checks if operations on reserved offers, if yes,
// returns reservation labels, otherwise nil.
func (h *serviceHandler) extractReserveationLabels(req *hostsvc.OfferOperationsRequest) *mesos.Labels {
	reqOps := req.GetOperations()
	if len(reqOps) == 1 &&
		reqOps[0].GetType() == hostsvc.OfferOperation_LAUNCH {
		return reqOps[0].GetReservationLabels()
	}

	if len(reqOps) == 2 &&
		reqOps[0].GetType() == hostsvc.OfferOperation_CREATE &&
		reqOps[1].GetType() == hostsvc.OfferOperation_LAUNCH {
		return reqOps[0].GetReservationLabels()
	}

	return nil
}

// OfferOperations implements InternalHostService.OfferOperations.
func (h *serviceHandler) OfferOperations(
	ctx context.Context,
	req *hostsvc.OfferOperationsRequest) (
	*hostsvc.OfferOperationsResponse,
	error) {
	log.WithField("request", req).Debug("Offer operations called.")

	if err := validateOfferOperationsRequest(req); err != nil {
		h.metrics.OfferOperationsInvalid.Inc(1)
		return &hostsvc.OfferOperationsResponse{
			Error: &hostsvc.OfferOperationsResponse_Error{
				InvalidArgument: &hostsvc.InvalidArgument{
					Message: err.Error(),
				},
			},
		}, nil
	}

	reservedOfferLabels := h.extractReserveationLabels(req)
	offers, err := h.offerPool.ClaimForLaunch(
		req.GetHostname(),
		reservedOfferLabels != nil, /* useReservedOffers */
	)
	if err != nil {
		log.WithError(err).
			WithField("request", req).
			WithField("offers", offers).
			Error("claim offer for operations failed.")
		h.metrics.OfferOperationsInvalidOffers.Inc(1)
		return &hostsvc.OfferOperationsResponse{
			Error: &hostsvc.OfferOperationsResponse_Error{
				InvalidOffers: &hostsvc.InvalidOffers{
					Message: err.Error(),
				},
			},
		}, nil
	}

	// TODO: Use `offers` so we can support reservation, port picking, etc.
	log.WithField("offers", offers).Debug("Offers found for launch")

	var offerIds []*mesos.OfferID
	var mesosResources []*mesos.Resource
	var agentID *mesos.AgentID
	for _, offer := range offers {
		offerIds = append(offerIds, offer.GetId())
		agentID = offer.GetAgentId()
		for _, res := range offer.GetResources() {
			if reservedOfferLabels == nil ||
				reservedOfferLabels.String() == res.GetReservation().GetLabels().String() {
				mesosResources = append(mesosResources, res)
			}
		}
	}

	factory := operation.NewOfferOperationsFactory(
		req.GetOperations(),
		mesosResources,
		req.GetHostname(),
		agentID,
	)
	offerOperations, err := factory.GetOfferOperations()
	if err == nil {
		// write the volume info into db if no error.
		err = h.persistVolumeInfo(ctx, offerOperations, req.GetHostname())
	}
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"request": req,
			"offers":  offers,
		}).Error("get offer operations failed.")
		// For now, decline all offers to Mesos in the hope that next
		// call to pool will select some different host.
		// An alternative is to mark offers on the host as ready.
		if reservedOfferLabels == nil {
			if err := h.offerPool.DeclineOffers(ctx, offerIds); err != nil {
				log.WithError(err).
					WithField("offers", offerIds).
					Warn("Cannot decline offers task building error")
			}
		}

		h.metrics.OfferOperationsInvalid.Inc(1)
		return &hostsvc.OfferOperationsResponse{
			Error: &hostsvc.OfferOperationsResponse_Error{
				InvalidArgument: &hostsvc.InvalidArgument{
					Message: "Cannot get offer operations: " + err.Error(),
				},
			},
		}, nil
	}

	callType := sched.Call_ACCEPT
	msg := &sched.Call{
		FrameworkId: h.frameworkInfoProvider.GetFrameworkID(ctx),
		Type:        &callType,
		Accept: &sched.Call_Accept{
			OfferIds:   offerIds,
			Operations: offerOperations,
		},
	}

	log.WithFields(log.Fields{
		"call": msg,
	}).Debug("Accepting offer with operations.")

	// TODO: add retry / put back offer and tasks in failure scenarios
	msid := h.frameworkInfoProvider.GetMesosStreamID(ctx)
	err = h.schedulerClient.Call(msid, msg)
	if err != nil {
		h.metrics.OfferOperationsFail.Inc(1)
		log.WithError(err).WithFields(log.Fields{
			"operations": offerOperations,
			"offers":     offerIds,
			"error":      err,
		}).Warn("Offer operations failure")

		return &hostsvc.OfferOperationsResponse{
			Error: &hostsvc.OfferOperationsResponse_Error{
				Failure: &hostsvc.OperationsFailure{
					Message: err.Error(),
				},
			},
		}, nil
	}

	h.metrics.OfferOperations.Inc(1)
	return &hostsvc.OfferOperationsResponse{}, nil
}

// persistVolumeInfo write volume information into db.
func (h *serviceHandler) persistVolumeInfo(
	ctx context.Context,
	offerOperations []*mesos.Offer_Operation,
	hostname string) error {
	createOperation := operation.GetOfferCreateOperation(offerOperations)
	// Skip creating volume if no create operation.
	if createOperation == nil {
		return nil
	}

	volumeRes := createOperation.GetCreate().GetVolumes()[0]
	volumeID := &peloton.VolumeID{
		Value: volumeRes.GetDisk().GetPersistence().GetId(),
	}

	pv, err := h.volumeStore.GetPersistentVolume(ctx, volumeID)
	if err != nil {
		_, ok := err.(*storage.VolumeNotFoundError)
		if !ok {
			// volume store db read error.
			return err
		}
	}

	if pv != nil {
		switch pv.GetState() {
		case volume.VolumeState_CREATED, volume.VolumeState_DELETED:
			log.WithFields(log.Fields{
				"volume":           pv,
				"offer_operations": offerOperations,
				"hostname":         hostname,
			}).Error("try create to create volume that already exists")
		}
		// TODO(mu): Volume info already exist in db and check if we need to update hostname
		return nil
	}

	jobID, instanceID, err := reservation.ParseReservationLabels(
		volumeRes.GetReservation().GetLabels())
	if err != nil {
		return err
	}

	volumeInfo := &volume.PersistentVolumeInfo{
		Id: volumeID,
		JobId: &peloton.JobID{
			Value: jobID,
		},
		InstanceId:    instanceID,
		Hostname:      hostname,
		State:         volume.VolumeState_INITIALIZED,
		GoalState:     volume.VolumeState_CREATED,
		SizeMB:        uint32(volumeRes.GetScalar().GetValue()),
		ContainerPath: volumeRes.GetDisk().GetVolume().GetContainerPath(),
	}

	err = h.volumeStore.CreatePersistentVolume(ctx, volumeInfo)
	return err
}

// LaunchTasks implements InternalHostService.LaunchTasks.
func (h *serviceHandler) LaunchTasks(
	ctx context.Context,
	body *hostsvc.LaunchTasksRequest) (
	*hostsvc.LaunchTasksResponse,
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
		}, nil
	}

	offers, err := h.offerPool.ClaimForLaunch(body.GetHostname(), false)
	if err != nil {
		h.metrics.LaunchTasksInvalidOffers.Inc(1)
		return &hostsvc.LaunchTasksResponse{
			Error: &hostsvc.LaunchTasksResponse_Error{
				InvalidOffers: &hostsvc.InvalidOffers{
					Message: err.Error(),
				},
			},
		}, nil
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

	builder := task.NewBuilder(mesosResources)

	for _, t := range body.GetTasks() {
		mesosTask, err := builder.Build(
			t.GetTaskId(), t.GetConfig(), t.GetPorts(), nil, nil)
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"task_id":             t.TaskId,
				"tasks_total":         len(body.GetTasks()),
				"tasks":               body.GetTasks(),
				"matched_tasks":       mesosTasks,
				"matched_task_ids":    mesosTaskIds,
				"matched_tasks_total": len(mesosTasks),
				"offers":              offers,
				"hostname":            body.GetHostname(),
			}).Warn("Fail to get correct Mesos TaskInfo")
			h.metrics.LaunchTasksInvalid.Inc(1)

			// For now, decline all offers to Mesos in the hope that next
			// call to pool will select some different host.
			// An alternative is to mark offers on the host as ready.
			if derr := h.offerPool.DeclineOffers(ctx, offerIds); derr != nil {
				log.WithError(err).WithFields(log.Fields{
					"offers":   offerIds,
					"hostname": body.GetHostname(),
				}).Warn("Cannot decline offers task building error")
			}

			if err == task.ErrNotEnoughResource {
				return &hostsvc.LaunchTasksResponse{
					Error: &hostsvc.LaunchTasksResponse_Error{
						InvalidOffers: &hostsvc.InvalidOffers{
							Message: "not enough resource to run task: " + err.Error(),
						},
					},
				}, nil
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
			}, nil
		}

		mesosTask.AgentId = body.GetAgentId()
		mesosTasks = append(mesosTasks, mesosTask)
		mesosTaskIds = append(mesosTaskIds, *mesosTask.TaskId.Value)
	}

	callType := sched.Call_ACCEPT
	opType := mesos.Offer_Operation_LAUNCH
	msg := &sched.Call{
		FrameworkId: h.frameworkInfoProvider.GetFrameworkID(ctx),
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
	msid := h.frameworkInfoProvider.GetMesosStreamID(ctx)
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
		}, nil
	}

	h.metrics.LaunchTasks.Inc(int64(len(mesosTasks)))
	log.WithFields(log.Fields{
		"tasks":  len(mesosTasks),
		"offers": len(offerIds),
	}).Debug("Tasks launched.")

	return &hostsvc.LaunchTasksResponse{}, nil
}

func validateLaunchTasks(request *hostsvc.LaunchTasksRequest) error {
	if len(request.Tasks) <= 0 {
		return errors.New("empty task list in LaunchTasksRequest")
	}

	if len(request.GetAgentId().GetValue()) <= 0 {
		return errors.New("empty agent id in LaunchTasksRequest")
	}

	if len(request.Hostname) <= 0 {
		return errors.New("empty hostname in LaunchTasksRequest")
	}

	return nil
}

func validateShutdownExecutors(request *hostsvc.ShutdownExecutorsRequest) error {
	executorList := request.GetExecutors()

	if len(executorList) <= 0 {
		return errors.New("empty executor list in ShutdownExecutorsRequest")
	}

	for _, executor := range executorList {
		if executor.GetAgentId() == nil || executor.GetExecutorId() == nil {
			return errors.New("empty Executor Id or Agent Id")
		}
	}
	return nil
}

// ShutdownExecutors implements InternalHostService.ShutdownExecutors.
func (h *serviceHandler) ShutdownExecutors(
	ctx context.Context,
	body *hostsvc.ShutdownExecutorsRequest) (
	*hostsvc.ShutdownExecutorsResponse, error) {
	log.WithField("request", body).Debug("ShutdownExecutor called.")
	shutdownExecutors := body.GetExecutors()

	if err := validateShutdownExecutors(body); err != nil {
		h.metrics.ShutdownExecutorsInvalid.Inc(1)
		return &hostsvc.ShutdownExecutorsResponse{
			Error: &hostsvc.ShutdownExecutorsResponse_Error{
				InvalidExecutors: &hostsvc.InvalidExecutors{
					Message: err.Error(),
				},
			},
		}, nil
	}

	var wg sync.WaitGroup
	failedMutex := &sync.Mutex{}
	var failedExecutors []*hostsvc.ExecutorOnAgent
	var errs []string
	for _, shutdownExecutor := range shutdownExecutors {
		wg.Add(1)
		go func(shutdownExecutor *hostsvc.ExecutorOnAgent) {
			defer wg.Done()
			executorID := shutdownExecutor.GetExecutorId()
			agentID := shutdownExecutor.GetAgentId()

			callType := sched.Call_SHUTDOWN
			msg := &sched.Call{
				FrameworkId: h.frameworkInfoProvider.GetFrameworkID(ctx),
				Type:        &callType,
				Shutdown: &sched.Call_Shutdown{
					ExecutorId: executorID,
					AgentId:    agentID,
				},
			}
			msid := h.frameworkInfoProvider.GetMesosStreamID(ctx)
			err := h.schedulerClient.Call(msid, msg)
			if err != nil {
				h.metrics.ShutdownExecutorsFail.Inc(1)
				log.WithFields(log.Fields{
					"executor_id": executorID,
					"agent_id":    agentID,
					"error":       err,
				}).Error("Shutdown executor failure")

				failedMutex.Lock()
				defer failedMutex.Unlock()
				failedExecutors = append(
					failedExecutors, shutdownExecutor)
				errs = append(errs, err.Error())
				return
			}
			h.metrics.ShutdownExecutors.Inc(1)
		}(shutdownExecutor)
	}
	wg.Wait()
	if len(failedExecutors) > 0 {
		return &hostsvc.ShutdownExecutorsResponse{
			Error: &hostsvc.ShutdownExecutorsResponse_Error{
				ShutdownFailure: &hostsvc.ShutdownFailure{
					Message:   strings.Join(errs, ";"),
					Executors: failedExecutors,
				},
			},
		}, nil
	}

	return &hostsvc.ShutdownExecutorsResponse{}, nil
}

// KillTasks implements InternalHostService.KillTasks.
func (h *serviceHandler) KillTasks(
	ctx context.Context,
	body *hostsvc.KillTasksRequest) (
	*hostsvc.KillTasksResponse, error) {

	log.WithField("request", body).Debug("KillTasks called.")
	taskIds := body.GetTaskIds()
	if len(taskIds) == 0 {
		return &hostsvc.KillTasksResponse{
			Error: &hostsvc.KillTasksResponse_Error{
				InvalidTaskIDs: &hostsvc.InvalidTaskIDs{
					Message: "Empty task ids",
				},
			},
		}, nil
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
				FrameworkId: h.frameworkInfoProvider.GetFrameworkID(ctx),
				Type:        &callType,
				Kill: &sched.Call_Kill{
					TaskId: taskID,
				},
			}

			msid := h.frameworkInfoProvider.GetMesosStreamID(ctx)
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
		}, nil
	}

	return &hostsvc.KillTasksResponse{}, nil
}

// ReserveResources implements InternalHostService.ReserveResources.
func (h *serviceHandler) ReserveResources(
	ctx context.Context,
	body *hostsvc.ReserveResourcesRequest) (
	*hostsvc.ReserveResourcesResponse, error) {

	log.Debug("ReserveResources called.")
	return nil, fmt.Errorf("Unimplemented")
}

// UnreserveResources implements InternalHostService.UnreserveResources.
func (h *serviceHandler) UnreserveResources(
	ctx context.Context,
	body *hostsvc.UnreserveResourcesRequest) (
	*hostsvc.UnreserveResourcesResponse, error) {

	log.Debug("UnreserveResources called.")
	return nil, fmt.Errorf("Unimplemented")
}

// CreateVolumes implements InternalHostService.CreateVolumes.
func (h *serviceHandler) CreateVolumes(
	ctx context.Context,
	body *hostsvc.CreateVolumesRequest) (
	*hostsvc.CreateVolumesResponse, error) {

	log.Debug("CreateVolumes called.")
	return nil, fmt.Errorf("Unimplemented")
}

// DestroyVolumes implements InternalHostService.DestroyVolumes.
func (h *serviceHandler) DestroyVolumes(
	ctx context.Context,
	body *hostsvc.DestroyVolumesRequest) (
	*hostsvc.DestroyVolumesResponse, error) {

	log.Debug("DestroyVolumes called.")
	return nil, fmt.Errorf("Unimplemented")
}

// ClusterCapacity fetches the allocated resources to the framework
func (h *serviceHandler) ClusterCapacity(
	ctx context.Context,
	body *hostsvc.ClusterCapacityRequest) (
	*hostsvc.ClusterCapacityResponse, error) {

	log.WithField("request", body).Debug("ClusterCapacity called.")

	// Get frameworkID
	frameWorkID := h.frameworkInfoProvider.GetFrameworkID(ctx)

	// Validate FrameworkID
	if len(frameWorkID.GetValue()) == 0 {
		return &hostsvc.ClusterCapacityResponse{
			Error: &hostsvc.ClusterCapacityResponse_Error{
				ClusterUnavailable: &hostsvc.ClusterUnavailable{
					Message: "unable to fetch framework ID",
				},
			},
		}, nil
	}

	// Fetch allocated resources
	allocatedResources, err := h.operatorMasterClient.AllocatedResources(frameWorkID.GetValue())

	if err != nil {
		h.metrics.ClusterCapacityFail.Inc(1)
		log.WithError(err).Error("error making cluster capacity request")
		return &hostsvc.ClusterCapacityResponse{
			Error: &hostsvc.ClusterCapacityResponse_Error{
				ClusterUnavailable: &hostsvc.ClusterUnavailable{
					Message: err.Error(),
				},
			},
		}, nil
	}

	// Get scalar resource from Mesos resources
	tAllocatedResources := scalar.FromMesosResources(allocatedResources)

	agentMap := host.GetAgentMap()
	if agentMap == nil || len(agentMap.RegisteredAgents) == 0 {
		h.metrics.ClusterCapacityFail.Inc(1)
		log.Error("error getting host agentmap")
		return &hostsvc.ClusterCapacityResponse{
			Error: &hostsvc.ClusterCapacityResponse_Error{
				ClusterUnavailable: &hostsvc.ClusterUnavailable{
					Message: "error getting host agentmap",
				},
			},
		}, nil
	}

	// NOTE: This only works if
	// 1) no quota is set for any role, or
	// 2) quota is set for the same role peloton is registered under.
	// If operator set a quota for another role but leave peloton's role unset,
	// cluster capacity will be over estimated.
	var res scalar.Resources
	quotaResources, err := h.operatorMasterClient.GetQuota(h.roleName)
	if err != nil {
		h.metrics.ClusterCapacityFail.Inc(1)
		log.WithError(err).Error("error getting quota")
		return &hostsvc.ClusterCapacityResponse{
			Error: &hostsvc.ClusterCapacityResponse_Error{
				ClusterUnavailable: &hostsvc.ClusterUnavailable{
					Message: err.Error(),
				},
			},
		}, nil
	}

	if err == nil && quotaResources != nil {
		res = scalar.FromMesosResources(quotaResources)
		if res.GetCPU() <= 0 {
			res.CPU = agentMap.Capacity.GetCPU()
		}
		if res.GetMem() <= 0 {
			res.Mem = agentMap.Capacity.GetMem()
		}
		if res.GetDisk() <= 0 {
			res.Disk = agentMap.Capacity.GetDisk()
		}
		if res.GetGPU() <= 0 {
			res.GPU = agentMap.Capacity.GetGPU()
		}
	} else {
		res = agentMap.Capacity
	}
	h.metrics.ClusterCapacity.Inc(1)

	clusterCapacityResponse := &hostsvc.ClusterCapacityResponse{
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
		PhysicalResources: []*hostsvc.Resource{
			{
				Kind:     common.CPU,
				Capacity: res.CPU,
			}, {
				Kind:     common.DISK,
				Capacity: res.Disk,
			}, {
				Kind:     common.GPU,
				Capacity: res.GPU,
			}, {
				Kind:     common.MEMORY,
				Capacity: res.Mem,
			},
		},
	}

	h.metrics.RefreshClusterCapacityGauges(clusterCapacityResponse)
	return clusterCapacityResponse, nil
}

// GetMesosMasterHostPort returns the Leader Mesos Master hostname and port.
func (h *serviceHandler) GetMesosMasterHostPort(
	ctx context.Context,
	body *hostsvc.MesosMasterHostPortRequest) (*hostsvc.MesosMasterHostPortResponse, error) {

	mesosMasterInfo := strings.Split(h.mesosDetector.HostPort(), ":")
	if len(mesosMasterInfo) != 2 {
		return nil, errors.New("unable to fetch leader mesos master hostname & port")
	}
	mesosMasterHostPortResponse := &hostsvc.MesosMasterHostPortResponse{
		Hostname: mesosMasterInfo[0],
		Port:     mesosMasterInfo[1],
	}

	return mesosMasterHostPortResponse, nil
}

/*
* ReserveHosts reserves the host for a specified task in the request.
* Host Manager will keep the host offers to itself till the time
* it does not have enough offers to itself and once that's fulfilled
* it will return the reservation with the offer to placement engine.
* till the time reservation is fulfilled or reservation timeout ,
* offers from that host will not be given to any other placement engine.
 */
func (h *serviceHandler) ReserveHosts(
	ctx context.Context,
	req *hostsvc.ReserveHostsRequest,
) (*hostsvc.ReserveHostsResponse, error) {

	log.WithField("request", req).Debug("ReserveHosts called.")
	if req.GetReservation() == nil {
		return &hostsvc.ReserveHostsResponse{
			Error: &hostsvc.ReserveHostsResponse_Error{
				Failed: &hostsvc.ReservationFailed{
					Message: "reservation is nil",
				},
			},
		}, nil
	}
	err := h.reserver.EnqueueReservation(ctx, req.Reservation)
	if err != nil {
		return &hostsvc.ReserveHostsResponse{
			Error: &hostsvc.ReserveHostsResponse_Error{
				Failed: &hostsvc.ReservationFailed{
					Message: errReservationNotFound.Error(),
				},
			},
		}, nil
	}
	log.Debug("ReserveHosts returned.")
	return &hostsvc.ReserveHostsResponse{}, nil
}

/*
 * GetCompletedReservations gets the completed host reservations from
 * reserver. Based on the reserver it returns the list of completed
 * Reservations (hostsvc.CompletedReservation) or return the NoFound Error.
 */
func (h *serviceHandler) GetCompletedReservations(
	ctx context.Context,
	req *hostsvc.GetCompletedReservationRequest,
) (*hostsvc.GetCompletedReservationResponse, error) {
	log.WithField("request", req).Debug("GetCompletedReservations called.")
	completedReservations, err := h.reserver.DequeueCompletedReservation(ctx, _completedReservationLimit)
	if err != nil {
		return &hostsvc.GetCompletedReservationResponse{
			Error: &hostsvc.GetCompletedReservationResponse_Error{
				NotFound: &hostsvc.NotFound{
					Message: err.Error(),
				},
			},
		}, nil
	}
	log.Debug("GetCompletedReservations returned.")
	return &hostsvc.GetCompletedReservationResponse{
		CompletedReservations: completedReservations,
	}, nil
}

// GetDrainingHosts implements InternalHostService.GetDrainingHosts
func (h *serviceHandler) GetDrainingHosts(ctx context.Context, request *hostsvc.GetDrainingHostsRequest) (*hostsvc.GetDrainingHostsResponse, error) {
	timeout := time.Duration(request.GetTimeout())
	var hostnames []string
	limit := request.GetLimit()
	// If request.limit is not specified, set limit to length of maintenance queue
	if limit == 0 {
		limit = uint32(h.maintenanceQueue.Length())
	}
	for i := uint32(0); i < limit; i++ {
		hostname, err := h.maintenanceQueue.Dequeue(timeout * time.Millisecond)
		if err != nil {
			if _, isTimeout := err.(queue.DequeueTimeOutError); !isTimeout {
				// error is not due to timeout so we log the error
				log.WithError(err).
					Error("unable to dequeue task from maintenance queue")
				h.metrics.GetDrainingHostsFail.Inc(1)
				return nil, err
			}
			break
		}
		hostnames = append(hostnames, hostname)
		h.metrics.GetDrainingHosts.Inc(1)
	}
	log.WithField("hosts", hostnames).Debug("Maintenance Queue - Dequeued hosts")

	return &hostsvc.GetDrainingHostsResponse{
		Hostnames: hostnames,
	}, nil
}

// MarkHostDrained implements InternalHostService.MarkHostDrained
func (h *serviceHandler) MarkHostDrained(ctx context.Context, request *hostsvc.MarkHostDrainedRequest) (*hostsvc.MarkHostDrainedResponse, error) {
	response, err := h.operatorMasterClient.GetMaintenanceStatus()
	if err != nil {
		log.WithError(err).Error("Error getting maintenance status")
		h.metrics.MarkHostDrainedFail.Inc(1)
		return nil, err
	}
	status := response.GetStatus()
	isHostDraining := false
	var machineID *mesos.MachineID
	for _, clusterStatusDrainingMachine := range status.GetDrainingMachines() {
		if clusterStatusDrainingMachine.GetId().GetHostname() == request.GetHostname() {
			isHostDraining = true
			machineID = clusterStatusDrainingMachine.GetId()
			break
		}
	}

	if isHostDraining {
		// If the host is currently in 'DRAINING' state, then start maintenance
		// on the host by posting to /machine/down endpoint of the Mesos Master
		err = h.operatorMasterClient.StartMaintenance([]*mesos.MachineID{machineID})
		if err != nil {
			log.WithError(err).Error("failed to down host: " + request.GetHostname())
			h.metrics.MarkHostDrainedFail.Inc(1)
			return nil, err
		}
	} else {
		log.Info("Host " + request.GetHostname() + " not in draining state")
	}

	h.metrics.MarkHostDrained.Inc(1)
	return &hostsvc.MarkHostDrainedResponse{}, nil
}
