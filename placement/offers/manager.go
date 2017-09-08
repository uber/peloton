package offers

import (
	"context"
	"errors"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	"code.uber.internal/infra/peloton/placement/models"
	log "github.com/sirupsen/logrus"
)

// Manager will manage offers used by any placement strategy.
type Manager interface {
	// Acquire fetches a batch of offers from the host manager.
	Acquire(ctx context.Context, fetchTasks bool, taskType resmgr.TaskType, filter *hostsvc.HostFilter) (offers []*models.Offer, filterResults map[string]uint32, err error)

	// Release will return the given offers to the host manager.
	Release(ctx context.Context, offers []*models.Offer) (err error)
}

// NewManager will create a new offer manager.
func NewManager(hostManager hostsvc.InternalHostServiceYARPCClient, resourceManager resmgrsvc.ResourceManagerServiceYARPCClient) Manager {
	return &offerManager{
		hostManager:     hostManager,
		resourceManager: resourceManager,
	}
}

type offerManager struct {
	hostManager     hostsvc.InternalHostServiceYARPCClient
	resourceManager resmgrsvc.ResourceManagerServiceYARPCClient
}

func (manager *offerManager) fetchOffers(ctx context.Context, filter *hostsvc.HostFilter) (
	[]*hostsvc.HostOffer, map[string]uint32, error) {
	offersRequest := &hostsvc.AcquireHostOffersRequest{
		Filter: filter,
	}
	log.WithField("request", offersRequest).Debug("Calling AcquireHostOffers")
	offersResponse, err := manager.hostManager.AcquireHostOffers(ctx, offersRequest)
	if err != nil {
		log.WithField("error", err).Error("Acquire offers failed")
		return nil, nil, err
	}
	log.WithFields(log.Fields{
		"request":  offersRequest,
		"response": offersResponse,
	}).Debug("AcquireHostOffers returned")
	if respErr := offersResponse.GetError(); respErr != nil {
		log.WithField("error", respErr).Error("AcquireHostOffers error")
		// TODO: Differentiate known error types by metrics and logs.
		return nil, nil, errors.New(respErr.String())
	}
	log.WithField("tasks", offersResponse.HostOffers).Debug("Dequeued offers")
	return offersResponse.GetHostOffers(), offersResponse.GetFilterResultCounts(), nil
}

func (manager *offerManager) fetchTasks(ctx context.Context, hostOffers []*hostsvc.HostOffer,
	taskType resmgr.TaskType) (map[string]*resmgrsvc.TaskList, error) {
	// Extract the hostnames
	hostnames := make([]string, 0, len(hostOffers))
	for _, hostOffer := range hostOffers {
		hostnames = append(hostnames, hostOffer.Hostname)
	}

	// Get tasks running on all the offers
	tasksRequest := &resmgrsvc.GetTasksByHostsRequest{
		Type:      taskType,
		Hostnames: hostnames,
	}
	tasksResponse, err := manager.resourceManager.GetTasksByHosts(ctx, tasksRequest)
	if err != nil {
		log.WithField("error", err).Error("GetTasksByHosts call failed")
		return nil, err
	}
	return tasksResponse.HostTasksMap, nil
}

func (manager *offerManager) convertOffers(hostOffers []*hostsvc.HostOffer,
	tasks map[string]*resmgrsvc.TaskList) []*models.Offer {
	offers := make([]*models.Offer, 0, len(hostOffers))
	for _, hostOffer := range hostOffers {
		var taskList []*resmgr.Task
		if tasks != nil && tasks[hostOffer.Hostname] != nil {
			taskList = tasks[hostOffer.Hostname].Tasks
		}
		offers = append(offers, models.NewOffer(hostOffer, taskList, time.Now()))
	}
	return offers
}

func (manager *offerManager) Acquire(ctx context.Context, fetchTasks bool, taskType resmgr.TaskType, filter *hostsvc.HostFilter) (
	[]*models.Offer, map[string]uint32, error) {
	// Get offers
	hostOffers, filterResults, err := manager.fetchOffers(ctx, filter)
	if err != nil {
		return nil, nil, err
	}

	// Get tasks running on all the offers
	var hostTasksMap map[string]*resmgrsvc.TaskList
	if fetchTasks {
		hostTasksMap, err = manager.fetchTasks(ctx, hostOffers, taskType)
		if err != nil {
			return nil, nil, err
		}
	}

	// Create placement offers from the host offers
	offers := manager.convertOffers(hostOffers, hostTasksMap)
	return offers, filterResults, nil
}

func (manager *offerManager) Release(ctx context.Context, offers []*models.Offer) error {
	hostOffers := make([]*hostsvc.HostOffer, 0, len(offers))
	for _, offer := range offers {
		hostOffers = append(hostOffers, offer.Offer())
	}
	request := &hostsvc.ReleaseHostOffersRequest{
		HostOffers: hostOffers,
	}
	response, err := manager.hostManager.ReleaseHostOffers(ctx, request)
	if err != nil {
		log.WithField("error", err).Error("ReleaseHostOffers failed")
		return err
	}
	if respErr := response.GetError(); respErr != nil {
		log.WithField("error", respErr).Error("ReleaseHostOffers error")
		// TODO: Differentiate known error types by metrics and logs.
		return errors.New(respErr.String())
	}
	log.WithField("host_offers", offers).Debug("Returned unused host offers")
	return nil
}
