package offers

import (
	"context"
	"errors"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	"code.uber.internal/infra/peloton/placement/metrics"
	"code.uber.internal/infra/peloton/placement/models"
	log "github.com/sirupsen/logrus"
)

// Service will manage offers used by any placement strategy.
type Service interface {
	// Acquire fetches a batch of offers from the host service.
	Acquire(ctx context.Context, fetchTasks bool, taskType resmgr.TaskType, filter *hostsvc.HostFilter) (offers []*models.Host)

	// Release will return the given offers to the host service.
	Release(ctx context.Context, offers []*models.Host)
}

// NewService will create a new offer service.
func NewService(hostManager hostsvc.InternalHostServiceYARPCClient, resourceManager resmgrsvc.ResourceManagerServiceYARPCClient, metrics *metrics.Metrics) Service {
	return &service{
		hostManager:     hostManager,
		resourceManager: resourceManager,
		metrics:         metrics,
	}
}

const _timeout = 10 * time.Second

type service struct {
	hostManager     hostsvc.InternalHostServiceYARPCClient
	resourceManager resmgrsvc.ResourceManagerServiceYARPCClient
	metrics         *metrics.Metrics
}

func (s *service) fetchOffers(ctx context.Context, filter *hostsvc.HostFilter) (
	[]*hostsvc.HostOffer, map[string]uint32, error) {
	ctx, cancelFunc := context.WithTimeout(ctx, _timeout)
	defer cancelFunc()
	offersRequest := &hostsvc.AcquireHostOffersRequest{
		Filter: filter,
	}
	offersResponse, err := s.hostManager.AcquireHostOffers(ctx, offersRequest)
	if err != nil {
		return nil, nil, err
	}
	log.WithFields(log.Fields{
		"request":  offersRequest,
		"response": offersResponse,
	}).Debug("AcquireHostOffers returned")
	if respErr := offersResponse.GetError(); respErr != nil {
		return nil, nil, errors.New(respErr.String())
	}
	return offersResponse.GetHostOffers(), offersResponse.GetFilterResultCounts(), nil
}

func (s *service) fetchTasks(ctx context.Context, hostOffers []*hostsvc.HostOffer,
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
	tasksResponse, err := s.resourceManager.GetTasksByHosts(ctx, tasksRequest)
	if err != nil {
		return nil, err
	}
	return tasksResponse.HostTasksMap, nil
}

func (s *service) convertOffers(hostOffers []*hostsvc.HostOffer,
	tasks map[string]*resmgrsvc.TaskList, now time.Time) []*models.Host {
	offers := make([]*models.Host, 0, len(hostOffers))
	for _, hostOffer := range hostOffers {
		var taskList []*resmgr.Task
		if tasks != nil && tasks[hostOffer.Hostname] != nil {
			taskList = tasks[hostOffer.Hostname].Tasks
		}
		offers = append(offers, models.NewHost(hostOffer, taskList, now))
	}
	return offers
}

func (s *service) Acquire(ctx context.Context, fetchTasks bool, taskType resmgr.TaskType, filter *hostsvc.HostFilter) []*models.Host {
	// Get offers
	hostOffers, filterResults, err := s.fetchOffers(ctx, filter)
	if err != nil {
		log.WithFields(log.Fields{
			log.ErrorKey:     err,
			"hostOffers":     hostOffers,
			"filter_results": filterResults,
			"filter":         filter,
			"taskType":       taskType,
			"fetchTasks":     fetchTasks,
		}).Error("Failed to dequeue hosts")
		s.metrics.OfferGetFail.Inc(1)
		return nil
	}

	if len(hostOffers) == 0 {
		log.WithFields(log.Fields{
			"hostOffers":     hostOffers,
			"filter_results": filterResults,
			"filter":         filter,
			"taskType":       taskType,
			"fetchTasks":     fetchTasks,
		}).Warn("No hosts dequeued")
	}

	// Get tasks running on all the offers
	var hostTasksMap map[string]*resmgrsvc.TaskList
	if fetchTasks {
		hostTasksMap, err = s.fetchTasks(ctx, hostOffers, taskType)
		if err != nil {
			log.WithFields(log.Fields{
				log.ErrorKey:     err,
				"hostOffers":     hostOffers,
				"filter_results": filterResults,
				"filter":         filter,
				"taskType":       taskType,
				"fetchTasks":     fetchTasks,
			}).Error("Failed to fetch tasks on hosts")
			s.metrics.OfferGetFail.Inc(1)
			return nil
		}
	}

	log.WithFields(log.Fields{
		"hostOffers":     hostOffers,
		"filter_results": filterResults,
		"filter":         filter,
		"taskType":       taskType,
		"fetchTasks":     fetchTasks,
	}).Debug("Dequeue hosts")
	// Create placement offers from the host offers
	return s.convertOffers(hostOffers, hostTasksMap, time.Now())
}

func (s *service) Release(ctx context.Context, hosts []*models.Host) {
	if len(hosts) == 0 {
		return
	}
	hostOffers := make([]*hostsvc.HostOffer, 0, len(hosts))
	for _, offer := range hosts {
		hostOffers = append(hostOffers, offer.Offer())
	}
	// ToDo: buffer the hosts until we have a batch of a certain size and return that.
	request := &hostsvc.ReleaseHostOffersRequest{
		HostOffers: hostOffers,
	}
	ctx, cancelFunc := context.WithTimeout(ctx, _timeout)
	defer cancelFunc()
	response, err := s.hostManager.ReleaseHostOffers(ctx, request)
	log.WithFields(log.Fields{
		"request":  request,
		"response": response,
	}).Debug("ReleaseHostOffers returned")
	if err != nil {
		log.WithField("error", err).Error("ReleaseHostOffers failed")
		return
	}
	if respErr := response.GetError(); respErr != nil {
		log.WithField("error", respErr).Error("ReleaseHostOffers error")
		// TODO: Differentiate known error types by metrics and logs.
		return
	}
	log.WithField("hosts", hosts).Debug("Returned unused hosts")
}