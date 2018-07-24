package hosts

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

const (
	_failedToFetchTasksOnHosts = "failed to fetch tasks on hosts"
	_timeout                   = 10 * time.Second
)

var (
	errfailedToAcquireHosts = errors.New("failed to acquire hosts")
	errnotImplemented       = errors.New("Not Implemented")
)

// Service will manage hosts used to get hosts and reserve hosts.
type Service interface {
	// GetHosts fetches a batch of hosts from the host manager matching filter.
	GetHosts(ctx context.Context, task *resmgr.Task, filter *hostsvc.HostFilter) (hosts []*models.Host, err error)

	// ReserveHosts Makes reservation for the host in hostmanager.
	ReserveHost(ctx context.Context, host []*models.Host, task *resmgr.Task) (err error)

	// GetCompletedReservation gets the completed reservation from host manager
	GetCompletedReservation(ctx context.Context) ([]*hostsvc.CompletedReservation, error)
}

// service is implementing Service interface
type service struct {
	// hostmanager yarpc client
	hostManager hostsvc.InternalHostServiceYARPCClient
	// resource manager yarpc client
	resourceManager resmgrsvc.ResourceManagerServiceYARPCClient
	// placement engine metrics object
	metrics *metrics.Metrics
}

// NewService will create a new host service.
func NewService(
	hostManager hostsvc.InternalHostServiceYARPCClient,
	resourceManager resmgrsvc.ResourceManagerServiceYARPCClient,
	metrics *metrics.Metrics) Service {
	return &service{
		hostManager:     hostManager,
		resourceManager: resourceManager,
		metrics:         metrics,
	}
}

// GetHosts fetches a batch of hosts from the host manager
// based on the host filter provided
func (s *service) GetHosts(
	ctx context.Context,
	task *resmgr.Task,
	filter *hostsvc.HostFilter) (hosts []*models.Host, err error) {
	req := &hostsvc.GetHostsRequest{
		Filter: filter,
	}
	res, err := s.hostManager.GetHosts(ctx, req)
	if err != nil {
		s.metrics.HostGetFail.Inc(1)
		return nil, errfailedToAcquireHosts
	}
	if respErr := res.GetError(); respErr != nil {
		s.metrics.HostGetFail.Inc(1)
		return nil, errors.New(respErr.String())
	}

	// Get tasks running on hosts
	var hostTasksMap map[string]*resmgrsvc.TaskList
	if len(res.Hosts) > 0 {
		hostTasksMap, err = s.getTasks(ctx, res.Hosts, task.Type)
		if err != nil {
			log.WithFields(log.Fields{
				"hosts":     hosts,
				"filter":    filter,
				"task_type": task.Type,
			}).Info(_failedToFetchTasksOnHosts)
			s.metrics.HostGetFail.Inc(1)
			return nil, err
		}
	}
	s.metrics.HostGet.Inc(1)
	return s.fillTasksInHost(res.GetHosts(), hostTasksMap), nil
}

// fillTasksInHost creates host Info into placement hosts.
// One key notion is to add already running tasks on this host
// such that placement can take care of task-task affinity.
func (s *service) fillTasksInHost(
	hosts []*hostsvc.HostInfo,
	tasks map[string]*resmgrsvc.TaskList) []*models.Host {
	placementHosts := make([]*models.Host, 0, len(hosts))
	for _, host := range hosts {
		var taskList []*resmgr.Task
		if tasks != nil && tasks[host.Hostname] != nil {
			taskList = tasks[host.Hostname].Tasks
		}
		placementHosts = append(placementHosts, models.NewHosts(host, taskList))
	}

	return placementHosts
}

// getTasks returns the tasks running on provided host from resource manager.
func (s *service) getTasks(
	ctx context.Context,
	hostsInfo []*hostsvc.HostInfo,
	taskType resmgr.TaskType) (map[string]*resmgrsvc.TaskList, error) {
	ctx, cancelFunc := context.WithTimeout(ctx, _timeout)
	defer cancelFunc()

	// Extract the hostname
	hostnames := make([]string, 0, len(hostsInfo))
	for _, hostInfo := range hostsInfo {
		hostnames = append(hostnames, hostInfo.Hostname)
	}

	// Get tasks running on provided hosts
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

// ReserveHost reserves the given host for the given task in Host Manager
func (s *service) ReserveHost(ctx context.Context, hosts []*models.Host, task *resmgr.Task) error {
	return errors.New(errnotImplemented.Error())
}

// GetCompletedReservation gets the completed reservation from host manager
func (s *service) GetCompletedReservation(ctx context.Context,
) ([]*hostsvc.CompletedReservation, error) {
	return nil, errors.New(errnotImplemented.Error())
}
