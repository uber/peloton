// Placement Engine Interface
// IN: job
// OUT: placement decision <task, node>
// https://github.com/Netflix/Fenzo

package placement

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"peloton/api/task"
	"peloton/api/task/config"
	"peloton/private/hostmgr/hostsvc"
	"peloton/private/resmgr/taskqueue"

	mesos "mesos/v1"

	"code.uber.internal/infra/peloton/hostmgr/scalar"
	placement_config "code.uber.internal/infra/peloton/placement/config"
	placement_metrics "code.uber.internal/infra/peloton/placement/metrics"
	log "github.com/Sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"
)

const (
	// GetOfferTimeout is the timeout value for get offer request
	GetOfferTimeout = 1 * time.Second
	// GetTaskTimeout is the timeout value for get task request
	GetTaskTimeout = 1 * time.Second
)

// Engine is an interface implementing a way to Start and Stop the placement engine
type Engine interface {
	Start()
	Stop()
}

// New creates a new placement engine
func New(
	d yarpc.Dispatcher,
	cfg *placement_config.PlacementConfig,
	scope tally.Scope,
	resMgrClientName string,
	hostMgrClientName string) Engine {
	metrics := placement_metrics.New(scope)
	s := placementEngine{
		cfg:           cfg,
		resMgrClient:  json.New(d.ClientConfig(resMgrClientName)),
		hostMgrClient: json.New(d.ClientConfig(hostMgrClientName)),
		rootCtx:       context.Background(),
		metrics:       &metrics,
	}
	return &s
}

type placementEngine struct {
	cfg           *placement_config.PlacementConfig
	resMgrClient  json.Client
	hostMgrClient json.Client
	rootCtx       context.Context
	started       int32
	shutdown      int32
	metrics       *placement_metrics.Metrics
}

// Start starts placement engine
func (s *placementEngine) Start() {
	if atomic.CompareAndSwapInt32(&s.started, 0, 1) {
		log.Info("Placement Engine started")
		s.metrics.Running.Update(1)
		go s.workLoop()
		return
	}
	log.Warn("Placement Engine already started")
}

// Stop stops placement engine
func (s *placementEngine) Stop() {
	log.Info("Placement Engine stopping")
	s.metrics.Running.Update(0)
	atomic.StoreInt32(&s.shutdown, 1)
}

// placeTaskGroup is the internal loop that makes placement decisions on a group of tasks
// with same grouping constraint.
func (s *placementEngine) placeTaskGroup(group *taskGroup) {
	for shutdown := atomic.LoadInt32(&s.shutdown); shutdown == 0; {
		hostOffers, err := s.getHostOffers(group)
		// TODO: Add a stopping condition so this does not loop forever.
		if err != nil {
			log.WithField("error", err).Error("Failed to dequeue offer")
			s.metrics.OfferGetFail.Inc(1)
			time.Sleep(GetOfferTimeout)
			continue
		}

		if len(hostOffers) == 0 {
			s.metrics.OfferStarved.Inc(1)
			time.Sleep(GetOfferTimeout)
			continue
		}
		s.metrics.OfferGet.Inc(1)

		index := 0
		for _, hostOffer := range hostOffers {
			group.tasks = s.placeTasks(group.tasks, group.resourceConfig, hostOffer)
			if len(group.tasks) == 0 {
				log.Debug("All tasks in group are placed")
				break
			}
			index++
		}

		unused := hostOffers[index:]
		// TODO: Handle remaining offers rather than a log.
		if len(unused) > 0 {
			log.WithField("remaining_offers", unused).Warn("HostOffes reamining will not be used until next cycle!")
		}
		log.WithField("remainng_tasks", len(group.tasks)).Info("Tasks remaining for next placeTaskGroup")
	}
}

// getHostOffers calls hostmgr and obtain HostOffers for given task group.
func (s *placementEngine) getHostOffers(group *taskGroup) ([]*hostsvc.HostOffer, error) {
	// Right now, this limits number of hosts to request from hostsvc.
	// In the longer term, we should consider converting this to total resources necessary.
	limit := s.cfg.OfferDequeueLimit
	if len(group.tasks) < limit {
		limit = len(group.tasks)
	}

	ctx, cancelFunc := context.WithTimeout(s.rootCtx, 10*time.Second)
	defer cancelFunc()
	var response hostsvc.GetHostOffersResponse
	var request = &hostsvc.GetHostOffersRequest{
		Constraints: []*hostsvc.Constraint{
			{
				Limit: uint32(limit),
				ResourceConstraint: &hostsvc.ResourceConstraint{
					Minimum: group.resourceConfig,
				},
			},
		},
	}
	_, err := s.hostMgrClient.Call(
		ctx,
		yarpc.NewReqMeta().Procedure("InternalHostService.GetHostOffers"),
		request,
		&response,
	)

	if err != nil {
		log.WithField("error", err).Error("GetHostOffers failed")
		return nil, err
	}

	if respErr := response.GetError(); respErr != nil {
		log.WithField("error", respErr).Error("GetHostOffer error")
		// TODO: Differentiate known error types by metrics and logs.
		return nil, errors.New(respErr.String())
	}

	log.Debug("Obtained HostOffers")
	return response.GetHostOffers(), nil
}

// placeTasks makes placement decisions by assigning tasks to offer
func (s *placementEngine) placeTasks(
	tasks []*task.TaskInfo,
	resourceConfig *config.ResourceConfig,
	hostOffer *hostsvc.HostOffer) []*task.TaskInfo {
	usage := scalar.FromResourceConfig(resourceConfig)
	remain := scalar.FromMesosResources(hostOffer.GetResources())
	nTasks := len(tasks)

	var selectedTasks []*task.TaskInfo
	for i := 0; i < nTasks; i++ {
		if !remain.TrySubtract(&usage) {
			break
		}

		selectedTasks = append(selectedTasks, tasks[i])
	}

	log.WithField("selected_tasks", len(selectedTasks)).Debug("Selected tasks count")
	tasks = tasks[len(selectedTasks) : len(tasks)-1]
	// TODO: replace launch task with resmgr.SetPlacement once it's implemented,
	//       and move task launching logic into Jobmgr
	if len(selectedTasks) > 0 {
		ctx, cancelFunc := context.WithTimeout(s.rootCtx, 10*time.Second)
		defer cancelFunc()
		var response hostsvc.LaunchTasksResponse
		var request = &hostsvc.LaunchTasksRequest{
			Hostname: hostOffer.GetHostname(),
			Tasks:    createLaunchableTasks(selectedTasks),
			AgentId:  hostOffer.GetAgentId(),
			OfferIds: hostOffer.GetOfferIds(),
		}
		_, err := s.hostMgrClient.Call(
			ctx,
			yarpc.NewReqMeta().Procedure("InternalHostService.LaunchTasks"),
			request,
			&response,
		)
		// TODO: add retry / put back offer and tasks in failure scenarios
		if err != nil {
			log.WithFields(log.Fields{
				"tasks": len(selectedTasks),
				"error": err,
			}).Error("Failed to launch tasks")
			s.metrics.TaskLaunchDispatchesFail.Inc(1)
			return tasks
		}
		if response.Error != nil {
			log.WithFields(log.Fields{
				"tasks": len(selectedTasks),
				"error": response.Error.String(),
			}).Error("Failed to launch tasks")
			s.metrics.TaskLaunchDispatchesFail.Inc(1)
			return tasks
		}
		s.metrics.TaskLaunchDispatches.Inc(1)

		log.WithFields(log.Fields{
			"tasks":    selectedTasks,
			"hostname": hostOffer.GetHostname(),
			"offers":   hostOffer.GetOfferIds(),
		}).Info("Launched tasks")
	}
	return tasks
}

// workLoop is the internal loop that gets tasks and makes placement decisions
func (s *placementEngine) workLoop() {
	for shutdown := atomic.LoadInt32(&s.shutdown); shutdown == 0; {
		tasks, err := s.getTasks(s.cfg.TaskDequeueLimit)
		if err != nil {
			log.WithField("error", err).Error("Failed to dequeue tasks")
			time.Sleep(GetTaskTimeout)
			continue
		}
		if len(tasks) == 0 {
			time.Sleep(GetTaskTimeout)
			continue
		}
		log.WithField("tasks", len(tasks)).Info("Dequeued from task queue")
		taskGroups := groupTasksByResource(tasks)
		for _, taskGroup := range taskGroups {
			s.placeTaskGroup(taskGroup)
		}
	}
}

// getTasks deques tasks from task queue in resource manager
func (s *placementEngine) getTasks(limit int) (
	taskInfos []*task.TaskInfo, err error) {
	// It could happen that the work loop is started before the
	// peloton master inbound is started.  In such case it could
	// panic. This we capture the panic, return error, wait then
	// resume
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("Recovered from panic %v", r)
		}
	}()

	ctx, cancelFunc := context.WithTimeout(s.rootCtx, 10*time.Second)
	defer cancelFunc()
	var response taskqueue.DequeueResponse
	var request = &taskqueue.DequeueRequest{
		Limit: uint32(limit),
	}
	_, err = s.resMgrClient.Call(
		ctx,
		yarpc.NewReqMeta().Procedure("TaskQueue.Dequeue"),
		request,
		&response,
	)
	if err != nil {
		log.WithField("error", err).Error("Dequeue failed")
		return nil, err
	}
	return response.Tasks, nil
}

type taskGroup struct {
	resourceConfig *config.ResourceConfig
	tasks          []*task.TaskInfo
}

// groupTasksByResource groups tasks which are to be placed based on their ResourceConfig.
// Returns grouped tasks keyed by serialized ResourceLimit
func groupTasksByResource(tasks []*task.TaskInfo) map[string]*taskGroup {
	groups := make(map[string]*taskGroup)
	for _, t := range tasks {
		rc := t.GetConfig().GetResource()
		// String() function on protobuf message should be nil-safe.
		s := rc.String()
		if _, ok := groups[s]; !ok {
			groups[s] = &taskGroup{
				resourceConfig: rc,
				tasks:          []*task.TaskInfo{},
			}
		}
		groups[s].tasks = append(groups[s].tasks, t)
	}
	return groups
}

// createLaunchTasksRequest generates hostsvc.LaunchTasksRequest from tasks and offer
func createLaunchTasksRequest(
	tasks []*task.TaskInfo,
	offer *mesos.Offer) *hostsvc.LaunchTasksRequest {
	return &hostsvc.LaunchTasksRequest{
		Hostname: offer.GetHostname(),
		Tasks:    createLaunchableTasks(tasks),
		AgentId:  offer.GetAgentId(),
		OfferIds: []*mesos.OfferID{offer.Id},
	}
}

// createLaunchableTasks generates list of hostsvc.LaunchableTask from list of task.TaskInfo
func createLaunchableTasks(tasks []*task.TaskInfo) []*hostsvc.LaunchableTask {
	var launchableTasks []*hostsvc.LaunchableTask
	for _, task := range tasks {
		launchableTask := hostsvc.LaunchableTask{
			TaskId: task.GetRuntime().GetTaskId(),
			Config: task.GetConfig(),
		}
		launchableTasks = append(launchableTasks, &launchableTask)
	}
	return launchableTasks
}
