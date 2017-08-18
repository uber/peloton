package placement

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common/async"
	"code.uber.internal/infra/peloton/hostmgr/scalar"
	"code.uber.internal/infra/peloton/placement/models"
	"code.uber.internal/infra/peloton/placement/offers"
	"code.uber.internal/infra/peloton/placement/tasks"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
)

const (
	// _getOfferTimeout is the timeout value for get offer request
	_getOfferTimeout = 1 * time.Second
	// _getTaskTimeout is the timeout value for get task request
	_getTaskTimeout = 1 * time.Second
)

// Engine is an interface implementing a way to Start and Stop the
// placement engine
type Engine interface {
	Start()
	Stop()
}

// New creates a new placement engine
func New(
	d *yarpc.Dispatcher,
	parent tally.Scope,
	cfg *Config,
	resMgrClientName string,
	hostMgrClientName string,
	taskStore storage.TaskStore) Engine {
	resourceManager := resmgrsvc.NewResourceManagerServiceYARPCClient(d.ClientConfig(resMgrClientName))
	hostManager := hostsvc.NewInternalHostServiceYARPCClient(d.ClientConfig(hostMgrClientName))
	s := placementEngine{
		cfg:          cfg,
		offerManager: offers.NewManager(hostManager, resourceManager),
		taskManager:  tasks.NewManager(resourceManager),
		taskStore:    taskStore,
		rootCtx:      context.Background(),
		metrics:      NewMetrics(parent.SubScope("placement")),
		pool:         async.NewPool(async.PoolOptions{}),
	}
	return &s
}

type placementEngine struct {
	cfg          *Config
	offerManager offers.Manager
	taskManager  tasks.Manager
	taskStore    storage.TaskStore
	rootCtx      context.Context
	started      int32
	shutdown     int32
	metrics      *Metrics
	tick         <-chan time.Time
	pool         *async.Pool
}

// Start will start the placement engine.
func (s *placementEngine) Start() {
	if atomic.CompareAndSwapInt32(&s.started, 0, 1) {
		log.Info("Placement Engine starting")
		s.metrics.Running.Update(1)
		go func() {
			// TODO: We need to revisit here if we want to run multiple
			// Placement engine threads
			for s.isRunning() {
				delay := s.placeRound()
				time.Sleep(delay)
			}
		}()
	}
	log.Info("Placement Engine started")
}

// Stop will stop the placement engine.
func (s *placementEngine) Stop() {
	log.Info("Placement Engine stopping")
	s.metrics.Running.Update(0)
	atomic.StoreInt32(&s.shutdown, 1)
}

// placeTaskGroup is the internal loop that makes placement decisions on a group of tasks
// with same grouping constraint.
func (s *placementEngine) placeTaskGroup(group *taskGroup) {
	log.WithFields(log.Fields{
		"tasks":  group.tasks,
		"filter": group.hostFilter,
	}).Debug("Placing task group")
	totalTasks := len(group.tasks)
	// TODO: move this loop out to the call site of current function,
	//       so we don't need to loop in the test code.
	placementDeadline := time.Now().Add(s.cfg.MaxPlacementDuration)
	for time.Now().Before(placementDeadline) && s.isRunning() {
		if len(group.tasks) == 0 {
			log.Debug("Finishing place task group loop because all tasks are placed")
			return
		}

		offers, resultCounts, err := s.acquireHostOffers(group)
		// TODO: Add a stopping condition so this does not loop forever.
		if err != nil {
			log.WithError(err).Error("Failed to acquire host offer")
			s.metrics.OfferGetFail.Inc(1)
			time.Sleep(_getOfferTimeout)
			continue
		}

		if len(offers) == 0 {
			s.metrics.OfferStarved.Inc(1)
			log.WithField("filter_result_counts", resultCounts).
				Warn("Empty offers received")
			time.Sleep(_getOfferTimeout)
			continue
		}
		s.metrics.OfferGet.Inc(1)

		log.WithFields(log.Fields{
			"tasks":               group.tasks,
			"host_offers":         offers,
			"filter_result_count": resultCounts,
		}).Debug("acquired host offers for tasks")

		// Creating the placements for all the host offers
		var placements []*resmgr.Placement
		var placement *resmgr.Placement
		index := 0
		for _, offer := range offers {
			if len(group.tasks) <= 0 {
				break
			}
			placement, group.tasks = s.placeTasks(
				group.tasks,
				group.getResourceConfig(),
				offer,
			)
			placements = append(placements, placement)
			index++
		}

		// Setting the placements for all the placements
		err = s.setPlacements(placements)

		if err != nil {
			log.WithFields(log.Fields{
				"placements":        placements,
				"total_host_offers": offers,
			}).WithError(err).Error("set placements failed")
			// If there is error in placement returning all the host offer
			s.returnUnused(offers)
		} else {
			unused := offers[index:]
			if len(unused) > 0 {
				log.WithFields(log.Fields{
					"placements":         placements,
					"unused_host_offers": unused,
					"total_host_offers":  offers,
				}).Debug("return unused offers after set placement")
				s.returnUnused(unused)
			}
			log.WithField("num_placements", len(placements)).Info("set placements succeeded")
		}

		log.WithFields(log.Fields{
			"acquired_host_offers": offers,
			"placements":           placements,
			"remaining_tasks":      group.tasks,
		}).Debug("Tasks remaining for next placeTaskGroup")
	}
	if len(group.tasks) > 0 {
		log.WithFields(log.Fields{
			"tasks_remaining":       group.tasks,
			"tasks_remaining_total": len(group.tasks),
			"tasks_total":           totalTasks,
		}).Warn("Could not place tasks due to insufficiant offers and placement timeout")
		// TODO: add metrics for this
		// TODO(mu): send unplaced tasks back to correct state (READY) per T1028631.
	}
}

// returnUnused returns unused host offers back to host manager.
func (s *placementEngine) returnUnused(offers []*models.Offer) error {
	ctx, cancelFunc := context.WithTimeout(s.rootCtx, 10*time.Second)
	defer cancelFunc()
	return s.offerManager.Release(ctx, offers)
}

// acquireHostOffers calls hostmgr and obtain HostOffers for given task group.
func (s *placementEngine) acquireHostOffers(group *taskGroup) (
	[]*models.Offer,
	map[string]uint32,
	error,
) {
	// Make a deep copy because we are modifying this struct here.
	filter := proto.Clone(&group.hostFilter).(*hostsvc.HostFilter)
	// Right now, this limits number of hosts to request from hostsvc.
	// In the longer term, we should consider converting this to total
	// resources necessary.
	maxHosts := s.cfg.OfferDequeueLimit
	if maxHosts > len(group.tasks) {
		// only require up to #tasks in the group.
		maxHosts = len(group.tasks)
	}
	filter.Quantity = &hostsvc.QuantityControl{
		MaxHosts: uint32(maxHosts),
	}

	ctx, cancelFunc := context.WithTimeout(s.rootCtx, 10*time.Second)
	defer cancelFunc()
	offers, filterResults, err := s.offerManager.Acquire(ctx, false, resmgr.TaskType_UNKNOWN, filter)
	if err != nil {
		return nil, nil, err
	}
	return offers, filterResults, nil
}

// placeTasks takes the tasks and convert them to placements
func (s *placementEngine) placeTasks(
	tasks []*resmgr.Task,
	resourceConfig *task.ResourceConfig,
	offer *models.Offer) (*resmgr.Placement, []*resmgr.Task) {

	if len(tasks) == 0 {
		return nil, tasks
	}

	usage := scalar.FromResourceConfig(resourceConfig)
	remain := scalar.FromMesosResources(offer.Offer.GetResources())

	numTotalTasks := uint32(len(tasks))
	// Assuming all tasks within the same taskgroup have the same ports num
	// config, as numports is used as task group key.
	numPortsPerTask := tasks[0].GetNumPorts()
	availablePorts := make(map[uint32]bool)
	if numPortsPerTask > 0 {
		availablePorts = util.GetPortsSetFromResources(
			offer.Offer.GetResources())
		numAvailablePorts := len(availablePorts)
		numTasksByAvailablePorts := uint32(numAvailablePorts) / numPortsPerTask
		if numTasksByAvailablePorts < numTotalTasks {
			log.WithFields(log.Fields{
				"resmgr_task":         tasks[0],
				"num_available_ports": numAvailablePorts,
				"num_available_tasks": numTasksByAvailablePorts,
				"total_tasks":         numTotalTasks,
			}).Warn("Insufficient ports resources.")
			numTotalTasks = numTasksByAvailablePorts
		}
	}

	var selectedTasks []*resmgr.Task
	for i := uint32(0); i < numTotalTasks; i++ {
		trySubtract := remain.TrySubtract(&usage)
		if trySubtract == nil {
			// NOTE: current placement implementation means all
			// tasks in the same group has the same resource configuration.
			log.WithFields(log.Fields{
				"remain": remain,
				"usage":  usage,
			}).Debug("Insufficient resource in remain")
			break
		}
		remain = *trySubtract
		selectedTasks = append(selectedTasks, tasks[i])
	}
	tasks = tasks[len(selectedTasks):]

	log.WithFields(log.Fields{
		"selected_tasks":        selectedTasks,
		"selected_tasks_total":  len(selectedTasks),
		"remaining_tasks":       tasks,
		"remaining_tasks_total": len(tasks),
		"host_offer":            offer,
	}).Debug("Selected tasks to place")

	numSelectedTasks := len(selectedTasks)
	if numSelectedTasks <= 0 {
		return nil, tasks
	}

	var selectedPorts []uint32
	for port := range availablePorts {
		// Port distribution is randomized by iterating map.
		if uint32(len(selectedPorts)) >= uint32(numSelectedTasks)*numPortsPerTask {
			break
		}
		selectedPorts = append(selectedPorts, port)
	}

	placement := s.createTasksPlacement(
		selectedTasks,
		offer,
		selectedPorts,
	)

	return placement, tasks
}

func (s *placementEngine) setPlacements(placements []*resmgr.Placement) error {
	ctx, cancelFunc := context.WithTimeout(s.rootCtx, 10*time.Second)
	defer cancelFunc()
	setPlacementStart := time.Now()
	err := s.taskManager.SetPlacements(ctx, placements)
	setPlacementDuration := time.Since(setPlacementStart)
	s.metrics.SetPlacementDuration.Record(setPlacementDuration)
	if err != nil {
		s.metrics.SetPlacementFail.Inc(1)
		return err
	}
	s.metrics.SetPlacementSuccess.Inc(int64(len(placements)))
	log.WithFields(log.Fields{
		"num_placements": len(placements),
		"placements":     placements,
		"duration":       setPlacementDuration.Seconds(),
	}).Debug("Set placements call returned")
	return nil
}

// createTasksPlacement creates the placement for resource manager
// It also returns the list of tasks which can not be placed
func (s *placementEngine) createTasksPlacement(tasks []*resmgr.Task,
	offer *models.Offer,
	selectedPorts []uint32) *resmgr.Placement {

	createPlacementStart := time.Now()
	createPlacementDuration := time.Since(createPlacementStart)
	var tasksIds []*peloton.TaskID
	for _, t := range tasks {
		taskID := t.Id
		tasksIds = append(tasksIds, taskID)
	}

	placement := &resmgr.Placement{
		AgentId:  offer.Offer.AgentId,
		Hostname: offer.Offer.Hostname,
		Tasks:    tasksIds,
		Ports:    selectedPorts,
		Type:     tasks[0].Type,
		// TODO : We are not setting offerId's
		// we need to remove it from protobuf
	}

	log.WithFields(log.Fields{
		"tasks":      tasks,
		"host_offer": offer,
		"num_tasks":  len(tasksIds),
	}).Debug("created placement")

	s.metrics.CreatePlacementDuration.Record(createPlacementDuration)
	return placement
}

func (s *placementEngine) isRunning() bool {
	shutdown := atomic.LoadInt32(&s.shutdown)
	return shutdown == 0
}

// placeRound tries one round of placement action
func (s *placementEngine) placeRound() time.Duration {
	tasks, err := s.getTasks(s.cfg.TaskDequeueLimit)
	if err != nil {
		log.WithError(err).Error("Failed to dequeue tasks")
		return _getTaskTimeout
	}
	if len(tasks) == 0 {
		log.Debug("No task to place in workLoop")
		// Retry immediately.
		return 0
	}

	log.WithField("tasks", len(tasks)).Info("Dequeued from task queue")

	taskGroups := groupTasks(tasks)
	for _, tg := range taskGroups {
		// Enqueue per task group.
		group := tg
		s.pool.Enqueue(async.JobFunc(func(ctx context.Context) {
			s.placeTaskGroup(group)
		}))
	}

	return 0
}

// getTasks deques tasks from task queue in resource manager
func (s *placementEngine) getTasks(limit int) (
	taskInfos []*resmgr.Task, err error) {
	ctx, cancelFunc := context.WithTimeout(s.rootCtx, 10*time.Second)
	defer cancelFunc()
	gangs, err := s.taskManager.Dequeue(ctx, resmgr.TaskType_UNKNOWN, s.cfg.TaskDequeueLimit, s.cfg.TaskDequeueTimeOut)
	if err != nil {
		return nil, err
	}
	for _, gang := range gangs {
		for _, task := range gang.GetTasks() {
			taskInfos = append(taskInfos, task)
		}
	}

	log.WithField("tasks", taskInfos).Debug("Dequeued tasks from resmgr")

	return taskInfos, nil
}

type taskGroup struct {
	hostFilter hostsvc.HostFilter
	tasks      []*resmgr.Task
}

func (g *taskGroup) getResourceConfig() *task.ResourceConfig {
	return g.hostFilter.GetResourceConstraint().GetMinimum()
}

func getHostFilter(t *resmgr.Task) hostsvc.HostFilter {
	result := hostsvc.HostFilter{
		// HostLimit will be later determined by number of tasks.
		ResourceConstraint: &hostsvc.ResourceConstraint{
			Minimum:  t.Resource,
			NumPorts: t.NumPorts,
		},
	}
	if t.Constraint != nil {
		result.SchedulingConstraint = t.Constraint
	}
	return result
}

// groupTasks groups tasks based on call constraint to hostsvc.
// Returns grouped tasks keyed by serialized hostsvc.Constraint.
func groupTasks(tasks []*resmgr.Task) map[string]*taskGroup {
	groups := make(map[string]*taskGroup)
	for _, t := range tasks {
		filter := getHostFilter(t)
		// String() function on protobuf message should be nil-safe.
		s := filter.String()
		if _, ok := groups[s]; !ok {
			groups[s] = &taskGroup{
				hostFilter: filter,
				tasks:      []*resmgr.Task{},
			}
		}
		groups[s].tasks = append(groups[s].tasks, t)
	}
	return groups
}
