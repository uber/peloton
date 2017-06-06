package placement

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common/async"
	"code.uber.internal/infra/peloton/hostmgr/scalar"
	"code.uber.internal/infra/peloton/util"
)

const (
	// GetOfferTimeout is the timeout value for get offer request
	GetOfferTimeout = 1 * time.Second
	// GetTaskTimeout is the timeout value for get task request
	GetTaskTimeout = 1 * time.Second
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
	hostMgrClientName string) Engine {

	s := placementEngine{
		cfg:           cfg,
		resMgrClient:  resmgrsvc.NewResourceManagerServiceYarpcClient(d.ClientConfig(resMgrClientName)),
		hostMgrClient: hostsvc.NewInternalHostServiceYarpcClient(d.ClientConfig(hostMgrClientName)),
		rootCtx:       context.Background(),
		metrics:       NewMetrics(parent.SubScope("placement")),
		pool:          async.NewPool(async.PoolOptions{}),
	}
	return &s
}

type placementEngine struct {
	cfg           *Config
	resMgrClient  resmgrsvc.ResourceManagerServiceYarpcClient
	hostMgrClient hostsvc.InternalHostServiceYarpcClient
	rootCtx       context.Context
	started       int32
	shutdown      int32
	metrics       *Metrics
	tick          <-chan time.Time
	pool          *async.Pool
}

// Start starts placement engine
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

// Stop stops placement engine
func (s *placementEngine) Stop() {
	log.Info("Placement Engine stopping")
	s.metrics.Running.Update(0)
	atomic.StoreInt32(&s.shutdown, 1)
}

// placeTaskGroup is the internal loop that makes placement decisions on a group of tasks
// with same grouping constraint.
func (s *placementEngine) placeTaskGroup(group *taskGroup) {
	log.WithField("group", group).Debug("Placing task group")
	totalTasks := len(group.tasks)
	// TODO: move this loop out to the call site of current function,
	//       so we don't need to loop in the test code.
	placementDeadline := time.Now().Add(s.cfg.MaxPlacementDuration)
	for time.Now().Before(placementDeadline) && s.isRunning() {
		if len(group.tasks) == 0 {
			log.Debug("Finishing place task group loop because all tasks are placed")
			return
		}

		hostOffers, err := s.AcquireHostOffers(group)
		// TODO: Add a stopping condition so this does not loop forever.
		if err != nil {
			log.WithField("error", err).Error("Failed to dequeue offer")
			s.metrics.OfferGetFail.Inc(1)
			time.Sleep(GetOfferTimeout)
			continue
		}

		if len(hostOffers) == 0 {
			s.metrics.OfferStarved.Inc(1)
			log.Warn("Empty hostOffers received")
			time.Sleep(GetOfferTimeout)
			continue
		}
		s.metrics.OfferGet.Inc(1)

		// Creating the placements for all the host offers
		var placements []*resmgr.Placement
		var placement *resmgr.Placement
		index := 0
		for _, hostOffer := range hostOffers {
			if len(group.tasks) <= 0 {
				break
			}
			placement, group.tasks = s.placeTasks(
				group.tasks,
				group.getResourceConfig(),
				hostOffer,
			)
			placements = append(placements, placement)
			index++
		}

		// Setting the placements for all the placements
		err = s.setPlacements(placements)

		if err != nil {
			// If there is error in placement returning all the host offer
			s.returnUnused(hostOffers)
		} else {
			unused := hostOffers[index:]
			if len(unused) > 0 {
				s.returnUnused(unused)
			}
		}

		log.WithField("remaining_tasks", group.tasks).
			Debug("Tasks remaining for next placeTaskGroup")
	}
	if len(group.tasks) > 0 {
		log.WithFields(log.Fields{
			"Tasks Remaining": len(group.tasks),
			"Tasks Total":     totalTasks,
		}).Warn("Could not place Tasks due to insufficiant Offers")

		log.WithField("task_group", group.tasks).
			Debug("Task group still has remaining tasks " +
				"after allowed duration")
		// TODO: add metrics for this
		// TODO: send unplaced tasks back to correct state (READY).
	}
}

// returnUnused returns unused host offers back to host manager.
func (s *placementEngine) returnUnused(hostOffers []*hostsvc.HostOffer) error {
	ctx, cancelFunc := context.WithTimeout(s.rootCtx, 10*time.Second)
	defer cancelFunc()
	var request = &hostsvc.ReleaseHostOffersRequest{
		HostOffers: hostOffers,
	}

	response, err := s.hostMgrClient.ReleaseHostOffers(ctx, request)
	if err != nil {
		log.WithField("error", err).Error("ReleaseHostOffers failed")
		return err
	}

	if respErr := response.GetError(); respErr != nil {
		log.WithField("error", respErr).Error("ReleaseHostOffers error")
		// TODO: Differentiate known error types by metrics and logs.
		return errors.New(respErr.String())
	}

	log.WithField("host_offers", hostOffers).Debug("Returned unused host offers")
	return nil
}

// AcquireHostOffers calls hostmgr and obtain HostOffers for given task group.
func (s *placementEngine) AcquireHostOffers(group *taskGroup) ([]*hostsvc.HostOffer, error) {
	// Right now, this limits number of hosts to request from hostsvc.
	// In the longer term, we should consider converting this to total resources necessary.
	limit := s.cfg.OfferDequeueLimit
	if len(group.tasks) < limit {
		limit = len(group.tasks)
	}

	// Make a deep copy because we are modifying this struct here.
	constraint := proto.Clone(&group.constraint).(*hostsvc.Constraint)
	constraint.HostLimit = uint32(limit)

	ctx, cancelFunc := context.WithTimeout(s.rootCtx, 10*time.Second)
	defer cancelFunc()
	var request = &hostsvc.AcquireHostOffersRequest{
		Constraint: constraint,
	}

	log.WithField("request", request).Debug("Calling AcquireHostOffers")

	response, err := s.hostMgrClient.AcquireHostOffers(ctx, request)

	if err != nil {
		log.WithField("error", err).Error("AcquireHostOffers failed")
		return nil, err
	}

	log.WithField("response", response).Debug("AcquireHostOffers returned")

	if respErr := response.GetError(); respErr != nil {
		log.WithField("error", respErr).Error("AcquireHostOffers error")
		// TODO: Differentiate known error types by metrics and logs.
		return nil, errors.New(respErr.String())
	}

	result := response.GetHostOffers()
	return result, nil
}

// placeTasks takes the tasks and convert them to placements
func (s *placementEngine) placeTasks(
	tasks []*resmgr.Task,
	resourceConfig *task.ResourceConfig,
	hostOffer *hostsvc.HostOffer) (*resmgr.Placement, []*resmgr.Task) {

	if len(tasks) == 0 {
		log.Debug("No task to place")
		return nil, tasks
	}

	usage := scalar.FromResourceConfig(resourceConfig)
	remain := scalar.FromMesosResources(hostOffer.GetResources())

	numTotalTasks := uint32(len(tasks))
	// Assuming all tasks within the same taskgroup have the same ports num
	// config, as numports is used as task group key.
	numPortsPerTask := tasks[0].GetNumPorts()
	availablePorts := make(map[uint32]bool)
	if numPortsPerTask > 0 {
		availablePorts = util.GetPortsSetFromResources(
			hostOffer.GetResources())
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
		"selected_tasks":  selectedTasks,
		"remaining_tasks": tasks,
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
		hostOffer,
		selectedPorts,
	)

	return placement, tasks
}

func (s *placementEngine) setPlacements(placements []*resmgr.Placement) error {
	if len(placements) == 0 {
		log.Debug("No task to place")
		err := errors.New("No placements to set")
		return err
	}

	ctx, cancelFunc := context.WithTimeout(s.rootCtx, 10*time.Second)
	defer cancelFunc()
	var request = &resmgrsvc.SetPlacementsRequest{
		Placements: placements,
	}
	log.WithField("request", request).Debug("Calling SetPlacements")

	setPlacementStart := time.Now()
	response, err := s.resMgrClient.SetPlacements(ctx, request)
	setPlacementDuration := time.Since(setPlacementStart)
	s.metrics.SetPlacementDuration.Record(setPlacementDuration)

	// TODO: add retry / put back offer and tasks in failure scenarios
	if err != nil {
		log.WithFields(log.Fields{
			"num_placements": len(placements),
			"error":          err.Error(),
		}).WithError(errors.New("Failed to set placements"))

		s.metrics.SetPlacementFail.Inc(1)
		return err
	}

	log.WithField("response", response).Debug("SetPlacements returned")

	if response.GetError() != nil {
		log.WithFields(log.Fields{
			"num_placements": len(placements),
			"error":          response.Error.String(),
		}).Error("Failed to place tasks")
		s.metrics.SetPlacementFail.Inc(1)
		return errors.New("Failed to place tasks")
	}
	lenTasks := 0
	for _, p := range placements {
		lenTasks += len(p.Tasks)
	}
	s.metrics.SetPlacementSuccess.Inc(int64(len(placements)))

	log.WithFields(log.Fields{
		"num_placements": len(placements),
		"duration":       setPlacementDuration.Seconds(),
	}).Info("Set placements")
	return nil
}

// createTasksPlacement creates the placement for resource manager
// It also returns the list of tasks which can not be placed
func (s *placementEngine) createTasksPlacement(tasks []*resmgr.Task,
	hostOffer *hostsvc.HostOffer,
	selectedPorts []uint32) *resmgr.Placement {

	createPlacementStart := time.Now()
	createPlacementDuration := time.Since(createPlacementStart)
	var tasksIds []*peloton.TaskID
	for _, t := range tasks {
		taskID := t.Id
		tasksIds = append(tasksIds, taskID)
	}

	placement := &resmgr.Placement{
		AgentId:  hostOffer.AgentId,
		Hostname: hostOffer.Hostname,
		Tasks:    tasksIds,
		Ports:    selectedPorts,
		Type:     tasks[0].Type,
		// TODO : We are not setting offerId's
		// we need to remove it from protobuf
	}

	log.WithFields(log.Fields{
		"num_tasks": len(tasksIds),
	}).Info("Create Placements")

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
		log.WithField("error", err).Error("Failed to dequeue tasks")
		return GetTaskTimeout
	}
	if len(tasks) == 0 {
		log.Debug("No task to place in workLoop")
		return GetTaskTimeout
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
	var request = &resmgrsvc.DequeueGangsRequest{
		Limit:   uint32(limit),
		Timeout: uint32(s.cfg.TaskDequeueTimeOut),
	}

	log.WithField("request", request).Debug("Dequeuing tasks")
	response, err := s.resMgrClient.DequeueGangs(ctx, request)
	if err != nil {
		log.WithField("error", err).Error("Dequeue failed")
		return nil, err
	}
	for _, gang := range response.GetGangs() {
		for _, task := range gang.GetTasks() {
			taskInfos = append(taskInfos, task)
		}
	}

	log.WithField("tasks", taskInfos).Debug("Dequeued tasks")

	return taskInfos, nil
}

type taskGroup struct {
	constraint hostsvc.Constraint
	tasks      []*resmgr.Task
}

func (g *taskGroup) getResourceConfig() *task.ResourceConfig {
	return g.constraint.GetResourceConstraint().GetMinimum()
}

func getHostSvcConstraint(t *resmgr.Task) hostsvc.Constraint {
	result := hostsvc.Constraint{
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
		c := getHostSvcConstraint(t)
		// String() function on protobuf message should be nil-safe.
		s := c.String()
		if _, ok := groups[s]; !ok {
			groups[s] = &taskGroup{
				constraint: c,
				tasks:      []*resmgr.Task{},
			}
		}
		groups[s].tasks = append(groups[s].tasks, t)
	}
	return groups
}
