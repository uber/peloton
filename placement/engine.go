package placement

import (
	"context"
	"strings"
	"time"

	"code.uber.internal/infra/peloton/placement/plugins"
	"code.uber.internal/infra/peloton/placement/plugins/batch"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	"code.uber.internal/infra/peloton/common/async"
	"code.uber.internal/infra/peloton/mimir-lib"
	"code.uber.internal/infra/peloton/mimir-lib/model/metrics"
	"code.uber.internal/infra/peloton/placement/config"
	tally_metrics "code.uber.internal/infra/peloton/placement/metrics"
	"code.uber.internal/infra/peloton/placement/models"
	"code.uber.internal/infra/peloton/placement/offers"
	//"code.uber.internal/infra/peloton/placement/plugins/batch"
	mimir_strategy "code.uber.internal/infra/peloton/placement/plugins/mimir"
	"code.uber.internal/infra/peloton/placement/tasks"
	"code.uber.internal/infra/peloton/storage"
)

const (
	// _noOffersTimeoutPenalty is the timeout value for a get offers request.
	_noOffersTimeoutPenalty = 1 * time.Second
	// _noTasksTimeoutPenalty is the timeout value for a get tasks request.
	_noTasksTimeoutPenalty = 1 * time.Second
)

// Engine represents a placement engine that can be started and stopped.
type Engine interface {
	Start()
	Stop()
}

// New creates a new placement engine having one dedicated coordinator per task type.
func New(dispatcher *yarpc.Dispatcher, parent tally.Scope, cfg *config.PlacementConfig, resMgrClientName string,
	hostMgrClientName string, taskStore storage.TaskStore) Engine {
	resourceManager := resmgrsvc.NewResourceManagerServiceYARPCClient(dispatcher.ClientConfig(resMgrClientName))
	hostManager := hostsvc.NewInternalHostServiceYARPCClient(dispatcher.ClientConfig(hostMgrClientName))
	tallyMetrics := tally_metrics.NewMetrics(parent.SubScope("placement"))
	offerService := offers.NewService(hostManager, resourceManager, tallyMetrics)
	taskService := tasks.NewService(resourceManager, cfg, tallyMetrics)
	scope := tally_metrics.NewMetrics(parent.SubScope(strings.ToLower(cfg.TaskType.String())))
	var strategy plugins.Strategy
	switch cfg.Strategy {
	case config.Batch:
		strategy = batch.New()
	case config.Mimir:
		cfg.Concurrency = 1
		deriver := metrics.NewDeriver([]metrics.FreeMetricTuple{
			{metrics.CPUFree, metrics.CPUUsed, metrics.CPUTotal},
			{metrics.MemoryFree, metrics.MemoryUsed, metrics.MemoryTotal},
			{metrics.DiskFree, metrics.DiskUsed, metrics.DiskTotal},
			{metrics.GPUFree, metrics.GPUUsed, metrics.GPUTotal},
			{metrics.PortsFree, metrics.PortsUsed, metrics.PortsTotal},
		})
		placer := mimir.NewPlacer(deriver)
		strategy = mimir_strategy.New(placer, cfg)
	}
	engine := NewEngine(cfg, offerService, taskService, strategy, async.NewPool(async.PoolOptions{
		MaxWorkers: cfg.Concurrency,
	}), scope)
	return engine
}

// NewEngine will creates a new placement engine.
func NewEngine(config *config.PlacementConfig, offerService offers.Service, taskService tasks.Service,
	strategy plugins.Strategy, pool *async.Pool, scope *tally_metrics.Metrics) Engine {
	result := &engine{
		config:       config,
		offerService: offerService,
		taskService:  taskService,
		strategy:     strategy,
		pool:         pool,
		metrics:      scope,
	}
	result.daemon = async.NewDaemon("Placement Engine", result)
	return result
}

type engine struct {
	config       *config.PlacementConfig
	metrics      *tally_metrics.Metrics
	pool         *async.Pool
	offerService offers.Service
	taskService  tasks.Service
	strategy     plugins.Strategy
	daemon       async.Daemon
}

func (e *engine) Start() {
	e.daemon.Start()
	e.metrics.Running.Update(1)
}

func (e *engine) Run(ctx context.Context) error {
	timer := time.NewTimer(time.Duration(0))
	for {
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
		delay := e.Place(ctx)
		timer.Reset(delay)
	}
}

func (e *engine) Stop() {
	e.daemon.Stop()
	e.metrics.Running.Update(0)
}

func (e *engine) getTaskIDs(tasks []*models.Task) []*peloton.TaskID {
	var taskIDs []*peloton.TaskID
	for _, task := range tasks {
		taskIDs = append(taskIDs, task.GetTask().Id)
	}
	return taskIDs
}

func (e *engine) assignPorts(offer *models.Host, tasks []*models.Task) []uint32 {
	availablePortRanges := map[*models.PortRange]struct{}{}
	for _, resource := range offer.GetOffer().GetResources() {
		if resource.GetName() != "ports" {
			continue
		}
		for _, portRange := range resource.GetRanges().GetRange() {
			availablePortRanges[models.NewPortRange(portRange)] = struct{}{}
		}
	}
	var selectedPorts []uint32
	for _, taskEntity := range tasks {
		assignedPorts := uint32(0)
		neededPorts := taskEntity.GetTask().NumPorts
		depletedRanges := []*models.PortRange{}
		for portRange := range availablePortRanges {
			ports := portRange.TakePorts(neededPorts - assignedPorts)
			assignedPorts += uint32(len(ports))
			selectedPorts = append(selectedPorts, ports...)
			if portRange.NumPorts() == 0 {
				depletedRanges = append(depletedRanges, portRange)
			}
			if assignedPorts >= neededPorts {
				break
			}
		}
		for _, portRange := range depletedRanges {
			delete(availablePortRanges, portRange)
		}
	}
	return selectedPorts
}

func (e *engine) filterAssignments(now time.Time, assignments []*models.Assignment) (assigned, retryable,
	unassigned []*models.Assignment) {
	for _, assignment := range assignments {
		task := assignment.GetTask()
		if assignment.GetHost() == nil {
			if task.PastDeadline(now) {
				unassigned = append(unassigned, assignment)
				continue
			}
		} else {
			task.IncRounds()
			if task.PastMaxRounds() || task.PastDeadline(now) {
				assigned = append(assigned, assignment)
				continue
			}
		}
		retryable = append(retryable, assignment)
	}
	log.WithFields(log.Fields{
		"failed":    len(unassigned),
		"retryable": len(retryable),
		"success":   len(assigned),
	}).Debug("assignment outcome")
	return assigned, retryable, unassigned
}

// findUsedHosts will find the hosts that are used by the retryable assignments.
func (e *engine) findUsedHosts(retryable []*models.Assignment) []*models.Host {
	offers := map[*models.Host]struct{}{}
	for _, assignment := range retryable {
		if offer := assignment.GetHost(); offer != nil {
			offers[offer] = struct{}{}
		}
	}
	var used []*models.Host
	for offer := range offers {
		used = append(used, offer)
	}
	return used
}

// findUnusedHosts will find the hosts that are unused by the assigned and retryable assignments.
func (e *engine) findUnusedHosts(assigned, retryable []*models.Assignment,
	hosts []*models.Host) []*models.Host {
	assignments := make([]*models.Assignment, 0, len(assigned)+len(retryable))
	assignments = append(assignments, assigned...)
	assignments = append(assignments, retryable...)

	// For each offer determine if any tasks where assigned to it.
	usedOffers := map[*models.Host]struct{}{}
	for _, placement := range assignments {
		offer := placement.GetHost()
		if offer == nil {
			continue
		}
		usedOffers[offer] = struct{}{}
	}
	// Find the unused hosts
	unusedOffers := []*models.Host{}
	for _, offer := range hosts {
		if _, used := usedOffers[offer]; !used {
			unusedOffers = append(unusedOffers, offer)
		}
	}
	return unusedOffers
}

func (e *engine) createPlacement(assigned []*models.Assignment) []*resmgr.Placement {
	createPlacementStart := time.Now()
	// For each offer find all tasks assigned to it.
	offersToTasks := map[*models.Host][]*models.Task{}
	for _, placement := range assigned {
		task := placement.GetTask()
		offer := placement.GetHost()
		if offer == nil {
			continue
		}
		if _, exists := offersToTasks[offer]; !exists {
			offersToTasks[offer] = []*models.Task{}
		}
		offersToTasks[offer] = append(offersToTasks[offer], task)
	}

	// For each offer create a placement with all the tasks assigned to it.
	resPlacements := []*resmgr.Placement{}
	for offer, tasks := range offersToTasks {
		taskIDs := e.getTaskIDs(tasks)
		selectedPorts := e.assignPorts(offer, tasks)
		placement := &resmgr.Placement{
			Hostname: offer.GetOffer().Hostname,
			AgentId:  offer.GetOffer().AgentId,
			Type:     e.config.TaskType,
			Tasks:    taskIDs,
			Ports:    selectedPorts,
		}
		resPlacements = append(resPlacements, placement)
	}
	createPlacementDuration := time.Since(createPlacementStart)
	e.metrics.CreatePlacementDuration.Record(createPlacementDuration)
	return resPlacements
}

func (e *engine) cleanup(ctx context.Context, assigned, retryable, unassigned []*models.Assignment,
	offers []*models.Host) {
	// Create the resource manager placements.
	resPlacements := e.createPlacement(assigned)
	e.taskService.SetPlacements(ctx, resPlacements)

	// Return tasks that failed to get placed.
	e.taskService.Enqueue(ctx, unassigned)

	// Find the unused offers.
	unusedOffers := e.findUnusedHosts(assigned, retryable, offers)

	// Release the unused offers.
	e.offerService.Release(ctx, unusedOffers)
}

func (e *engine) pastDeadline(now time.Time, assignments []*models.Assignment) bool {
	for _, assignment := range assignments {
		if !assignment.GetTask().PastDeadline(now) {
			return false
		}
	}
	return true
}

func (e *engine) placeAssignmentGroup(ctx context.Context, filter *hostsvc.HostFilter, assignments []*models.Assignment) {
	log.WithFields(log.Fields{
		"filter":      filter,
		"assignments": assignments,
	}).Debug("placing assignment group")
	for len(assignments) > 0 {
		// Try and get some hosts
		hosts := e.offerService.Acquire(
			ctx, e.config.FetchOfferTasks, e.config.TaskType, filter)
		existing := e.findUsedHosts(assignments)
		now := time.Now()
		for !e.pastDeadline(now, assignments) && len(hosts)+len(existing) == 0 {
			time.Sleep(_noOffersTimeoutPenalty)
			hosts = e.offerService.Acquire(
				ctx, e.config.FetchOfferTasks, e.config.TaskType, filter)
			now = time.Now()
		}

		// Add any hosts still assigned to any task so the offers will eventually be returned or used in a placement.
		hosts = append(hosts, existing...)

		// We where starved from hosts
		if len(hosts) == 0 {
			log.WithFields(log.Fields{
				"filter":      filter,
				"assignments": assignments,
			}).Warn("failed to place tasks due to offer starvation")
			e.metrics.OfferStarved.Inc(1)
			// Return the tasks
			e.taskService.Enqueue(ctx, assignments)
			return
		}
		e.metrics.OfferGet.Inc(1)

		// PlaceOnce the tasks on the hosts by delegating to the placement strategy.
		e.strategy.PlaceOnce(assignments, hosts)

		// Filter the assignments according to if they got assigned, should be retried or where unassigned.
		assigned, retryable, unassigned := e.filterAssignments(time.Now(), assignments)

		// We will retry the retryable tasks
		assignments = retryable

		log.WithFields(log.Fields{
			"filter":     filter,
			"assigned":   assigned,
			"retryable":  retryable,
			"unassigned": unassigned,
			"hosts":      hosts,
		}).Debug("placed assignment group")
		// Set placements and return unused offers and failed tasks
		e.cleanup(ctx, assigned, retryable, unassigned, hosts)
	}
}

// Place will let the coordinator do one placement round.
func (e *engine) Place(ctx context.Context) time.Duration {
	// Try and get some tasks/assignments
	assignments := e.taskService.Dequeue(
		ctx, e.config.TaskType, e.config.TaskDequeueLimit, e.config.TaskDequeueTimeOut)
	if len(assignments) == 0 {
		return _noTasksTimeoutPenalty
	}

	filters := e.strategy.Filters(assignments)
	for f, b := range filters {
		filter, batch := f, b
		// Run the placement of each batch in parallel
		e.pool.Enqueue(async.JobFunc(func(context.Context) {
			e.placeAssignmentGroup(ctx, filter, batch)
		}))
	}

	if !e.strategy.ConcurrencySafe() {
		// Wait for all batches to be processed
		e.pool.WaitUntilProcessed()
	}

	return time.Duration(0)
}
