package respool

import (
	"container/list"
	"math"
	"sync"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common"
	rc "code.uber.internal/infra/peloton/resmgr/common"
	"code.uber.internal/infra/peloton/resmgr/queue"
	"code.uber.internal/infra/peloton/resmgr/scalar"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

// node represents a node in a tree
type node interface {
	// Returns the resource pool name.
	Name() string
	// Returns the resource pool ID.
	ID() string
	// Returns the resource pool's parent.
	Parent() ResPool
	// Sets the parent of the resource pool.
	SetParent(ResPool)
	// Returns the children(if any) of the resource pool.
	Children() *list.List
	// Sets the children of the resource pool.
	SetChildren(*list.List)
	// Returns true of the resource pool is a leaf.
	IsLeaf() bool
	// GetPath returns the resource pool path.
	GetPath() string
	// IsRoot returns true if the node is the root of the resource tree.
	IsRoot() bool
}

// ResPool is a node in a resource pool hierarchy.
type ResPool interface {
	node

	// Returns the config of the resource pool.
	ResourcePoolConfig() *respool.ResourcePoolConfig
	// Sets the resource pool config.
	SetResourcePoolConfig(*respool.ResourcePoolConfig)

	// Returns a map of resources and its resource config.
	Resources() map[string]*respool.ResourceConfig

	// Converts to resource pool info.
	ToResourcePoolInfo() *respool.ResourcePoolInfo

	// Aggregates the child reservations by resource type.
	AggregatedChildrenReservations() (map[string]float64, error)

	// Enqueues gang (task list) into resource pool pending queue.
	EnqueueGang(gang *resmgrsvc.Gang) error
	// Dequeues gangs (task list) from the resource pool.
	DequeueGangs(int) ([]*resmgrsvc.Gang, error)
	// PeekGangs returns a list of gangs from the resource pool's queue based
	// on the queue type. limit determines the max number of gangs to be
	// returned.
	PeekGangs(qt QueueType, limit uint32) ([]*resmgrsvc.Gang, error)

	// SetEntitlement sets the entitlement for the resource pool.
	// input is map[ResourceKind]->EntitledCapacity
	SetEntitlement(map[string]float64)
	// SetEntitlementResources sets the entitlement for
	// the resource pool based on the resources.
	SetEntitlementResources(res *scalar.Resources)
	// SetEntitlementByKind sets the entitlement of the respool
	// by kind.
	SetEntitlementByKind(kind string, entitlement float64)
	//GetEntitlement gets the entitlement for the resource pool.
	GetEntitlement() *scalar.Resources

	// AddToAllocation adds resources to current allocation
	// for the resource pool.
	AddToAllocation(*scalar.Allocation) error
	// SubtractFromAllocation recaptures the resources from task.
	SubtractFromAllocation(*scalar.Allocation) error

	// GetTotalAllocatedResources returns the total resource allocation for the resource
	// pool.
	GetTotalAllocatedResources() *scalar.Resources
	// CalculateTotalAllocatedResources calculates the total allocation recursively for
	// all the children.
	CalculateTotalAllocatedResources() *scalar.Resources
	// SetAllocation sets the resource allocation for the resource pool.
	// Used by tests.
	SetTotalAllocatedResources(resources *scalar.Resources)

	// AddToDemand adds resources to current demand
	// for the resource pool.
	AddToDemand(res *scalar.Resources) error
	// SubtractFromDemand subtracts resources from current demand
	// for the resource pool.
	SubtractFromDemand(res *scalar.Resources) error
	// GetDemand returns the resource demand for the resource pool.
	GetDemand() *scalar.Resources
	// CalculateDemand calculates the resource demand
	// for the resource pool recursively for the subtree.
	CalculateDemand() *scalar.Resources

	// AddInvalidTask will add the killed tasks to respool which can be
	// discarded asynchronously which scheduling.
	AddInvalidTask(task *peloton.TaskID)
}

// resPool implements the ResPool interface.
type resPool struct {
	sync.RWMutex

	id       string
	path     string
	children *list.List
	parent   ResPool

	preemptionCfg rc.PreemptionConfig

	resourceConfigs map[string]*respool.ResourceConfig
	poolConfig      *respool.ResourcePoolConfig

	// Tracks the allocation across different task dimensions
	allocation *scalar.Allocation
	// Tracks the max resources this resource pool can use in a given
	// entitlement cycle
	entitlement *scalar.Resources
	// Tracks the demand of resources, in the pending and the controller queue,
	// which are waiting to be admitted.
	// Once admitted their resources are accounted for in `allocation`.
	demand *scalar.Resources
	// the reserved resources of this pool
	reservation *scalar.Resources

	// queue containing gangs waiting to be admitted into the resource pool.
	// queue semantics is defined by the SchedulingPolicy
	pendingQueue queue.Queue
	// queue containing controller tasks(
	// gang with 1 task) waiting to be admitted.
	// All tasks are enqueued to the pending queue,
	// the only reason a task would move from pending queue to controller
	// queue is when the task is of type CONTROLLER and it can't be admitted,
	// in that case the task is moved to the controller queue so that it
	// doesn't block the rest of the tasks in pending queue.
	controllerQueue queue.Queue
	// queue containing non-preemptible gangs, waiting to be admitted.
	// All tasks are enqueued to the pending queue,
	// the only reason a task would move from pending queue to np
	// queue is when the task is non-preemptible and it can't be admitted,
	// in that case the task is moved to the np queue so that it
	// doesn't block the rest of the tasks in pending queue.
	npQueue queue.Queue

	// The max limit of resources controller tasks can use in this pool
	controllerLimit *scalar.Resources

	// set of invalid tasks which will be discarded during admission control.
	invalidTasks map[string]bool

	metrics *Metrics
}

// NewRespool will initialize the resource pool node and return that.
func NewRespool(
	scope tally.Scope,
	id string,
	parent ResPool,
	config *respool.ResourcePoolConfig,
	preemptionConfig rc.PreemptionConfig) (ResPool, error) {

	if config == nil {
		return nil, errors.Errorf("error creating resource pool %s; "+
			"ResourcePoolConfig is nil", id)
	}

	pq, err := queue.CreateQueue(config.Policy, math.MaxInt64)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating resource pool %s", id)
	}

	cq, err := queue.CreateQueue(config.Policy, math.MaxInt64)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating resource pool %s", id)
	}

	nq, err := queue.CreateQueue(config.Policy, math.MaxInt64)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating resource pool %s", id)
	}

	pool := &resPool{
		id:              id,
		children:        list.New(),
		parent:          parent,
		resourceConfigs: make(map[string]*respool.ResourceConfig),
		poolConfig:      config,
		pendingQueue:    pq,
		controllerQueue: cq,
		npQueue:         nq,
		allocation:      scalar.NewAllocation(),
		entitlement:     &scalar.Resources{},
		demand:          &scalar.Resources{},
		reservation:     &scalar.Resources{},
		invalidTasks:    make(map[string]bool),
		preemptionCfg:   preemptionConfig,
	}

	// Initialize metrics
	pool.metrics = NewMetrics(scope.Tagged(map[string]string{
		"path": pool.GetPath(),
	}))

	// Initialize resources and limits.
	pool.initialize(config)
	pool.updateResourceMetrics()
	pool.metrics.PendingQueueSize.Update(float64(pool.pendingQueue.Size()))

	return pool, nil
}

// ID returns the resource pool UUID.
func (n *resPool) ID() string {
	return n.id
}

// Name returns the resource pool name.
func (n *resPool) Name() string {
	return n.poolConfig.Name
}

// Parent returns the resource pool's parent pool.
func (n *resPool) Parent() ResPool {
	n.RLock()
	defer n.RUnlock()
	return n.parent
}

// SetParent will be setting the parent for the resource pool.
func (n *resPool) SetParent(parent ResPool) {
	n.Lock()
	n.parent = parent
	// reset path to be calculated again.
	n.path = ""
	n.Unlock()
}

// SetChildren will be setting the children for the resource pool.
func (n *resPool) SetChildren(children *list.List) {
	n.Lock()
	n.children = children
	n.Unlock()
}

// Children will be getting the children for the resource pool.
func (n *resPool) Children() *list.List {
	n.RLock()
	defer n.RUnlock()
	return n.children
}

// Resources returns the resource configs.
func (n *resPool) Resources() map[string]*respool.ResourceConfig {
	n.RLock()
	defer n.RUnlock()
	return n.resourceConfigs
}

// SetResourcePoolConfig sets the resource pool config and initializes the
// resources.
func (n *resPool) SetResourcePoolConfig(config *respool.ResourcePoolConfig) {
	n.Lock()
	defer n.Unlock()
	n.poolConfig = config
	n.initialize(config)
}

// ResourcePoolConfig returns the resource pool config.
func (n *resPool) ResourcePoolConfig() *respool.ResourcePoolConfig {
	n.RLock()
	defer n.RUnlock()
	return n.poolConfig
}

// IsLeaf will tell us if this resource pool is leaf or not
func (n *resPool) IsLeaf() bool {
	n.RLock()
	defer n.RUnlock()
	return n.isLeaf()
}

// EnqueueGang inserts a gang, which is a task list representing a gang
// of 1 or more (same priority) tasks, into pending queue.
func (n *resPool) EnqueueGang(gang *resmgrsvc.Gang) error {
	if (gang == nil) || (len(gang.Tasks) == 0) {
		err := errors.Errorf("gang has no elements")
		return err
	}
	if n.isLeaf() {
		err := n.pendingQueue.Enqueue(gang)
		return err
	}
	n.metrics.PendingQueueSize.Update(float64(n.pendingQueue.Size()))
	err := errors.Errorf("resource pool %s is not a leaf node", n.id)
	return err
}

// DequeueGangs dequeues a list of gangs from the
// resource pool which can be admitted.
// Each gang is a task list representing a gang
// of 1 or more (same priority) tasks.
// If there is no gang present then its a error, if any one present
// then it will return all the gangs within limit without error
// The dequeue works in the following way
// 1. Checks the non preemptible queue to admit non preemptible gangs
// 2. Checks the controller queue to admit controller preemptible gangs
// 3. Checks the pending queue (
//    and moves gangs to non preemptible queue or controller queue)
func (n *resPool) DequeueGangs(limit int) ([]*resmgrsvc.Gang, error) {
	if limit <= 0 {
		err := errors.Errorf("limit %d is not valid", limit)
		return nil, err
	}

	if !n.isLeaf() {
		err := errors.Errorf("resource pool %s is not a leaf node", n.id)
		return nil, err
	}

	var err error
	var gangList []*resmgrsvc.Gang

	for _, qt := range []QueueType{NonPreemptibleQueue, ControllerQueue, PendingQueue} {
		// check how many gangs left from the limit
		left := limit - len(gangList)
		if left == 0 {
			break
		}
		gangs, err := n.dequeue(qt, left)
		if err != nil {
			log.WithFields(log.Fields{
				"respool_id": n.id,
				"queue":      qt,
				"gangs_left": left,
			}).WithError(err).
				Warn("Error dequeueing from queue")
			continue
		}
		gangList = append(gangList, gangs...)
	}

	n.metrics.PendingQueueSize.Update(float64(n.pendingQueue.Size()))
	return gangList, err
}

// dequeues limit number of gangs from the respool for admission.
func (n *resPool) dequeue(qt QueueType, limit int) ([]*resmgrsvc.Gang,
	error) {

	var err error
	var gangList []*resmgrsvc.Gang

	if limit <= 0 {
		return gangList, nil
	}

	for i := 0; i < limit; i++ {
		gangs, err := n.queue(qt).Peek(1)
		if err != nil {
			if _, ok := err.(queue.ErrorQueueEmpty); ok {
				// queue is empty we are done
				return gangList, nil
			}
			log.WithError(err).
				Error("Failed to peek into queue")
			return gangList, err
		}
		gang := gangs[0]
		err = admission.TryAdmit(gang, n, qt)
		if err != nil {
			if err == errGangInvalid || err == errSkipNonPreemptibleGang ||
				err == errSkipControllerGang {
				// the admission can fail  :
				// 1. Because the gang is invalid.
				// In this case we move on to the next gang in the queue with the
				// expectation that the invalid gang is removed from the head of
				// the queue.
				// 2. Because the gang should be skipped (
				// controller gang or non-preemptible gang)
				log.WithFields(log.Fields{
					"respool_id": n.id,
					"error":      err.Error(),
				}).Debug("skipping gang from admission")
				continue
			}
			break
		}
		gangList = append(gangList, gang)
	}
	return gangList, err
}

// AggregatedChildrenReservations returns aggregated child reservations by resource kind
func (n *resPool) AggregatedChildrenReservations() (map[string]float64, error) {
	totalReservation := make(map[string]float64)
	n.RLock()
	defer n.RUnlock()
	nodes := n.Children()
	// We need to find out the total reservation
	for e := nodes.Front(); e != nil; e = e.Next() {
		n, ok := e.Value.(ResPool)
		if !ok {
			return totalReservation, errors.Errorf("failed to type assert child resource pool %v",
				e.Value)
		}
		resources := n.Resources()
		for kind, resource := range resources {
			totalReservation[kind] =
				totalReservation[kind] + resource.Reservation
		}
	}
	return totalReservation, nil
}

// ToResourcePoolInfo converts ResPool to ResourcePoolInfo
func (n *resPool) ToResourcePoolInfo() *respool.ResourcePoolInfo {
	n.RLock()
	defer n.RUnlock()
	childrenResPools := n.children
	childrenResourcePoolIDs := make([]*peloton.ResourcePoolID, 0, childrenResPools.Len())
	for child := childrenResPools.Front(); child != nil; child = child.Next() {
		childrenResourcePoolIDs = append(childrenResourcePoolIDs, &peloton.ResourcePoolID{
			Value: child.Value.(*resPool).id,
		})
	}

	var parentResPoolID *peloton.ResourcePoolID

	// handle Root's parent == nil
	if n.parent != nil {
		parentResPoolID = &peloton.ResourcePoolID{
			Value: n.parent.ID(),
		}
	}

	return &respool.ResourcePoolInfo{
		Id: &peloton.ResourcePoolID{
			Value: n.id,
		},
		Parent:   parentResPoolID,
		Config:   n.poolConfig,
		Children: childrenResourcePoolIDs,
		Usage:    n.createRespoolUsage(n.CalculateTotalAllocatedResources()),
	}
}

// CalculateTotalAllocatedResources calculates the total allocation
// recursively for all the children.
func (n *resPool) CalculateTotalAllocatedResources() *scalar.Resources {
	return n.calculateAllocation().GetByType(scalar.TotalAllocation)
}

// calculateAllocation calculates the allocation recursively for
// all the children.
func (n *resPool) calculateAllocation() *scalar.Allocation {
	if n.isLeaf() {
		return n.allocation
	}
	allocation := scalar.NewAllocation()
	for child := n.children.Front(); child != nil; child = child.Next() {
		if childResPool, ok := child.Value.(*resPool); ok {
			allocation = allocation.Add(
				childResPool.calculateAllocation())
		}
	}
	n.allocation = allocation
	return allocation
}

// CalculateAndSetDemand calculates and sets the resource demand
// for the resource pool recursively for the subtree
func (n *resPool) CalculateDemand() *scalar.Resources {
	if n.isLeaf() {
		return n.demand
	}
	demand := &scalar.Resources{}
	for child := n.children.Front(); child != nil; child = child.Next() {
		if childResPool, ok := child.Value.(*resPool); ok {
			demand = demand.Add(
				childResPool.CalculateDemand())
		}
	}
	n.demand = demand
	return demand
}

// getQueue returns the queue depending on the queue type
func (n *resPool) queue(qt QueueType) queue.Queue {
	switch qt {
	case ControllerQueue:
		return n.controllerQueue
	case NonPreemptibleQueue:
		return n.npQueue
	case PendingQueue:
		return n.pendingQueue
	}

	// should never come here
	return nil
}

func (n *resPool) createRespoolUsage(allocation *scalar.Resources) []*respool.ResourceUsage {
	resUsage := make([]*respool.ResourceUsage, 0, 4)
	ru := &respool.ResourceUsage{
		Kind:       common.CPU,
		Allocation: allocation.CPU,
		// Hard coding until we have real slack
		Slack: float64(0),
	}
	resUsage = append(resUsage, ru)
	ru = &respool.ResourceUsage{
		Kind:       common.GPU,
		Allocation: allocation.GPU,
		// Hard coding until we have real slack
		Slack: float64(0),
	}
	resUsage = append(resUsage, ru)
	ru = &respool.ResourceUsage{
		Kind:       common.MEMORY,
		Allocation: allocation.MEMORY,
		// Hard coding until we have real slack
		Slack: float64(0),
	}
	resUsage = append(resUsage, ru)
	ru = &respool.ResourceUsage{
		Kind:       common.DISK,
		Allocation: allocation.DISK,
		// Hard coding until we have real slack
		Slack: float64(0),
	}
	resUsage = append(resUsage, ru)
	return resUsage
}

func (n *resPool) isLeaf() bool {
	return n.children.Len() == 0
}

// initializes the resources and limits for this pool
// NB: The function calling initResources should acquire the lock
func (n *resPool) initialize(cfg *respool.ResourcePoolConfig) {
	n.initResConfig(cfg)
	n.initControllerLimit(cfg)
	n.initReservation(cfg)
}

// initializes the reserved resources
func (n *resPool) initReservation(cfg *respool.ResourcePoolConfig) {
	for kind, res := range n.resourceConfigs {
		switch kind {
		case common.CPU:
			n.reservation.CPU = res.Reservation
		case common.MEMORY:
			n.reservation.MEMORY = res.Reservation
		case common.GPU:
			n.reservation.GPU = res.Reservation
		case common.DISK:
			n.reservation.DISK = res.Reservation
		}
	}
	log.WithField("reservation", n.reservation).
		WithField("respool_id", n.id).
		Info("Setting reservation")
}

func (n *resPool) initResConfig(cfg *respool.ResourcePoolConfig) {
	for _, res := range cfg.Resources {
		n.resourceConfigs[res.Kind] = res
	}
}

func (n *resPool) initControllerLimit(cfg *respool.ResourcePoolConfig) {
	climit := cfg.GetControllerLimit()
	if climit == nil {
		n.controllerLimit = nil
		return
	}

	controllerLimit := &scalar.Resources{}
	multiplier := climit.MaxPercent / 100
	for kind, res := range n.resourceConfigs {
		switch kind {
		case common.CPU:
			controllerLimit.CPU = res.Reservation * multiplier
		case common.MEMORY:
			controllerLimit.MEMORY = res.Reservation * multiplier
		case common.GPU:
			controllerLimit.GPU = res.Reservation * multiplier
		case common.DISK:
			controllerLimit.DISK = res.Reservation * multiplier
		}
	}

	log.WithField("controller_limit", controllerLimit).
		WithField("respool_id", n.id).
		Info("Setting controller limit")
	n.controllerLimit = controllerLimit
}

// ControllerLimit returns the controller limit of the resource pool
func (n *resPool) ControllerLimit() *scalar.Resources {
	return n.controllerLimit
}

// SetControllerLimit sets the controller limit of this resource pool
func (n *resPool) SetControllerLimit(controllerLimit *scalar.Resources) {
	n.controllerLimit = controllerLimit
}

// SetEntitlement sets the entitlement for the resource pool
func (n *resPool) SetEntitlement(entitlement map[string]float64) {
	n.Lock()
	defer n.Unlock()

	if entitlement == nil {
		return
	}

	for kind, capacity := range entitlement {
		switch kind {
		case common.CPU:
			n.entitlement.CPU = capacity
		case common.DISK:
			n.entitlement.DISK = capacity
		case common.GPU:
			n.entitlement.GPU = capacity
		case common.MEMORY:
			n.entitlement.MEMORY = capacity
		}
	}
	log.WithFields(log.Fields{
		"respool_id": n.ID(),
		"cpu":        n.entitlement.CPU,
		"disk":       n.entitlement.DISK,
		"memory":     n.entitlement.MEMORY,
		"gpu":        n.entitlement.GPU,
	}).Debug("Setting Entitlement for Respool")
	n.updateResourceMetrics()
}

// SetEntitlementByKind sets the entitlement for the resource pool
func (n *resPool) SetEntitlementByKind(kind string, entitlement float64) {
	n.Lock()
	defer n.Unlock()
	switch kind {
	case common.CPU:
		n.entitlement.CPU = entitlement
	case common.DISK:
		n.entitlement.DISK = entitlement
	case common.GPU:
		n.entitlement.GPU = entitlement
	case common.MEMORY:
		n.entitlement.MEMORY = entitlement
	}
	log.WithFields(log.Fields{
		"respool_id":  n.ID(),
		"kind":        kind,
		"entitlement": entitlement,
	}).Debug("Setting Entitlement")
	n.updateResourceMetrics()
}

func (n *resPool) SetEntitlementResources(res *scalar.Resources) {
	n.Lock()
	defer n.Unlock()
	n.entitlement = res
	log.WithFields(log.Fields{
		"respool_id":  n.ID(),
		"entitlement": res,
	}).Debug("Setting Entitlement")
	n.updateResourceMetrics()
}

// GetEntitlement gets the entitlement for the resource pool
func (n *resPool) GetEntitlement() *scalar.Resources {
	n.RLock()
	defer n.RUnlock()
	return n.entitlement
}

// GetTotalAllocatedResources gets the resource allocation for the pool
func (n *resPool) GetTotalAllocatedResources() *scalar.Resources {
	n.RLock()
	defer n.RUnlock()
	return n.allocation.GetByType(scalar.TotalAllocation)
}

// SetAllocation sets the resource allocation for the pool
func (n *resPool) SetTotalAllocatedResources(allocation *scalar.Resources) {
	n.Lock()
	defer n.Unlock()
	n.allocation.Value[scalar.TotalAllocation] = allocation
}

// GetDemand gets the resource demand for the pool
func (n *resPool) GetDemand() *scalar.Resources {
	n.RLock()
	defer n.RUnlock()
	return n.demand
}

// SubtractFromAllocation updates the allocation for the resource pool
func (n *resPool) SubtractFromAllocation(allocation *scalar.Allocation) error {
	n.Lock()
	defer n.Unlock()

	newAllocation := n.allocation.Subtract(allocation)

	if newAllocation == nil {
		return errors.Errorf("Couldn't update the resources")
	}
	n.allocation = newAllocation

	log.
		WithField("respool_id", n.ID()).
		WithField("total_alloc", n.allocation.GetByType(scalar.TotalAllocation)).
		WithField("non_preemptible_alloc",
			n.allocation.GetByType(scalar.NonPreemptibleAllocation)).
		WithField("controller_alloc",
			n.allocation.GetByType(scalar.ControllerAllocation)).
		Debug("Current Allocation after subtracting allocation")

	n.updateResourceMetrics()
	return nil
}

// IsRoot returns true if the node is the root in the resource
// pool hierarchy
func (n *resPool) IsRoot() bool {
	return n.ID() == common.RootResPoolID
}

// GetPath returns the fully qualified path of the resource pool
// in the resource pool hierarchy
// For the below resource hierarchy ; the "compute" resource pool would be
// designated by path: /infrastructure/compute
//             root
//               ├─ infrastructure
//               │  └─ compute
//               └─ marketplace
func (n *resPool) GetPath() string {
	n.RLock()
	n.RUnlock()
	if n.path == "" {
		n.path = n.calculatePath()
	}
	return n.path
}

func (n *resPool) calculatePath() string {
	if n.IsRoot() {
		return ResourcePoolPathDelimiter
	}
	if n.parent == nil || n.parent.IsRoot() {
		return ResourcePoolPathDelimiter + n.Name()
	}
	return n.parent.GetPath() + ResourcePoolPathDelimiter + n.Name()
}

// AddToAllocation adds resources to the allocation
// for the resource pool
func (n *resPool) AddToAllocation(allocation *scalar.Allocation) error {
	n.Lock()
	defer n.Unlock()

	n.allocation = n.allocation.Add(allocation)

	log.
		WithField("respool_id", n.ID()).
		WithField("total_alloc", n.allocation.GetByType(scalar.TotalAllocation)).
		WithField("non_preemptible_alloc",
			n.allocation.GetByType(scalar.NonPreemptibleAllocation)).
		WithField("controller_alloc",
			n.allocation.GetByType(scalar.ControllerAllocation)).
		Debug("Current Allocation after adding allocation")

	n.updateResourceMetrics()
	return nil
}

// AddToDemand adds resources to the demand
// for the resource pool
func (n *resPool) AddToDemand(res *scalar.Resources) error {
	n.Lock()
	defer n.Unlock()

	n.demand = n.demand.Add(res)

	log.WithFields(log.Fields{
		"respool_id": n.ID(),
		"demand":     n.demand,
	}).Debug("Current Demand after Adding resources")

	n.updateResourceMetrics()
	return nil
}

// SubtractFromDemand subtracts resources from demand
// for the resource pool
func (n *resPool) SubtractFromDemand(res *scalar.Resources) error {
	n.Lock()
	defer n.Unlock()

	newDemand := n.demand.Subtract(res)
	if newDemand == nil {
		return errors.Errorf("Couldn't update the resources")
	}
	n.demand = newDemand

	log.
		WithField("respool_id", n.ID()).
		WithField("demand", n.demand).
		Debug("Current Demand " +
			"after removing resources")
	n.updateResourceMetrics()
	return nil
}

// updates static metrics(Share, Limit and Reservation) which depend on the config
func (n *resPool) updateStaticResourceMetrics() {
	n.metrics.ResourcePoolShare.Update(getShare(n.resourceConfigs))
	n.metrics.ResourcePoolLimit.Update(getLimits(n.resourceConfigs))
	n.metrics.ResourcePoolReservation.Update(n.reservation)

	if n.controllerLimit != nil {
		n.metrics.ControllerLimit.Update(n.controllerLimit)
	}
}

// updates dynamic metrics(Allocation, Entitlement, Available) which change based on
// usage and entitlement calculation
func (n *resPool) updateDynamicResourceMetrics() {
	n.metrics.Entitlement.Update(n.entitlement)
	n.metrics.TotalAllocation.Update(n.allocation.GetByType(scalar.TotalAllocation))
	n.metrics.NonPreemptibleAllocation.Update(n.allocation.GetByType(scalar.
		NonPreemptibleAllocation))
	n.metrics.ControllerAllocation.Update(n.allocation.GetByType(scalar.
		ControllerAllocation))
	n.metrics.Available.Update(n.entitlement.
		Subtract(n.allocation.GetByType(scalar.TotalAllocation)))
	n.metrics.Demand.Update(n.demand)
}

// updates all the metrics (static and dynamic)
func (n *resPool) updateResourceMetrics() {
	n.updateStaticResourceMetrics()
	n.updateDynamicResourceMetrics()
}

// AddInvalidTask adds an invalid task so that it can
// remove them from the respool later
func (n *resPool) AddInvalidTask(task *peloton.TaskID) {
	n.Lock()
	defer n.Unlock()
	n.invalidTasks[task.Value] = true
}

// PeekGangs returns a list of gangs from the queue based on the queue type.
func (n *resPool) PeekGangs(qt QueueType, limit uint32) ([]*resmgrsvc.Gang,
	error) {
	n.RLock()
	defer n.RUnlock()

	switch qt {
	case PendingQueue:
		return n.pendingQueue.Peek(limit)
	case ControllerQueue:
		return n.controllerQueue.Peek(limit)
	case NonPreemptibleQueue:
		return n.npQueue.Peek(limit)
	}

	// should never come here
	return nil, nil
}

func (n *resPool) isPreemptionEnabled() bool {
	return n.preemptionCfg.Enabled
}

func getLimits(resourceConfigs map[string]*respool.ResourceConfig) *scalar.Resources {
	var resources scalar.Resources
	for kind, res := range resourceConfigs {
		switch kind {
		case common.CPU:
			resources.CPU = res.Limit
		case common.GPU:
			resources.GPU = res.Limit
		case common.MEMORY:
			resources.MEMORY = res.Limit
		case common.DISK:
			resources.DISK = res.Limit
		}
	}
	return &resources
}

func getShare(resourceConfigs map[string]*respool.ResourceConfig) *scalar.Resources {
	var resources scalar.Resources
	for kind, res := range resourceConfigs {
		switch kind {
		case common.CPU:
			resources.CPU = res.Share
		case common.GPU:
			resources.GPU = res.Share
		case common.MEMORY:
			resources.MEMORY = res.Share
		case common.DISK:
			resources.DISK = res.Share
		}
	}
	return &resources
}
