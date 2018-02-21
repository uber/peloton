package respool

import (
	"container/list"
	"math"
	"sync"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common"
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
	// Sets the resource config.
	SetResourceConfig(*respool.ResourceConfig)

	// Converts to resource pool info.
	ToResourcePoolInfo() *respool.ResourcePoolInfo

	// Aggregates the child reservations by resource type.
	AggregatedChildrenReservations() (map[string]float64, error)

	// Forms a gang from a single task.
	MakeTaskGang(task *resmgr.Task) *resmgrsvc.Gang
	// Enqueues gang (task list) into resource pool pending queue.
	EnqueueGang(gang *resmgrsvc.Gang) error
	// Dequeues gang (task list) list from the resource pool.
	DequeueGangList(int) ([]*resmgrsvc.Gang, error)

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

	// GetAllocation returns the total resource allocation for the resource
	// pool.
	GetAllocation() *scalar.Resources
	// SetAllocation sets the resource allocation for the resource pool.
	SetAllocation(res *scalar.Resources)
	// SubtractFromAllocation recaptures the resources from task.
	SubtractFromAllocation(preemptible bool, res *scalar.Resources) error
	// AddToAllocation adds resources to current allocation
	// for the resource pool.
	AddToAllocation(preemptible bool, res *scalar.Resources) error
	// CalculateAllocation calculates the allocation recursively for
	// all the children.
	CalculateAllocation() *scalar.Resources

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
	// PeekPendingTasks returns the list of gangs which are in the
	// resource pools pending queue or an error.
	// limit determines the number of gangs to be returned.
	PeekPendingGangs(limit uint32) ([]*resmgrsvc.Gang, error)
}

// resPool implements the ResPool interface.
type resPool struct {
	sync.RWMutex

	id       string
	path     string
	children *list.List
	parent   ResPool

	resourceConfigs map[string]*respool.ResourceConfig
	poolConfig      *respool.ResourcePoolConfig

	entitlement *scalar.Resources
	totalAlloc  *scalar.Resources
	demand      *scalar.Resources

	// tracks the total allocation of non-preemptible tasks in this resource
	// pool by their type. Used for admissionController control of non-preemptible
	// tasks inside this resource pool.
	nonPreemptibleAlloc *scalar.Resources

	// set of invalid tasks which should be discarded during admissionController
	// control
	invalidTasks map[string]bool

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

	metrics *Metrics
}

// NewRespool will initialize the resource pool node and return that.
func NewRespool(
	scope tally.Scope,
	ID string,
	parent ResPool,
	config *respool.ResourcePoolConfig) (ResPool, error) {

	if config == nil {
		return nil, errors.Errorf("error creating resource pool %s; "+
			"ResourcePoolConfig is nil", ID)
	}

	pq, err := queue.CreateQueue(config.Policy, math.MaxInt64)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating resource pool %s", ID)
	}

	cq, err := queue.CreateQueue(config.Policy, math.MaxInt64)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating resource pool %s", ID)
	}

	pool := &resPool{
		id:                  ID,
		children:            list.New(),
		parent:              parent,
		resourceConfigs:     make(map[string]*respool.ResourceConfig),
		poolConfig:          config,
		pendingQueue:        pq,
		controllerQueue:     cq,
		entitlement:         &scalar.Resources{},
		totalAlloc:          &scalar.Resources{},
		demand:              &scalar.Resources{},
		nonPreemptibleAlloc: &scalar.Resources{},
		invalidTasks:        make(map[string]bool),
	}

	// Initialize metrics
	pool.metrics = NewMetrics(scope.Tagged(map[string]string{
		"path": pool.GetPath(),
	}))

	// Initialize resources.
	pool.initResources(config.GetResources())
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

// SetResourceConfig will set the resource poolConfig for one kind of resource.
func (n *resPool) SetResourceConfig(resources *respool.ResourceConfig) {
	n.Lock()
	defer n.Unlock()
	n.initResources([]*respool.ResourceConfig{resources})
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
	n.initResources(config.GetResources())
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

// MakeTaskGang forms a gang from a single task
func (n *resPool) MakeTaskGang(task *resmgr.Task) *resmgrsvc.Gang {
	var gang resmgrsvc.Gang
	gang.Tasks = append(gang.Tasks, task)
	return &gang
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

// DequeueGangList dequeues a list of gangs from the
// resource pool which can be admitted.
// Each gang is a task list representing a gang
// of 1 or more (same priority) tasks.
// If there is no gang present then its a error, if any one present
// then it will return all the gangs within limit without error
// The dequeue works in the following way
// 1. First checks the controller queue to admit controller jobs
// 2. Checks the pending queue
func (n *resPool) DequeueGangList(limit int) ([]*resmgrsvc.Gang, error) {
	if limit <= 0 {
		err := errors.Errorf("limit %d is not valid", limit)
		return nil, err
	}
	if !n.isLeaf() {
		err := errors.Errorf("resource pool %s is not a leaf node", n.id)
		return nil, err
	}

	var gangList []*resmgrsvc.Gang

	// In the first pass we check the controller queue to admit as many
	// controller tasks as we can satisfy with the controller quota of the
	// resource pool.
	cgs, err := n.dequeueController(limit)
	if err != nil {
		log.WithError(err).Warn("Error dequeueing from controller queue")
	}
	gangList = append(gangList, cgs...)

	// In the second pass we check the pending queue.
	// If the task at the head of the queue is a controller task and can't be
	// admitted with the controller quota,
	// we will move it to the controller queue so that it doesn't block the
	// rest of the tasks. That controller task will be picked up from the
	// controller queue in the next scheduling cycle.	left := len(gangList)
	left := limit - len(gangList)
	pgs, err := n.dequeuePending(left)
	if err != nil {
		log.WithError(err).Warn("Error dequeueing from pending queue")
	}
	gangList = append(gangList, pgs...)

	n.metrics.PendingQueueSize.Update(float64(n.pendingQueue.Size()))
	return gangList, err
}

func (n *resPool) dequeuePending(limit int) ([]*resmgrsvc.Gang,
	error) {

	var err error
	var gangList []*resmgrsvc.Gang

	for i := 0; i < limit; i++ {
		gangs, err := n.pendingQueue.Peek(1)
		if err != nil {
			if _, ok := err.(queue.ErrorQueueEmpty); ok {
				// pending queue is empty we are done
				return gangList, nil
			}
			log.WithError(err).
				Error("Failed to peek into pending queue")
			return gangList, err
		}
		gang := gangs[0]
		ok, err := pending.TryAdmit(gang, n)
		if err != nil {
			if err != errGangInvalid {
				log.WithField("respool_id", n.ID()).
					WithError(err).Error(
					"Failed to admit tasks from controller queue")
				break
			}
			continue
		}
		if ok {
			gangList = append(gangList, gang)
			continue
		}
	}
	return gangList, err
}

func (n *resPool) dequeueController(
	limit int) ([]*resmgrsvc.Gang, error) {

	var err error
	var gangList []*resmgrsvc.Gang
	for i := 0; i < limit; i++ {
		gangs, err := n.controllerQueue.Peek(1)
		if err != nil {
			if _, ok := err.(queue.ErrorQueueEmpty); ok {
				// queue is empty
				break
			}
			log.WithError(err).Error(
				"Failed to peek into controller queue")
			break
		}
		gang := gangs[0]

		ok, err := controller.TryAdmit(gang, n)
		if err != nil {
			log.WithField("respool_id", n.ID()).
				WithError(err).Error(
				"Failed to admit tasks from controller queue")
			break
		}
		if !ok {
			// can't admit any more gangs
			log.WithField("respool_id", n.ID()).
				Debug("Resource pool quota for controller tasks is full")
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
		Usage:    n.createRespoolUsage(n.CalculateAllocation()),
	}
}

// calculateAllocation calculates the total allocation recursively for
// all the children.
func (n *resPool) CalculateAllocation() *scalar.Resources {
	if n.isLeaf() {
		return n.totalAlloc
	}
	allocation := &scalar.Resources{}
	for child := n.children.Front(); child != nil; child = child.Next() {
		if childResPool, ok := child.Value.(*resPool); ok {
			allocation = allocation.Add(
				childResPool.CalculateAllocation())
		}
	}
	n.totalAlloc = allocation
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

func (n *resPool) createRespoolUsage(allocation *scalar.Resources) []*respool.ResourceUsage {
	resUsage := make([]*respool.ResourceUsage, 0, 4)
	ru := &respool.ResourceUsage{
		Kind:       common.CPU,
		Allocation: allocation.CPU,
		// Hardcoding until we have real slack
		Slack: float64(0),
	}
	resUsage = append(resUsage, ru)
	ru = &respool.ResourceUsage{
		Kind:       common.GPU,
		Allocation: allocation.GPU,
		// Hardcoding until we have real slack
		Slack: float64(0),
	}
	resUsage = append(resUsage, ru)
	ru = &respool.ResourceUsage{
		Kind:       common.MEMORY,
		Allocation: allocation.MEMORY,
		// Hardcoding until we have real slack
		Slack: float64(0),
	}
	resUsage = append(resUsage, ru)
	ru = &respool.ResourceUsage{
		Kind:       common.DISK,
		Allocation: allocation.DISK,
		// Hardcoding until we have real slack
		Slack: float64(0),
	}
	resUsage = append(resUsage, ru)
	return resUsage
}

func (n *resPool) isLeaf() bool {
	return n.children.Len() == 0
}

// initResources will initializing the resource poolConfig under resource pool
// NB: The function calling initResources should acquire the lock
func (n *resPool) initResources(resList []*respool.ResourceConfig) {
	for _, res := range resList {
		n.resourceConfigs[res.Kind] = res
	}
}

// logNodeResources will be printing the resources for the resource pool
func (n *resPool) logNodeResources() {
	n.RLock()
	defer n.RLock()
	for kind, res := range n.resourceConfigs {
		log.WithFields(log.Fields{
			"kind":        kind,
			"limit":       res.Limit,
			"reservation": res.Reservation,
			"share":       res.Share,
		}).Info("ResPool Resources for ResPool", n.id)
	}
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

// GetAllocation gets the resource allocation for the pool
func (n *resPool) GetAllocation() *scalar.Resources {
	n.RLock()
	defer n.RUnlock()
	return n.totalAlloc
}

// SetAllocation sets the resource allocation for the pool
func (n *resPool) SetAllocation(res *scalar.Resources) {
	n.Lock()
	defer n.Unlock()
	n.totalAlloc = res
}

// GetDemand gets the resource demand for the pool
func (n *resPool) GetDemand() *scalar.Resources {
	n.RLock()
	defer n.RUnlock()
	return n.demand
}

// SubtractFromAllocation updates the allocation for the resource pool
func (n *resPool) SubtractFromAllocation(preemptible bool, res *scalar.
	Resources) error {
	n.Lock()
	defer n.Unlock()

	newAllocation := n.totalAlloc.Subtract(res)

	if newAllocation == nil {
		return errors.Errorf("Couldn't update the resources")
	}
	n.totalAlloc = newAllocation

	if !preemptible {
		n.nonPreemptibleAlloc = n.nonPreemptibleAlloc.Subtract(res)

		//TODO av: Change to debug
		log.
			WithField("respool_id", n.ID()).
			WithField("non_preemptible_alloc", n.nonPreemptibleAlloc).
			Info("Current non-preemptible totalAlloc")
	}

	log.
		WithField("respool_id", n.ID()).
		WithField("total_alloc", n.totalAlloc).
		WithField("non_preemptible_alloc", n.totalAlloc).
		Debug("Current Allocation after Done Task")
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
func (n *resPool) AddToAllocation(preemptible bool,
	res *scalar.Resources) error {
	n.Lock()
	defer n.Unlock()

	n.totalAlloc = n.totalAlloc.Add(res)

	if !preemptible {
		n.nonPreemptibleAlloc = n.nonPreemptibleAlloc.Add(res)
	}

	log.WithFields(log.Fields{
		"respool_id":            n.ID(),
		"total_alloc":           n.totalAlloc,
		"non_preemptible_alloc": n.nonPreemptibleAlloc,
	}).Debug("Current Allocation after Adding resources")

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
	n.metrics.ResourcePoolReservation.Update(getReservations(n.resourceConfigs))
}

// updates dynamic metrics(Allocation, Entitlement, Available) which change based on
// usage and entitlement calculation
func (n *resPool) updateDynamicResourceMetrics() {
	n.metrics.Entitlement.Update(n.entitlement)
	n.metrics.TotalAllocation.Update(n.totalAlloc)
	n.metrics.NonPreemptibleAllocation.Update(n.nonPreemptibleAlloc)
	n.metrics.Available.Update(n.entitlement.Subtract(n.totalAlloc))
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

func (n *resPool) PeekPendingGangs(limit uint32) ([]*resmgrsvc.Gang, error) {
	n.RLock()
	defer n.RUnlock()
	return n.pendingQueue.Peek(limit)
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

func getReservations(resourceConfigs map[string]*respool.ResourceConfig) *scalar.Resources {
	var resources scalar.Resources
	for kind, res := range resourceConfigs {
		switch kind {
		case common.CPU:
			resources.CPU = res.Reservation
		case common.GPU:
			resources.GPU = res.Reservation
		case common.MEMORY:
			resources.MEMORY = res.Reservation
		case common.DISK:
			resources.DISK = res.Reservation
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
