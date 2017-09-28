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

// ResPool is a node in a resource tree
type ResPool interface {
	// Returns the resource pool name
	Name() string
	// Returns the resource pool ID
	ID() string
	// Returns the resource pool's parent
	Parent() ResPool
	// Sets the parent of the resource pool
	SetParent(ResPool)
	// Returns the children(if any) of the resource pool
	Children() *list.List
	// Sets the children of the resource pool
	SetChildren(*list.List)
	// Returns true of the resource pool is a leaf
	IsLeaf() bool
	// Returns the config of the resource pool
	ResourcePoolConfig() *respool.ResourcePoolConfig
	// Sets the resource pool config
	SetResourcePoolConfig(*respool.ResourcePoolConfig)
	// Returns a map of resources and its resource config
	Resources() map[string]*respool.ResourceConfig
	// Sets the resource config
	SetResourceConfig(*respool.ResourceConfig)
	// Converts to resource pool info
	ToResourcePoolInfo() *respool.ResourcePoolInfo
	// Aggregates the child reservations by resource type
	AggregatedChildrenReservations() (map[string]float64, error)
	// Forms a gang from a single task
	MakeTaskGang(task *resmgr.Task) *resmgrsvc.Gang
	// Enqueues gang (task list) into resource pool pending queue
	EnqueueGang(gang *resmgrsvc.Gang) error
	// Dequeues gang (task list) list from the resource pool
	DequeueGangList(int) ([]*resmgrsvc.Gang, error)
	// SetEntitlement sets the entitlement for the resource pool
	// input is map[ResourceKind]->EntitledCapacity
	SetEntitlement(map[string]float64)
	// SetEntitlementResources sets the entitlement for
	// the resource pool based on the resources
	SetEntitlementResources(res *scalar.Resources)
	// SetEntitlementByKind sets the entitlement of the respool
	// by kind
	SetEntitlementByKind(kind string, entitlement float64)
	//GetEntitlement gets the entitlement for the resource pool
	GetEntitlement() *scalar.Resources
	// GetChildReservation returns the total reservation of all
	// the children by kind
	GetChildReservation() map[string]float64
	// GetAllocation returns the resource allocation for the resource pool
	GetAllocation() *scalar.Resources
	// SetAllocation sets the resource allocation for the resource pool
	SetAllocation(res *scalar.Resources)
	// SubtractFromAllocation recaptures the resources from task
	SubtractFromAllocation(res *scalar.Resources) error
	// GetPath returns the resource pool path
	GetPath() string
	// IsRoot returns true if the node is the root of the resource tree
	IsRoot() bool
	// AddToAllocation adds resources to current allocation
	// for the resource pool
	AddToAllocation(res *scalar.Resources) error
	// AddToDemand adds resources to current demand
	// for the resource pool
	AddToDemand(res *scalar.Resources) error
	// SubtractFromDemand subtracts resources from current demand
	// for the resource pool
	SubtractFromDemand(res *scalar.Resources) error
	// GetDemand returns the resource demand for the resource pool
	GetDemand() *scalar.Resources
	// CalculateDemand calculates the resource demand
	// for the resource pool recursively for the subtree
	CalculateDemand() *scalar.Resources
	// CalculateAllocation calculates the allocation recursively for
	// all the children.
	CalculateAllocation() *scalar.Resources
	// AddInvalidTask will add the killed tasks to respool
	// By that respool can discard those tasks asynchronously
	AddInvalidTask(task *peloton.TaskID)
}

// resPool implements ResPool interface
type resPool struct {
	sync.RWMutex

	id              string
	path            string
	children        *list.List
	parent          ResPool
	resourceConfigs map[string]*respool.ResourceConfig
	poolConfig      *respool.ResourcePoolConfig
	pendingQueue    queue.Queue
	entitlement     *scalar.Resources
	allocation      *scalar.Resources
	demand          *scalar.Resources
	metrics         *Metrics
	invalidTasks    map[string]bool
}

// NewRespool will initialize the resource pool node and return that
func NewRespool(
	scope tally.Scope,
	ID string,
	parent ResPool,
	config *respool.ResourcePoolConfig) (ResPool, error) {

	if config == nil {
		log.WithField("ResPool: ", ID).Error("resource config is empty")
		return nil, errors.Errorf("error creating resource pool %s; "+
			"ResourcePoolConfig is nil", ID)
	}

	q, err := queue.CreateQueue(config.Policy, math.MaxInt64)
	if err != nil {
		log.WithField("ResPool: ", ID).
			WithError(err).
			Error("Error creating resource pool pending queue")
		return nil, errors.Wrapf(err, "error creating resource pool %s", ID)
	}

	pool := &resPool{
		id:              ID,
		children:        list.New(),
		parent:          parent,
		resourceConfigs: make(map[string]*respool.ResourceConfig),
		poolConfig:      config,
		pendingQueue:    q,
		entitlement:     &scalar.Resources{},
		allocation:      &scalar.Resources{},
		demand:          &scalar.Resources{},
		invalidTasks:    make(map[string]bool),
	}

	// Initialize metrics
	poolScope := scope.Tagged(map[string]string{
		"path": pool.GetPath(),
	})
	pool.metrics = NewMetrics(poolScope)

	// Initialize resources
	pool.initResources(config.GetResources())
	pool.updateDynamicResourceMetrics()
	pool.metrics.PendingQueueSize.Update(float64(pool.pendingQueue.Size()))

	return pool, nil
}

// ID returns the resource pool UUID
func (n *resPool) ID() string {
	return n.id
}

// Name returns the resource pool name
func (n *resPool) Name() string {
	return n.poolConfig.Name
}

// Parent returns the resource pool's parent pool
func (n *resPool) Parent() ResPool {
	n.RLock()
	defer n.RUnlock()
	return n.parent
}

// SetParent will be setting the parent for the resource pool
func (n *resPool) SetParent(parent ResPool) {
	n.Lock()
	n.parent = parent
	// reset path to be calculated again
	n.path = ""
	n.Unlock()
}

// SetChildren will be setting the children for the resource pool
func (n *resPool) SetChildren(children *list.List) {
	n.Lock()
	n.children = children
	n.Unlock()
}

// Children will be getting the children for the resource pool
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

// Resources returns the resource configs
func (n *resPool) Resources() map[string]*respool.ResourceConfig {
	n.RLock()
	defer n.RUnlock()
	return n.resourceConfigs
}

// SetResourcePoolConfig sets the resource pool config and initializes the resources
func (n *resPool) SetResourcePoolConfig(config *respool.ResourcePoolConfig) {
	n.Lock()
	defer n.Unlock()
	n.poolConfig = config
	n.initResources(config.GetResources())
}

// ResourcePoolConfig returns the resource pool config
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
	err := errors.Errorf("Respool %s is not a leaf node", n.id)
	return err
}

// DequeueGangList dequeues a list of gangs from the
// pending queue.  Each gang is a task list representing a gang
// of 1 or more (same priority) tasks, from the pending queue.
// If there is no gang present then its a error if any one present
// then it will return all the gangs within limit without error
func (n *resPool) DequeueGangList(limit int) ([]*resmgrsvc.Gang, error) {
	if limit <= 0 {
		err := errors.Errorf("limit %d is not valid", limit)
		return nil, err
	}
	if !n.isLeaf() {
		err := errors.Errorf("Respool %s is not a leaf node", n.id)
		return nil, err
	}

	var gangList []*resmgrsvc.Gang
	for i := 1; i <= limit; i++ {
		// First peek the gang and see if that can be
		// dequeued from the list
		gang, err := n.pendingQueue.Peek()

		if err != nil {
			if len(gangList) == 0 {
				return nil, err
			}
			break
		}

		// checking if the gang is stil valid
		// before admission control
		err = n.validateGang(gang)
		if err != nil {
			continue
		}

		if n.canBeAdmitted(gang) {
			log.WithField("gang", gang.Tasks[0].Id).
				Debug("Can be admitted")
			err := n.pendingQueue.Remove(gang)
			if err != nil {
				if len(gangList) == 0 {
					return nil, err
				}
				break
			}
			log.WithField("gang", gang).Debug("Deleted")
			gangList = append(gangList, gang)
			// Need to lock the usage before updating it
			n.Lock()
			n.allocation = n.allocation.Add(GetGangResources(gang))
			n.Unlock()
			n.updateDynamicResourceMetrics()
		} else {
			if len(gangList) <= 0 {
				log.WithField(
					"gang", gang).Debug("Need more" +
					" resources then available")
				return nil, errors.Errorf("Queue %s is already full", n.id)
			}
		}
	}
	n.metrics.PendingQueueSize.Update(float64(n.pendingQueue.Size()))
	return gangList, nil
}

// validateGang checks for all/some tasks been deleted. This function will delete that
// gang from pending queue if all tasks are deleted or enqueue gang back with rest of
// the tasks in the gang
func (n *resPool) validateGang(gang *resmgrsvc.Gang) error {
	n.Lock()
	defer n.Unlock()
	var invalidateGang bool

	if gang == nil {
		invalidateGang = true
	}
	var newTasks []*resmgr.Task

	// Check if gang is not nil
	if !invalidateGang {
		for _, task := range gang.GetTasks() {
			if _, exists := n.invalidTasks[task.Id.Value]; !exists {
				newTasks = append(newTasks, task)
			} else {
				delete(n.invalidTasks, task.Id.Value)
				invalidateGang = true
			}
		}
	}

	// Gang is validated so ready for admission control
	if !invalidateGang {
		return nil
	}

	// remove it from the pending queue
	log.WithFields(log.Fields{
		"Gang": gang,
	}).Info("Tasks are deleted for this gang")
	// Removing the gang from pending queue as tasks are deleted
	// from this gang
	err := n.pendingQueue.Remove(gang)
	if err != nil {
		log.WithError(err).Error("Not able to delete" +
			"gang from pending queue")
		return err
	}

	// We need to enqueue the gang if all the tasks are not
	// deleted from the gang
	if gang != nil && len(newTasks) != 0 {
		gang.Tasks = newTasks
		err = n.pendingQueue.Enqueue(gang)
		if err != nil {
			log.WithError(err).WithField("newGang", gang).
				Error("Not able to enqueue" +
					"gang to pending queue")
		}
		return err
	}
	// All the tasks in this gang is been deleted
	// sending error for the validation by that ignore this gang
	// for admission control
	log.WithField("gang", gang).Info("All tasks are killed " +
		"for gang")
	return errors.New("All tasks are killed for gang")
}

func (n *resPool) canBeAdmitted(gang *resmgrsvc.Gang) bool {
	n.RLock()
	defer n.RUnlock()

	currentEntitlement := n.entitlement
	log.WithField("entitlement", currentEntitlement).Debug("Current Entitlement")
	currentAllocation := n.allocation
	log.WithField("allocation", currentAllocation).Debug("Current Allocation")
	neededResources := GetGangResources(gang)
	log.WithField("neededResources", neededResources).Debug("Current Resources needed")
	return currentAllocation.
		Add(neededResources).
		LessThanOrEqual(currentEntitlement)
}

// GetGangResources aggregates gang resources to resmgr resources
func GetGangResources(gang *resmgrsvc.Gang) *scalar.Resources {
	if gang == nil {
		return nil
	}
	totalRes := &scalar.Resources{}
	for _, task := range gang.GetTasks() {
		totalRes = totalRes.Add(
			scalar.ConvertToResmgrResource(task.GetResource()))
	}
	return totalRes
}

// AggregatedChildrenReservations returns aggregated child reservations by resource kind
func (n *resPool) AggregatedChildrenReservations() (map[string]float64, error) {
	aggChildrenReservations := make(map[string]float64)
	n.RLock()
	defer n.RUnlock()
	for child := n.children.Front(); child != nil; child = child.Next() {
		if childResPool, ok := child.Value.(*resPool); ok {
			for kind, cResource := range childResPool.resourceConfigs {
				cReservation := cResource.Reservation
				if reservations, ok := aggChildrenReservations[kind]; ok {
					// We only need reservations
					cReservation += reservations
				}
				aggChildrenReservations[kind] = cReservation
			}
		} else {
			return nil, errors.Errorf("failed to type assert child resource pool %v",
				child.Value)
		}
	}
	return aggChildrenReservations, nil
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

// calculateAllocation calculates the allocation recursively for
// all the children.
func (n *resPool) CalculateAllocation() *scalar.Resources {
	if n.IsLeaf() {
		return n.allocation
	}
	allocation := &scalar.Resources{}
	for child := n.children.Front(); child != nil; child = child.Next() {
		if childResPool, ok := child.Value.(*resPool); ok {
			allocation = allocation.Add(
				childResPool.CalculateAllocation())
		}
	}
	n.allocation = allocation
	return allocation
}

// CalculateAndSetDemand calculates and sets the resource demand
// for the resource pool recursively for the subtree
func (n *resPool) CalculateDemand() *scalar.Resources {
	if n.IsLeaf() {
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
	n.updateStaticResourceMetrics()
}

// logNodeResources will be printing the resources for the resource pool
func (n *resPool) logNodeResources() {
	n.RLock()
	defer n.RLock()
	for kind, res := range n.resourceConfigs {
		log.WithFields(log.Fields{
			"Kind":        kind,
			"Limit":       res.Limit,
			"Reservation": res.Reservation,
			"Share":       res.Share,
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
		"ID":     n.ID(),
		"CPU":    n.entitlement.CPU,
		"DISK":   n.entitlement.DISK,
		"MEMORY": n.entitlement.MEMORY,
		"GPU":    n.entitlement.GPU,
	}).Debug("Setting Entitlement for Respool")
	n.updateDynamicResourceMetrics()
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
		"ID":       n.ID(),
		"Kind":     kind,
		"Capacity": entitlement,
	}).Debug("Setting Entitlement")
	n.updateDynamicResourceMetrics()
}

func (n *resPool) SetEntitlementResources(res *scalar.Resources) {
	n.Lock()
	defer n.Unlock()
	n.entitlement = res
	log.WithFields(log.Fields{
		"Name":     n.Name(),
		"Capacity": res,
	}).Debug("Setting Entitlement")
	n.updateDynamicResourceMetrics()
}

// GetEntitlement gets the entitlement for the resource pool
func (n *resPool) GetEntitlement() *scalar.Resources {
	n.RLock()
	defer n.RUnlock()
	return n.entitlement
}

// GetChildReservation returns the reservation of all the children
func (n *resPool) GetChildReservation() map[string]float64 {
	totalReservation := make(map[string]float64)
	nodes := n.Children()
	// We need to find out the total reservation
	for e := nodes.Front(); e != nil; e = e.Next() {
		n := e.Value.(ResPool)
		resources := n.Resources()
		for kind, resource := range resources {
			totalReservation[kind] =
				totalReservation[kind] + resource.Reservation
		}
	}
	return totalReservation
}

// GetAllocation gets the resource allocation for the pool
func (n *resPool) GetAllocation() *scalar.Resources {
	n.RLock()
	defer n.RUnlock()
	return n.allocation
}

// SetAllocation sets the resource allocation for the pool
func (n *resPool) SetAllocation(res *scalar.Resources) {
	n.Lock()
	defer n.Unlock()
	n.allocation = res
}

// GetDemand gets the resource allocation for the pool
func (n *resPool) GetDemand() *scalar.Resources {
	n.RLock()
	defer n.RUnlock()
	return n.demand
}

// SubtractFromAllocation updates the allocation for the resource pool
func (n *resPool) SubtractFromAllocation(res *scalar.Resources) error {
	n.Lock()
	defer n.Unlock()

	newAllocation := n.allocation.Subtract(res)

	if newAllocation == nil {
		return errors.Errorf("Couldn't update the resources")
	}
	n.allocation = newAllocation

	log.WithField("allocation", n.allocation).Debug("Current Allocation after Done Task")
	return nil
}

// IsRoot returns true if the node is the root in the resource
// pool hierarchy
func (n *resPool) IsRoot() bool {
	return n.ID() == RootResPoolID
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
func (n *resPool) AddToAllocation(res *scalar.Resources) error {
	n.Lock()
	defer n.Unlock()

	n.allocation = n.allocation.Add(res)

	log.WithFields(log.Fields{
		"respool":    n.Name(),
		"allocation": n.allocation,
	}).Debug("Current Allocation after Adding resources")
	return nil

}

// AddToDemand adds resources to the demand
// for the resource pool
func (n *resPool) AddToDemand(res *scalar.Resources) error {
	n.Lock()
	defer n.Unlock()

	n.demand = n.demand.Add(res)

	log.WithFields(log.Fields{
		"respool": n.Name(),
		"demand":  n.demand,
	}).Debug("Current Demand after Adding resources")

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

	log.WithField("demand", n.demand).Debug("Current Demand " +
		"after removing resources")
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
	n.metrics.ResourcePoolEntitlement.Update(n.entitlement)
	n.metrics.ResourcePoolAllocation.Update(n.allocation)
	n.metrics.ResourcePoolAvailable.Update(n.entitlement.Subtract(n.allocation))
}

// AddInvalidTask adds the invalid task by that it can
// remove them from the respool later
func (n *resPool) AddInvalidTask(task *peloton.TaskID) {
	n.Lock()
	defer n.Unlock()
	n.invalidTasks[task.Value] = true
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
