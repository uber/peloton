package respool

import (
	"container/list"
	"math"
	"sync"

	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/resmgr/queue"
	"code.uber.internal/infra/peloton/resmgr/scalar"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
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
	// SetEntitlementByKind sets the entitlement of the respool
	// by kind
	SetEntitlementByKind(kind string, entitlement float64)
	//GetEntitlement gets the entitlement for the resource pool
	GetEntitlement() *scalar.Resources
	// GetChildReservation returns the total reservation of all
	// the childs by kind
	GetChildReservation() (map[string]float64, error)
	//GetAllocation returns the resource allocation for the resource pool
	GetAllocation() *scalar.Resources
	// MarkItDone recaptures the resources from task
	MarkItDone(res *scalar.Resources) error
}

// resPool implements ResPool interface
type resPool struct {
	sync.RWMutex

	id              string
	children        *list.List
	parent          ResPool
	resourceConfigs map[string]*respool.ResourceConfig
	poolConfig      *respool.ResourcePoolConfig
	pendingQueue    queue.Queue
	entitlement     *scalar.Resources
	allocation      *scalar.Resources
	metrics         *Metrics
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

	// tag metrics scope with the resource pool ID
	poolScope := scope.SubScope("respool_node")
	poolScope.Tagged(map[string]string{
		"respool_id": ID,
	})

	pool := &resPool{
		id:              ID,
		children:        list.New(),
		parent:          parent,
		resourceConfigs: make(map[string]*respool.ResourceConfig),
		poolConfig:      config,
		pendingQueue:    q,
		entitlement:     &scalar.Resources{},
		allocation:      &scalar.Resources{},
		metrics:         NewMetrics(poolScope),
	}

	// Initialize
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
	n.RLock()
	defer n.RUnlock()
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
			n.allocation = n.allocation.Add(n.getGangResources(gang))
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

func (n *resPool) canBeAdmitted(gang *resmgrsvc.Gang) bool {
	n.RLock()
	defer n.RUnlock()

	currentEntitlement := n.entitlement
	log.WithField("entitlement", currentEntitlement).Debug("Current Entitlement")
	currentAllocation := n.allocation
	log.WithField("allocation", currentAllocation).Debug("Current Allocation")
	neededResources := n.getGangResources(gang)
	log.WithField("neededResources", neededResources).Debug("Current Resources needed")
	return currentAllocation.
		Add(neededResources).
		LessThanOrEqual(currentEntitlement)
}

func (n *resPool) getGangResources(
	gang *resmgrsvc.Gang,
) *scalar.Resources {
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
	childrenResourcePoolIDs := make([]*respool.ResourcePoolID, 0, childrenResPools.Len())
	for child := childrenResPools.Front(); child != nil; child = child.Next() {
		childrenResourcePoolIDs = append(childrenResourcePoolIDs, &respool.ResourcePoolID{
			Value: child.Value.(*resPool).id,
		})
	}

	var parentResPoolID *respool.ResourcePoolID

	// handle Root's parent == nil
	if n.parent != nil {
		parentResPoolID = &respool.ResourcePoolID{
			Value: n.parent.ID(),
		}
	}

	return &respool.ResourcePoolInfo{
		Id: &respool.ResourcePoolID{
			Value: n.id,
		},
		Parent:   parentResPoolID,
		Config:   n.poolConfig,
		Children: childrenResourcePoolIDs,
		Usage:    n.createRespoolUsage(n.GetAllocation()),
	}
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
		log.WithFields(log.Fields{
			"ID":     n.ID(),
			"CPU":    n.entitlement.CPU,
			"DISK":   n.entitlement.DISK,
			"MEMORY": n.entitlement.MEMORY,
			"GPU":    n.entitlement.GPU,
		}).Debug("Setting Entitlement for Respool")
	}
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

// GetEntitlement gets the entitlement for the resource pool
func (n *resPool) GetEntitlement() *scalar.Resources {
	n.RLock()
	defer n.RUnlock()
	return n.entitlement
}

// GetChildReservation returns the reservation of all the children
func (n *resPool) GetChildReservation() (map[string]float64, error) {
	nodes := n.Children()
	if nodes == nil || nodes.Len() == 0 {
		return nil, errors.Errorf("respool %s does not have "+
			"children", n.id)
	}

	totalReservation := make(map[string]float64)
	// We need to find out the total reservation
	for e := nodes.Front(); e != nil; e = e.Next() {
		n := e.Value.(ResPool)
		resources := n.Resources()
		for kind, resource := range resources {
			totalReservation[kind] =
				totalReservation[kind] + resource.Reservation
		}
	}
	return totalReservation, nil
}

// GetAllocation gets the resource allocation for the pool
func (n *resPool) GetAllocation() *scalar.Resources {
	n.RLock()
	defer n.RUnlock()
	return n.allocation
}

// MarkItDone updates the allocation for the resource pool
func (n *resPool) MarkItDone(res *scalar.Resources) error {
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
