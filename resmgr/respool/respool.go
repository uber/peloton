package respool

import (
	"container/list"
	"math"
	"sync"

	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/resmgr/queue"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
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
	// Forms a scheduling unit from a single task
	MakeTaskSchedulingUnit(task *resmgr.Task) *list.List
	// Enqueues scheduling unit (task list) into resource pool pending queue
	EnqueueSchedulingUnit(tlist *list.List) error
	// Dequeues scheduling unit (task list) list from the resource pool
	DequeueSchedulingUnitList(int) (*list.List, error)
	// SetEntitlement sets the entitlement for the resource pool
	// input is map[ResourceKind]->EntitledCapacity
	SetEntitlement(map[string]float64)
	// SetEntitlementByKind sets the entitlement of the respool
	// by kind
	SetEntitlementByKind(kind string, entitlement float64)
	//GetEntitlement gets the entitlement for the resource pool
	GetEntitlement() map[string]float64
	// GetChildReservation returns the total reservation of all
	// the childs by kind
	GetChildReservation() (map[string]float64, error)
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
	entitlement     map[string]float64
}

// NewRespool will initialize the resource pool node and return that
func NewRespool(
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

	result := resPool{
		id:              ID,
		children:        list.New(),
		parent:          parent,
		resourceConfigs: make(map[string]*respool.ResourceConfig),
		poolConfig:      config,
		pendingQueue:    q,
		entitlement:     make(map[string]float64),
	}

	result.initResources(config)
	return &result, nil
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
	if resources == nil {
		return
	}
	n.resourceConfigs[resources.Kind] = resources
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
	n.initResources(config)
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

// MakeTaskSchedulingUnit forms a scheduling unit from a single task
func (n *resPool) MakeTaskSchedulingUnit(task *resmgr.Task) *list.List {
	tlist := new(list.List)
	tlist.PushBack(task)
	return tlist
}

// EnqueueSchedulingUnit inserts a scheduling unit, which is a task list
// representing a gang of 1 or more (same priority) tasks, into pending queue
func (n *resPool) EnqueueSchedulingUnit(tlist *list.List) error {
	if (tlist == nil) || (tlist.Len() <= 0) {
		err := errors.Errorf("scheduling unit has no elements")
		return err
	}
	if n.isLeaf() {
		err := n.pendingQueue.Enqueue(tlist)
		return err
	}
	err := errors.Errorf("Respool %s is not a leaf node", n.id)
	return err
}

// DequeueSchedulingUnitList dequeues a list of scheduling units from the
// pending queue.  Each scheduling unit is a task list representing a gang
// of 1 or more (same priority) tasks, from the pending queue.
func (n *resPool) DequeueSchedulingUnitList(limit int) (*list.List, error) {
	if n.isLeaf() {
		if limit <= 0 {
			err := errors.Errorf("limit %d is not valid", limit)
			return nil, err
		}
		l := new(list.List)
		for i := 1; i <= limit; i++ {
			resList, err := n.pendingQueue.Dequeue()
			if err != nil {
				if l.Len() == 0 {
					return nil, err
				}
				break
			}
			l.PushBack(resList)
		}
		return l, nil
	}
	err := errors.Errorf("Respool %s is not a leaf node", n.id)
	return nil, err
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
	}
}

func (n *resPool) isLeaf() bool {
	return n.children.Len() == 0
}

// initResources will initializing the resource poolConfig under resource pool
// NB: The function calling initResources should acquire the lock
func (n *resPool) initResources(config *respool.ResourcePoolConfig) {
	resList := config.GetResources()
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
		case
			common.CPU,
			common.DISK,
			common.GPU,
			common.MEMORY:
			n.entitlement[kind] = capacity
			log.WithFields(log.Fields{
				"ID":       n.ID(),
				"Kind":     kind,
				"Capacity": capacity,
			}).Debug("Setting Entitlement")
		}
	}
}

// SetEntitlement sets the entitlement for the resource pool
func (n *resPool) SetEntitlementByKind(kind string, entitlement float64) {
	n.Lock()
	defer n.Unlock()
	n.entitlement[kind] = entitlement
	log.WithFields(log.Fields{
		"ID":       n.ID(),
		"Kind":     kind,
		"Capacity": entitlement,
	}).Debug("Setting Entitlement")
}

// GetEntitlement gets the entitlement for the resource pool
func (n *resPool) GetEntitlement() map[string]float64 {
	n.RLock()
	defer n.RUnlock()
	return n.entitlement
}

// GetChildReservation returns the reservation of all the childs
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
