package respool

import (
	"container/list"
	"math"
	"sync"

	"peloton/api/respool"
	"peloton/private/resmgr"

	"code.uber.internal/infra/peloton/resmgr/queue"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
)

// ResPool is a node in a resource tree
type ResPool interface {
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
	// Enqueues task into the pending queue of the resource pool
	EnqueueTask(task *resmgr.Task) error
	// Dequques tasks from the resource pool
	DequeueTasks(int) (*list.List, error)
}

// resPool implements ResPool interface
type resPool struct {
	sync.RWMutex

	id              string
	children        *list.List
	parent          ResPool
	name            string
	resourceConfigs map[string]*respool.ResourceConfig
	poolConfig      *respool.ResourcePoolConfig
	pendingQueue    queue.Queue
}

// NewRespool will initialize the resource pool node and return that
func NewRespool(
	ID string,
	parent ResPool,
	config *respool.ResourcePoolConfig) (ResPool, error) {
	result := resPool{
		children:        list.New(),
		id:              ID,
		parent:          parent,
		resourceConfigs: make(map[string]*respool.ResourceConfig),
		poolConfig:      config,
	}

	result.initResources(config)
	q, err := queue.CreateQueue(config.Policy, math.MaxInt64)
	if err != nil {
		log.WithField("ResPool: ", ID).Error("Error creating resource pool pending queue")
		return nil, errors.Wrapf(err, "error creating resource pool %s", ID)
	}
	result.pendingQueue = q
	return &result, nil
}

// ID returns the resource pool ID
func (n *resPool) ID() string {
	return n.id
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

// EnqueueTask inserts the task into pending queue
func (n *resPool) EnqueueTask(task *resmgr.Task) error {
	n.Lock()
	defer n.Unlock()
	if n.isLeaf() {
		err := n.pendingQueue.Enqueue(task)
		return err
	}
	err := errors.Errorf("Respool %s is not a leaf node", n.name)
	return err
}

// DequeueTasks dequeues the tasks from the pending queue
func (n *resPool) DequeueTasks(limit int) (*list.List, error) {
	n.Lock()
	defer n.Unlock()
	if n.isLeaf() {
		if limit <= 0 {
			err := errors.Errorf("limt %d is not valid", limit)
			return nil, err
		}
		l := new(list.List)
		for i := 1; i <= limit; i++ {
			res, err := n.pendingQueue.Dequeue()
			if err != nil {
				if l.Len() == 0 {
					return nil, err
				}
				break
			}
			l.PushBack(res)
		}
		return l, nil
	}
	err := errors.Errorf("Respool %s is not a leaf node", n.name)
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
			return nil, errors.Errorf("failed to type assert child resource pool %v", child.Value)
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
