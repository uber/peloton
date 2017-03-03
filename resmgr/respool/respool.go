package respool

import (
	"container/list"
	"peloton/api/respool"

	"code.uber.internal/infra/peloton/resmgr/queue"
	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"math"
	"peloton/private/resmgr"
)

// ResPool this is the struct which will be holding the resource pool
type ResPool struct {
	// TODO: We need to add synchronization support
	ID              string
	children        *list.List
	parent          *ResPool
	name            string
	resourceConfigs map[string]*respool.ResourceConfig
	respoolConfig   *respool.ResourcePoolConfig
	schedulingPlicy respool.SchedulingPolicy
	pendingQueue    queue.Queue
}

// NewRespool will intializing the resource pool node and return that
func NewRespool(
	ID string,
	parent *ResPool,
	respoolConfig *respool.ResourcePoolConfig) *ResPool {
	result := ResPool{
		children:        list.New(),
		ID:              ID,
		parent:          parent,
		resourceConfigs: make(map[string]*respool.ResourceConfig),
		respoolConfig:   respoolConfig,
	}
	result.initResources(respoolConfig)
	result.schedulingPlicy = respoolConfig.Policy
	q, err := queue.CreateQueue(respoolConfig.Policy, math.MaxInt64)
	if err != nil {
		log.WithField("ResPool: ", ID).Error("Error creating resource pool pending queue")
		return nil
	}
	result.pendingQueue = q
	return &result
}

// SetParent will be setting the parent for the resource pool
func (n *ResPool) SetParent(parent *ResPool) {
	n.parent = parent
}

// SetChildren will be setting the children for the resource pool
func (n *ResPool) SetChildren(children *list.List) {
	n.children = children
}

// GetChildren will be getting the children for the resource pool
func (n *ResPool) GetChildren() *list.List {
	return n.children
}

// initResources will initializing the resource config under resource pool
func (n *ResPool) initResources(config *respool.ResourcePoolConfig) {
	resList := config.GetResources()
	for _, res := range resList {
		n.resourceConfigs[res.Kind] = res
	}
}

// logNodeResources will be printing the resources for the resource pool
func (n *ResPool) logNodeResources(node *ResPool) {
	for kind, res := range node.resourceConfigs {
		log.WithFields(log.Fields{
			"Kind":        kind,
			"Limit":       res.Limit,
			"Reservation": res.Reservation,
			"Share":       res.Share,
		}).Info("Node Resources for Node", node.ID)
	}
}

// SetResources will set the resource config for one kind of resource.
func (n *ResPool) SetResources(resources *respool.ResourceConfig) {
	if resources == nil {
		return
	}
	n.resourceConfigs[resources.Kind] = resources
}

// Isleaf will tell us if this resource pool is leaf or not
func (n *ResPool) Isleaf() bool {
	return (n.GetChildren().Len() == 0)
}

// EnqueueTask enques the task into pending queue
func (n *ResPool) EnqueueTask(task *resmgr.Task) error {
	if n.Isleaf() {
		err := n.pendingQueue.Enqueue(task)
		return err
	}
	err := errors.Errorf("Respool %s is not a leaf node", n.name)
	return err
}

// DequeueTask dequeues the task from the pending queue
func (n *ResPool) DequeueTask() (*resmgr.Task, error) {
	// TODO: We need to merge both dequeuetask and dequeuetasks
	if n.Isleaf() {
		res, err := n.pendingQueue.Dequeue()
		if err != nil {
			return nil, err
		}
		return res, err
	}
	err := errors.Errorf("Respool %s is not a leaf node", n.name)
	return nil, err
}

// DequeueTasks dequeues the tasks from the pending queue
func (n *ResPool) DequeueTasks(limit int) (*list.List, error) {
	if n.Isleaf() {
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
