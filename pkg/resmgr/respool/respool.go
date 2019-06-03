// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package respool

import (
	"container/list"
	"math"
	"sync"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/respool"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/common"
	rc "github.com/uber/peloton/pkg/resmgr/common"
	"github.com/uber/peloton/pkg/resmgr/queue"
	"github.com/uber/peloton/pkg/resmgr/scalar"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

const (
	// ToDo (varung): Move this to resource manager config
	// Represents the default slack limit at Cluster level
	_defaultSlackLimit  = 20
	_defaultReservation = 0
	_defaultLimit       = 0
	_defaultShare       = 1
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

	// SetEntitlement sets the entitlement of non-revocable resources
	// for non-revocable tasks + revocable tasks for this resource pool.
	SetEntitlement(res *scalar.Resources)
	// SetSlackEntitlement sets the entitlement of revocable cpus
	// + non-revocable resources [mem, disk, gpu] for the resource pool
	SetSlackEntitlement(res *scalar.Resources)
	// SetNonSlackEntitlement Sets the entitlement of non-revocable resources
	// for non-revocable tasks for this resource pool
	SetNonSlackEntitlement(res *scalar.Resources)

	// GetEntitlement returns the total entitlement of non-revocable resources
	// for this resource pool.
	GetEntitlement() *scalar.Resources
	// GetSlackEntitlement returns the entitlement for revocable tasks.
	GetSlackEntitlement() *scalar.Resources
	// GetNonSlackEntitlement returns the entitlement for non-revocable tasks.
	GetNonSlackEntitlement() *scalar.Resources

	// AddToAllocation adds resources to current allocation
	// for the resource pool.
	AddToAllocation(*scalar.Allocation) error
	// SubtractFromAllocation recaptures the resources from task.
	SubtractFromAllocation(*scalar.Allocation) error

	// GetTotalAllocatedResources returns the total resource allocation for the resource
	// pool.
	GetTotalAllocatedResources() *scalar.Resources
	// GetSlackAllocatedResources returns the slack allocation for the resource pool.
	GetSlackAllocatedResources() *scalar.Resources
	// GetNonSlackAllocatedResources returns resources allocated to non-revocable tasks.
	GetNonSlackAllocatedResources() *scalar.Resources

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

	// AddToSlackDemand adds resources to slack demand
	// for the resource pool.
	AddToSlackDemand(res *scalar.Resources) error
	// SubtractFromSlackDemand subtracts resources from slack demand
	// for the resource pool.
	SubtractFromSlackDemand(res *scalar.Resources) error
	// GetSlackDemand returns the slack resource demand for the resource pool.
	GetSlackDemand() *scalar.Resources
	// CalculateSlackDemand calculates the slack resource demand
	// for the resource pool recursively for the subtree.
	CalculateSlackDemand() *scalar.Resources

	// GetSlackLimit returns the limit of resources [mem,disk]
	// can be used by revocable tasks.
	GetSlackLimit() *scalar.Resources

	// AddInvalidTask will add the killed tasks to respool which can be
	// discarded asynchronously which scheduling.
	AddInvalidTask(task *peloton.TaskID)

	// UpdateResourceMetrics updates metrics for this resource pool
	// on each entitlement cycle calculation (15s)
	UpdateResourceMetrics()
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
	// Tracks the max resources for this resource pool to be given
	// for non-revocable tasks.
	nonSlackEntitlement *scalar.Resources
	// Tracks the max slack resources this resource pool can use
	// in a given entitlement cycle for revocable tasks.
	// As of now, slack entitlement is only calculated for [cpus]
	slackEntitlement *scalar.Resources

	// Tracks the demand of resources, in the pending and the controller queue,
	// which are waiting to be admitted.
	// Once admitted their resources are accounted for in `allocation`.
	demand *scalar.Resources
	// Tracks the demand of resources required by revocable tasks,
	// which are waiting to be admitted. Here, [cpus] is slack resources,
	// and [mem,disk] is regular resources, shared with non-revocable tasks.
	// Once admitted their resources are accounted for in `allocation`.
	slackDemand *scalar.Resources

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
	// queue containing revocable tasks waiting to be admitted.
	// All tasks are enqueued to the pending queue,
	// the only reason a task would move from pending queue to revocable
	// queue is when the task is revocable and it can't be admitted,
	// in that case the task is moved to the revocable queue so that it
	// doesn't block the rest of the tasks in pending queue.
	revocableQueue queue.Queue

	// The max limit of resources controller tasks can use in this pool
	controllerLimit *scalar.Resources

	// the max limit of resources revocable tasks can use in this pool.
	slackLimit *scalar.Resources

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

	rq, err := queue.CreateQueue(config.Policy, math.MaxInt64)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating revocable queue %s", id)
	}

	pool := &resPool{
		id:                  id,
		children:            list.New(),
		parent:              parent,
		resourceConfigs:     make(map[string]*respool.ResourceConfig),
		poolConfig:          config,
		pendingQueue:        pq,
		controllerQueue:     cq,
		npQueue:             nq,
		revocableQueue:      rq,
		allocation:          scalar.NewAllocation(),
		entitlement:         &scalar.Resources{},
		nonSlackEntitlement: &scalar.Resources{},
		slackEntitlement:    &scalar.Resources{},
		demand:              &scalar.Resources{},
		slackDemand:         &scalar.Resources{},
		slackLimit:          &scalar.Resources{},
		reservation:         &scalar.Resources{},
		invalidTasks:        make(map[string]bool),
		preemptionCfg:       preemptionConfig,
	}
	pool.path = pool.calculatePath()

	// Initialize metrics
	pool.metrics = NewMetrics(scope.Tagged(map[string]string{
		"path": pool.GetPath(),
	}))

	// Initialize resources and limits.
	pool.initialize(config)

	return pool, nil
}

// ID returns the resource pool UUID.
func (n *resPool) ID() string {
	return n.id
}

// Name returns the resource pool name.
func (n *resPool) Name() string {
	n.RLock()
	defer n.RUnlock()
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
	defer n.Unlock()
	n.parent = parent
	// calculate path again.
	n.path = n.calculatePath()
}

// SetChildren will be setting the children for the resource pool.
func (n *resPool) SetChildren(children *list.List) {
	n.Lock()
	defer n.Unlock()
	n.children = children
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

	if !n.isLeaf() {
		return errors.Errorf("resource pool %s is not a leaf node", n.id)
	}

	if err := n.pendingQueue.Enqueue(gang); err != nil {
		return err
	}

	// if task is revocable then add to slack demand
	if isRevocable(gang) {
		n.AddToSlackDemand(scalar.GetGangResources(gang))
		return nil
	}

	n.AddToDemand(scalar.GetGangResources(gang))
	return nil
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

	for _, qt := range []QueueType{
		NonPreemptibleQueue,
		ControllerQueue,
		RevocableQueue,
		PendingQueue} {
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
			}).WithError(err).Warn("Error dequeueing from queue")
			continue
		}
		gangList = append(gangList, gangs...)
	}

	return gangList, err
}

// dequeues limit number of gangs from the respool for admission.
func (n *resPool) dequeue(
	qt QueueType,
	limit int) ([]*resmgrsvc.Gang, error) {
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
			log.WithError(err).Error("Failed to peek into queue")
			return gangList, err
		}
		gang := gangs[0]
		err = admission.TryAdmit(gang, n, qt)
		if err != nil {
			if err == errGangInvalid ||
				err == errSkipNonPreemptibleGang ||
				err == errSkipControllerGang ||
				err == errSkipRevocableGang {
				// the admission can fail  :
				// 1. Because the gang is invalid.
				// In this case we move on to the next gang in the queue with the
				// expectation that the invalid gang is removed from the head of
				// the queue.
				// 2. Because the gang should be skipped (
				// revocable gang, controller gang or non-preemptible gang)
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

// AggregatedChildrenReservations returns aggregated child reservations by
// resource kind
func (n *resPool) AggregatedChildrenReservations() (map[string]float64, error) {
	totalReservation := make(map[string]float64)
	n.RLock()
	defer n.RUnlock()

	nodes := n.children
	// We need to find out the total reservation
	for e := nodes.Front(); e != nil; e = e.Next() {
		n, ok := e.Value.(ResPool)
		if !ok {
			return totalReservation, errors.Errorf(
				"failed to type assert child resource pool %v",
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
		Path:     &respool.ResourcePoolPath{Value: n.path},
		Usage: n.createRespoolUsage(
			n.allocation.GetByType(scalar.TotalAllocation),
			n.allocation.GetByType(scalar.SlackAllocation)),
	}
}

// CalculateTotalAllocatedResources calculates the total allocation
// recursively for all the children.
func (n *resPool) CalculateTotalAllocatedResources() *scalar.Resources {
	n.Lock()
	defer n.Unlock()
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

// CalculateDemand calculates and sets the non-revocable tasks
// resource demand for the resource pool recursively for the subtree
func (n *resPool) CalculateDemand() *scalar.Resources {
	n.Lock()
	defer n.Unlock()
	return n.calculateDemand()
}

// calculateDemand is a private method to recursively calculate demand for non-leaf
// resource pool nodes and return the demand for current resource pool
func (n *resPool) calculateDemand() *scalar.Resources {
	if n.isLeaf() {
		return n.demand
	}
	demand := &scalar.Resources{}
	for child := n.children.Front(); child != nil; child = child.Next() {
		if childResPool, ok := child.Value.(*resPool); ok {
			demand = demand.Add(
				childResPool.calculateDemand())
		}
	}
	n.demand = demand
	return demand
}

// CalculateAndSetDemand calculates and sets the resource demand
// for the resource pool recursively for the subtree
func (n *resPool) CalculateSlackDemand() *scalar.Resources {
	n.Lock()
	defer n.Unlock()
	return n.calculateSlackDemand()
}

// calculateSlackDemand in the private method to recursively calculate slack demand
// for non-leaf resource pool nodes and return the slack demand for current resource pool
func (n *resPool) calculateSlackDemand() *scalar.Resources {
	if n.isLeaf() {
		return n.slackDemand
	}
	slackDemand := &scalar.Resources{}
	for child := n.children.Front(); child != nil; child = child.Next() {
		if childResPool, ok := child.Value.(*resPool); ok {
			slackDemand = slackDemand.Add(
				childResPool.CalculateSlackDemand())
		}
	}
	n.slackDemand = slackDemand
	return slackDemand
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
	case RevocableQueue:
		return n.revocableQueue
	}

	// should never come here
	return nil
}

// creates the current resource pool's usage including all children.
// [cpus] is the only slack resource supported.
func (n *resPool) createRespoolUsage(
	allocation *scalar.Resources,
	slackAllocation *scalar.Resources) []*respool.ResourceUsage {
	resUsage := make([]*respool.ResourceUsage, 0, 4)
	ru := &respool.ResourceUsage{
		Kind:       common.CPU,
		Allocation: allocation.CPU - slackAllocation.CPU,
		Slack:      slackAllocation.CPU,
	}
	resUsage = append(resUsage, ru)
	ru = &respool.ResourceUsage{
		Kind:       common.GPU,
		Allocation: allocation.GPU - slackAllocation.GPU,
		Slack:      slackAllocation.GPU,
	}
	resUsage = append(resUsage, ru)
	ru = &respool.ResourceUsage{
		Kind:       common.MEMORY,
		Allocation: allocation.MEMORY - slackAllocation.MEMORY,
		Slack:      slackAllocation.MEMORY,
	}
	resUsage = append(resUsage, ru)
	ru = &respool.ResourceUsage{
		Kind:       common.DISK,
		Allocation: allocation.DISK - slackAllocation.DISK,
		Slack:      slackAllocation.DISK,
	}
	resUsage = append(resUsage, ru)
	return resUsage
}

// isLeaf checks if the current resource pool has child resource or not.
func (n *resPool) isLeaf() bool {
	return n.children.Len() == 0
}

// initializes the resources and limits for this pool
// NB: The function calling initResources should acquire the lock
func (n *resPool) initialize(cfg *respool.ResourcePoolConfig) {
	n.initResConfig(cfg)
	n.initControllerLimit(cfg)
	n.initSlackLimit(cfg)
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

// initControllerLimit initializes the limit of resources controller tasks can use.
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
	n.controllerLimit = controllerLimit

	log.WithField("controller_limit", controllerLimit).
		WithField("respool_id", n.id).
		Info("Setting controller limit")
}

// initSlackLimit initializes the limit of resources revocable tasks can use.
func (n *resPool) initSlackLimit(cfg *respool.ResourcePoolConfig) {
	slimit := cfg.GetSlackLimit()
	if slimit == nil {
		slimit = &respool.SlackLimit{
			MaxPercent: _defaultSlackLimit,
		}
	}

	slackLimit := &scalar.Resources{}
	multiplier := slimit.GetMaxPercent() / 100
	for kind, res := range n.resourceConfigs {
		switch kind {
		case common.CPU:
			slackLimit.CPU = 0
		case common.GPU:
			slackLimit.GPU = res.Reservation * multiplier
		case common.MEMORY:
			slackLimit.MEMORY = res.Reservation * multiplier
		case common.DISK:
			slackLimit.DISK = res.Reservation * multiplier
		}
	}
	n.slackLimit = slackLimit

	log.WithFields(log.Fields{
		"slack_limit": slackLimit,
		"respool_id":  n.id,
	}).Info("Setting slack limit")
}

// SlackLimit returns the slack limit of the resource pool
func (n *resPool) GetSlackLimit() *scalar.Resources {
	n.RLock()
	defer n.RUnlock()
	return n.slackLimit
}

// SetEntitlement sets the entitlement of non-revocable resources
// for non-revocable tasks + revocable tasks for this resource pool.
func (n *resPool) SetEntitlement(res *scalar.Resources) {
	n.Lock()
	defer n.Unlock()
	n.entitlement = res
	log.WithFields(log.Fields{
		"respool_id":  n.id,
		"entitlement": res,
	}).Debug("Setting Entitlement")
}

// SetSlackEntitlement sets the entitlement of revocable cpus
// + non-revocable resources [mem, disk, gpu] for the resource pool
func (n *resPool) SetSlackEntitlement(res *scalar.Resources) {
	n.Lock()
	defer n.Unlock()
	n.slackEntitlement = res
	log.WithFields(log.Fields{
		"respool_id":        n.id,
		"slack_entitlement": res,
	}).Debug("Setting Slack Entitlement")
}

// SetNonSlackEntitlement Sets the entitlement of non-revocable resources
// for non-revocable tasks for this resource pool
func (n *resPool) SetNonSlackEntitlement(res *scalar.Resources) {
	n.Lock()
	defer n.Unlock()
	n.nonSlackEntitlement = res
}

// GetEntitlement returns the total entitlement of non-revocable resources
// for this resource pool.
func (n *resPool) GetEntitlement() *scalar.Resources {
	n.RLock()
	defer n.RUnlock()
	return n.entitlement
}

// GetSlackEntitlement returns the entitlement for revocable tasks.
func (n *resPool) GetSlackEntitlement() *scalar.Resources {
	n.RLock()
	defer n.RUnlock()
	return n.slackEntitlement
}

// GetNonSlackEntitlement returns the entitlement for non-revocable tasks.
func (n *resPool) GetNonSlackEntitlement() *scalar.Resources {
	n.RLock()
	defer n.RUnlock()
	return n.nonSlackEntitlement
}

// GetTotalAllocatedResources gets the resource allocation for the pool
func (n *resPool) GetTotalAllocatedResources() *scalar.Resources {
	n.RLock()
	defer n.RUnlock()
	return n.allocation.GetByType(scalar.TotalAllocation)
}

// GetSlackAllocatedResources gets the resource allocation for the pool
func (n *resPool) GetSlackAllocatedResources() *scalar.Resources {
	n.RLock()
	defer n.RUnlock()
	return n.allocation.GetByType(scalar.SlackAllocation)
}

// GetNonSlackAllocatedResources returns resources allocated to non-revocable tasks.
func (n *resPool) GetNonSlackAllocatedResources() *scalar.Resources {
	n.RLock()
	defer n.RUnlock()
	return n.allocation.GetByType(scalar.NonSlackAllocation)
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

// GetSlackDemand gets the resource demand for the pool
func (n *resPool) GetSlackDemand() *scalar.Resources {
	n.RLock()
	defer n.RUnlock()
	return n.slackDemand
}

// SubtractFromAllocation updates the allocation for the resource pool
func (n *resPool) SubtractFromAllocation(allocation *scalar.Allocation) error {
	n.Lock()
	defer n.Unlock()

	newAllocation := n.allocation.Subtract(allocation)

	if newAllocation == nil {
		return errors.Errorf("couldn't update the resources")
	}
	n.allocation = newAllocation

	log.WithFields(log.Fields{
		"respool_id": n.id,
		"total_alloc": n.allocation.GetByType(
			scalar.TotalAllocation),
		"non_preemptible_alloc": n.allocation.GetByType(
			scalar.NonPreemptibleAllocation),
		"controller_alloc": n.allocation.GetByType(
			scalar.ControllerAllocation),
		"slack_alloc": n.allocation.GetByType(
			scalar.SlackAllocation),
	}).Debug("Current Allocation after subtracting allocation")

	return nil
}

// IsRoot returns true if the node is the root in the resource
// pool hierarchy
func (n *resPool) IsRoot() bool {
	n.RLock()
	defer n.RUnlock()
	return n.isRoot()
}

func (n *resPool) isRoot() bool {
	return n.id == common.RootResPoolID
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
	defer n.RUnlock()
	return n.path
}

func (n *resPool) calculatePath() string {
	if n.isRoot() {
		return ResourcePoolPathDelimiter
	}
	if n.parent == nil || n.parent.IsRoot() {
		return ResourcePoolPathDelimiter + n.poolConfig.Name
	}
	return n.parent.GetPath() + ResourcePoolPathDelimiter + n.poolConfig.Name
}

// AddToAllocation adds resources to the allocation
// for the resource pool
func (n *resPool) AddToAllocation(allocation *scalar.Allocation) error {
	n.Lock()
	defer n.Unlock()

	n.allocation = n.allocation.Add(allocation)

	log.WithFields(log.Fields{
		"respool_id": n.id,
		"total_alloc": n.allocation.GetByType(
			scalar.TotalAllocation),
		"non_preemptible_alloc": n.allocation.GetByType(
			scalar.NonPreemptibleAllocation),
		"controller_alloc": n.allocation.GetByType(
			scalar.ControllerAllocation),
		"slack_alloc": n.allocation.GetByType(
			scalar.SlackAllocation),
	}).Debug("Current Allocation after adding allocation")

	return nil
}

// AddToDemand adds resources to the demand
// for the resource pool
func (n *resPool) AddToDemand(res *scalar.Resources) error {
	n.Lock()
	defer n.Unlock()

	n.demand = n.demand.Add(res)

	log.WithFields(log.Fields{
		"respool_id": n.id,
		"demand":     n.demand,
	}).Debug("Current Demand after Adding resources")

	return nil
}

// AddToSlackDemand adds resources to the slack demand
// for the resource pool
func (n *resPool) AddToSlackDemand(res *scalar.Resources) error {
	n.Lock()
	defer n.Unlock()

	n.slackDemand = n.slackDemand.Add(res)

	log.WithFields(log.Fields{
		"respool_id": n.id,
		"demand":     n.slackDemand,
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

	log.
		WithField("respool_id", n.id).
		WithField("demand", n.demand).
		Debug("Current Demand " +
			"after removing resources")

	return nil
}

// SubtractFromSlackDemand subtracts resources from demand
// for the resource pool
func (n *resPool) SubtractFromSlackDemand(res *scalar.Resources) error {
	n.Lock()
	defer n.Unlock()

	newDemand := n.slackDemand.Subtract(res)
	if newDemand == nil {
		return errors.Errorf("Couldn't update the resources")
	}
	n.slackDemand = newDemand

	log.WithFields(log.Fields{
		"respool_id":   n.id,
		"slack_demand": n.slackDemand,
	}).Debug("Current Demand after removing resources")

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
	if n.slackLimit != nil {
		n.metrics.SlackLimit.Update(n.slackLimit)
	}
}

// updates dynamic metrics(Allocation, Entitlement, Available) which change based on
// usage and entitlement calculation
// ToDo (varung): expose available slack resources
func (n *resPool) updateDynamicResourceMetrics() {
	n.metrics.TotalEntitlement.Update(n.entitlement)
	n.metrics.SlackEntitlement.Update(n.slackEntitlement)
	n.metrics.NonSlackEntitlement.Update(n.nonSlackEntitlement)

	n.metrics.TotalAllocation.Update(n.allocation.GetByType(
		scalar.TotalAllocation))
	n.metrics.SlackAllocation.Update(n.allocation.GetByType(
		scalar.SlackAllocation))
	n.metrics.NonPreemptibleAllocation.Update(n.allocation.GetByType(
		scalar.NonPreemptibleAllocation))
	n.metrics.ControllerAllocation.Update(n.allocation.GetByType(
		scalar.ControllerAllocation))
	n.metrics.NonSlackAllocation.Update(n.allocation.GetByType(
		scalar.NonSlackAllocation))

	n.metrics.NonSlackAvailable.Update(n.nonSlackEntitlement.
		Subtract(n.allocation.GetByType(scalar.NonSlackAllocation)))
	n.metrics.SlackAvailable.Update(n.slackEntitlement.
		Subtract(n.allocation.GetByType(scalar.SlackAllocation)))

	n.metrics.Demand.Update(n.demand)
	n.metrics.SlackDemand.Update(n.slackDemand)

	n.metrics.PendingQueueSize.Update(float64(n.aggregateQueueByType(PendingQueue)))
	n.metrics.RevocableQueueSize.Update(float64(n.aggregateQueueByType(RevocableQueue)))
	n.metrics.ControllerQueueSize.Update(float64(n.aggregateQueueByType(ControllerQueue)))
	n.metrics.NPQueueSize.Update(float64(n.aggregateQueueByType(NonPreemptibleQueue)))
}

// aggregateQueueByType aggreagates the queue size for leaf resource pools
func (n *resPool) aggregateQueueByType(qt QueueType) int {
	if n.isLeaf() {
		return n.queue(qt).Size()
	}

	queueSize := 0
	for child := n.children.Front(); child != nil; child = child.Next() {
		if childResPool, ok := child.Value.(*resPool); ok {
			queueSize += childResPool.aggregateQueueByType(qt)
		}
	}
	return queueSize
}

// updates all the metrics (static and dynamic)
func (n *resPool) UpdateResourceMetrics() {
	n.RLock()
	defer n.RUnlock()
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
	case RevocableQueue:
		return n.revocableQueue.Peek(limit)
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
