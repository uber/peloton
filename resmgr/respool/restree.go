package respool

import (
	"container/list"
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/uber-go/tally"

	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"

	"code.uber.internal/infra/peloton/storage"
)

// Tree defines the interface for a Resource Pool Tree
type Tree interface {
	// Start starts a respooltree by loading all respools
	// and pending tasks from DB. This should be called when a
	// resource manager gains the leadership.
	Start() error

	// Stop resets a respool tree when a resource manager lost the
	// leadership.
	Stop() error

	// Get returns a respool node by the given ID
	Get(ID *respool.ResourcePoolID) (ResPool, error)

	// GetByPath returns the respool node by the given path
	GetByPath(path *respool.ResourcePoolPath) (ResPool, error)

	// GetAllNodes returns all respool nodes or all leaf respool nodes.
	GetAllNodes(leafOnly bool) *list.List

	// Upsert add/update a resource pool poolConfig to the tree
	Upsert(ID *respool.ResourcePoolID, resPoolConfig *respool.ResourcePoolConfig) error

	// UpdatedChannel is written to whenever the resource tree is changed. There
	// may be only one event for multiple updates.
	// TODO: Redo package imports, such that a method Calculator.SuggestRefresh
	// can be used instead, without breaking circular imports.
	UpdatedChannel() <-chan struct{}
}

// tree implements the Tree interface
type tree struct {
	sync.RWMutex

	store   storage.ResourcePoolStore
	metrics *Metrics
	root    ResPool
	// map of [ID] = ResPool
	resPools    map[string]ResPool
	jobStore    storage.JobStore
	taskStore   storage.TaskStore
	scope       tally.Scope
	updatedChan chan struct{}
}

// Singleton resource pool tree
var respoolTree *tree

// InitTree will be initializing the respool tree
func InitTree(
	scope tally.Scope,
	store storage.ResourcePoolStore,
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
) {

	if respoolTree != nil {
		log.Warning("Resource pool tree has already been initialized")
		return
	}

	respoolTree = &tree{
		store:       store,
		root:        nil,
		metrics:     NewMetrics(scope),
		resPools:    make(map[string]ResPool),
		jobStore:    jobStore,
		taskStore:   taskStore,
		scope:       scope.SubScope("restree"),
		updatedChan: make(chan struct{}, 1),
	}
}

// GetTree returns the interface of a Resource Pool Tree. This
// function assumes the tree has been initialized as part of the
// InitTree function.
func GetTree() Tree {
	if respoolTree == nil {
		log.Fatal("Resource pool tree is not initialized")
	}
	return respoolTree
}

// Start will start the respool tree by loading respools and tasks
// from storage
func (t *tree) Start() error {
	resPoolConfigs, err := t.store.GetAllResourcePools(context.Background())
	if err != nil {
		log.WithError(err).Error("GetAllResourcePools failed")
		return err
	}
	// Initializing the respoolTree
	t.root, err = t.initTree(resPoolConfigs)
	if err != nil {
		log.WithError(err).Error("initializeResourceTree failed")
		return errors.Wrap(err, "failed to start tree")
	}
	return nil
}

// Stop will stop the respool tree
func (t *tree) Stop() error {
	// TODO cleanup the queues?
	log.Info("Stopping Resource Pool Tree")
	t.Lock()
	defer t.Unlock()
	t.root = nil
	t.resPools = make(map[string]ResPool)
	log.Info("Resource Pool Tree Stopped")
	return nil
}

func (t *tree) UpdatedChannel() <-chan struct{} {
	return t.updatedChan
}

// initTree will initialize all the resource pools from Storage
func (t *tree) initTree(
	resPoolConfigs map[string]*respool.ResourcePoolConfig) (ResPool, error) {
	log.Info("Initializing Resource Tree")

	if resPoolConfigs == nil {
		return nil, errors.New("resPoolConfigs cannot be nil")
	}

	if len(resPoolConfigs) == 0 {
		// We should not return from here
		log.Warnf("There are no resource pools existing")
	}

	// initialize root resource pool config
	resPoolConfigs[RootResPoolID] = &respool.ResourcePoolConfig{
		Name:   RootResPoolID,
		Parent: nil,
		Policy: respool.SchedulingPolicy_PriorityFIFO,
	}

	root, err := t.buildTree(RootResPoolID, nil, resPoolConfigs)
	if err != nil {
		return nil, errors.Wrap(
			err,
			"failed to initialize tree")
	}
	log.Info("Resource Tree initialized successfully")
	return root, nil
}

// buildTree function will take the Parent node and create the tree underneath
func (t *tree) buildTree(
	ID string,
	parent ResPool,
	resPoolConfigs map[string]*respool.ResourcePoolConfig,
) (ResPool, error) {
	node, err := NewRespool(t.scope, ID, parent, resPoolConfigs[ID])
	if err != nil {
		log.WithError(err).Error("Error creating resource pool")
		return nil, err
	}

	t.resPools[ID] = node
	node.SetParent(parent)
	childConfigs := t.getChildResPoolConfigs(ID, resPoolConfigs)
	var childResourcePools = list.New()
	// TODO: We need to detect cycle here.
	for childResPoolID := range childConfigs {
		childNode, err := t.buildTree(childResPoolID, node, resPoolConfigs)
		if err != nil {
			return nil, errors.Wrapf(
				err,
				"failed to create resource pool: %s",
				childResPoolID)
		}
		childResourcePools.PushBack(childNode)
	}
	node.SetChildren(childResourcePools)
	return node, nil
}

// printTree will print the whole Resource Pool Tree in BFS manner
func (t *tree) printTree(root ResPool) {
	var queue list.List
	queue.PushBack(root)
	for queue.Len() != 0 {
		n := queue.Front()
		queue.Remove(n)
		nodeVar := n.Value.(*resPool)
		log.WithField("ResPool", nodeVar.ID).Info()
		nodeVar.logNodeResources()
		children := nodeVar.Children()
		for e := children.Front(); e != nil; e = e.Next() {
			queue.PushBack(e.Value.(*resPool))
		}
	}
}

// getChildResPoolConfigs will return map[respoolid] = respoolConfig for a
// parent resource pool
func (t *tree) getChildResPoolConfigs(
	parentID string,
	resPoolConfigs map[string]*respool.ResourcePoolConfig,
) map[string]*respool.ResourcePoolConfig {
	childRespoolConfigs := make(map[string]*respool.ResourcePoolConfig)
	for respool, respoolConf := range resPoolConfigs {
		if respoolConf.Parent.GetValue() == parentID {
			childRespoolConfigs[respool] = respoolConf
		}
	}
	return childRespoolConfigs
}

// getRoot will return the root node for the resource pool tree
func (t *tree) getRoot() ResPool {
	// TODO: Need to clone the tree
	return t.root
}

// GetAllNodes returns all the leaf nodes in the tree
func (t *tree) GetAllNodes(leafOnly bool) *list.List {
	t.RLock()
	defer t.RUnlock()
	nodesList := new(list.List)
	for _, n := range t.resPools {
		if !leafOnly || n.IsLeaf() {
			nodesList.PushBack(n)
		}
	}
	return nodesList
}

// SetAllNodes sets all nodes in the tree
func (t *tree) SetAllNodes(nodes *map[string]ResPool) {
	t.Lock()
	defer t.Unlock()
	t.resPools = *nodes
}

// Get returns resource pool config for the given resource pool
func (t *tree) Get(ID *respool.ResourcePoolID) (ResPool, error) {
	t.RLock()
	defer t.RUnlock()
	return t.lookupResPool(ID)
}

// GetByPath returns the respool node by the given path
// This function assumes the path provided is valid
func (t *tree) GetByPath(path *respool.ResourcePoolPath) (ResPool, error) {
	t.RLock()
	defer t.RUnlock()

	if t.root == nil {
		return nil, errors.Errorf("resource pool is not initialized")
	}

	if path.Value == ResourcePoolPathDelimiter {
		return t.root, nil
	}

	resPath := t.trimPath(path)
	nodes := strings.Split(resPath, ResourcePoolPathDelimiter)

	resPool, err := t.walkTree(t.root, nodes)
	if err != nil {
		return nil, errors.Wrapf(err, "unbale to find resource pool with path:%s", path.Value)
	}
	return resPool, nil
}

// trims the path of the
func (t *tree) trimPath(path *respool.ResourcePoolPath) string {
	return strings.TrimPrefix(
		strings.TrimSuffix(path.Value, ResourcePoolPathDelimiter),
		ResourcePoolPathDelimiter,
	)
}

// Upsert adds/updates a resource pool config to the tree
func (t *tree) Upsert(ID *respool.ResourcePoolID, resPoolConfig *respool.ResourcePoolConfig) error {
	// acquire RW lock
	t.Lock()
	defer t.Unlock()

	parentID := resPoolConfig.Parent

	// check if parent exits
	parent, err := t.lookupResPool(parentID)
	if err != nil {
		// parent is <nil>
		return errors.Wrap(err, "parent does not exists")
	}

	// check if already exists, and log
	resourcePool, _ := t.lookupResPool(ID)

	if resourcePool != nil {
		// update existing respool
		log.WithFields(log.Fields{
			"Id": ID.Value,
		}).Debug("Updating resource pool")

		// TODO update only if leaf node ???
		resourcePool.SetResourcePoolConfig(resPoolConfig)
	} else {
		// add resource pool
		log.WithFields(log.Fields{
			"Id": ID.Value,
		}).Debug("Adding resource pool")

		resourcePool, err = NewRespool(t.scope, ID.Value, parent, resPoolConfig)

		if err != nil {
			return errors.Wrapf(
				err,
				"failed to insert resource pool: %s",
				ID.Value)
		}

		// link parent to child resource pool
		children := parent.Children()
		children.PushBack(resourcePool)
	}

	t.resPools[ID.Value] = resourcePool

	select {
	case t.updatedChan <- struct{}{}:
	default:
	}

	return nil
}

// Returns the resource pool for the given resource pool ID
func (t *tree) lookupResPool(ID *respool.ResourcePoolID) (ResPool, error) {
	if val, ok := t.resPools[ID.Value]; ok {
		return val, nil
	}
	return nil, fmt.Errorf("Resource pool (%s) not found", ID.Value)
}

// Recursively walks the tree beneath the root based on resource pool names
func (t *tree) walkTree(root ResPool, nodes []string) (ResPool, error) {
	if len(nodes) == 0 {
		// found the node
		return root, nil
	}

	children := root.Children()
	for e := children.Front(); e != nil; e = e.Next() {
		child, _ := e.Value.(ResPool)
		if child.Name() == nodes[0] {
			// walk again with the child as the new root
			return t.walkTree(child, nodes[1:])
		}
	}

	return nil, errors.Errorf("Resource pool (%s) not found", nodes)
}
