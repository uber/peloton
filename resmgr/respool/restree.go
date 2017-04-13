package respool

import (
	"container/list"
	"fmt"
	"sync"

	"code.uber.internal/infra/peloton/storage"

	log "github.com/Sirupsen/logrus"
	"github.com/uber-go/tally"

	"peloton/api/respool"

	"github.com/pkg/errors"
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

	// GetAllNodes returns all respool nodes or all leaf respool nodes.
	GetAllNodes(leafOnly bool) *list.List

	//Upsert add/update a resource pool poolConfig to the tree
	Upsert(ID *respool.ResourcePoolID, resPoolConfig *respool.ResourcePoolConfig) error
}

// tree implements the Tree interface
type tree struct {
	sync.RWMutex

	store    storage.ResourcePoolStore
	metrics  *Metrics
	root     ResPool
	resPools map[string]*respool.ResourcePoolConfig
	// Hashmap of [ID] = ResPool, having all Nodes
	allNodes map[string]ResPool
}

// Singleton resource pool tree
var respoolTree *tree

// InitTree will be initializing the respool tree
func InitTree(
	scope tally.Scope,
	store storage.ResourcePoolStore) {

	if respoolTree != nil {
		log.Warning("Resource pool tree has already been initialized")
		return
	}

	respoolTree = &tree{
		store:    store,
		root:     nil,
		metrics:  NewMetrics(scope),
		allNodes: make(map[string]ResPool),
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
	resPools, err := t.store.GetAllResourcePools()
	if err != nil {
		log.WithError(err).Error("GetAllResourcePools failed")
		return err
	}
	t.resPools = resPools
	if len(resPools) == 0 {
		// We should not return from here
		log.Warnf("There are no resource pools existing")
	}
	// Initializing the respoolTree
	t.root, err = t.initializeResourceTree()
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
	t.resPools = nil
	t.allNodes = make(map[string]ResPool)
	log.Info("Resource Pool Tree Stopped")
	return nil
}

// initializeResourceTree will initialize all the resource pools from Storage
func (t *tree) initializeResourceTree() (ResPool, error) {
	log.Info("Initializing Resource Tree")

	// we assume the resPools needs to be created before calling init tree
	if t.resPools == nil {
		return nil, errors.New("respools cannot be nil")
	}

	rootResPoolConfig := respool.ResourcePoolConfig{
		Name:   "root",
		Parent: nil,
		Policy: respool.SchedulingPolicy_PriorityFIFO,
	}
	t.resPools[RootResPoolID] = &rootResPoolConfig
	root, err := t.buildTree(nil, RootResPoolID)
	if err != nil {
		return nil, errors.Wrap(
			err,
			"failed to initialize tree")
	}
	return root, nil
}

// buildTree function will take the Parent node and create the tree underneath
func (t *tree) buildTree(
	parent ResPool,
	ID string,
) (ResPool, error) {
	node, err := NewRespool(ID, parent, t.resPools[ID])
	if err != nil {
		return nil, err
	}

	t.allNodes[ID] = node
	node.SetParent(parent)
	childs := t.getChildResPools(ID)
	var childNodes = list.New()
	// TODO: We need to detect cycle here.
	for child := range childs {
		childNode, err := t.buildTree(node, child)
		if err != nil {
			return nil, errors.Wrapf(
				err,
				"failed to create resource pool: %s",
				child)
		}
		childNodes.PushBack(childNode)
	}
	node.SetChildren(childNodes)
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

// getChildResPools will return map[respoolid] = respoolConfig
func (t *tree) getChildResPools(parentID string) map[string]*respool.ResourcePoolConfig {
	childs := make(map[string]*respool.ResourcePoolConfig)
	for respool, respoolConf := range t.resPools {
		if respoolConf.Parent.GetValue() == parentID {
			childs[respool] = respoolConf
		}
	}
	return childs
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
	for _, n := range t.allNodes {
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
	t.allNodes = *nodes
}

// Get returns resource pool config for the given resource pool
func (t *tree) Get(ID *respool.ResourcePoolID) (ResPool, error) {
	t.RLock()
	defer t.RUnlock()
	return t.lookupResPool(ID)
}

// Returns the resource pool for the given resource pool ID
func (t *tree) lookupResPool(ID *respool.ResourcePoolID) (ResPool, error) {
	if val, ok := t.allNodes[ID.Value]; ok {
		return val, nil
	}
	return nil, fmt.Errorf("Resource pool (%s) not found", ID.Value)
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

		resourcePool, err = NewRespool(ID.Value, parent, resPoolConfig)

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

	t.allNodes[ID.Value] = resourcePool
	t.resPools[ID.Value] = resPoolConfig

	return nil
}
