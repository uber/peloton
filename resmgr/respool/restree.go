package respool

import (
	"container/list"
	"fmt"
	"sync"

	"peloton/api/respool"

	"code.uber.internal/infra/peloton/storage"

	log "github.com/Sirupsen/logrus"
	"github.com/uber-go/tally"
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

	// LookupResPool returns a respool node by the given ID
	LookupResPool(ID *respool.ResourcePoolID) (*ResPool, error)

	// GetAllNodes returns all respool nodes or all leaf respool nodes.
	GetAllNodes(leafOnly bool) *list.List

	// FIXME: the following functions are used by
	// scheduler_test.go. Need to mock the ResourcePoolStore interface
	// in the test so that we don't leak these functions.
	SetAllNodes(nodes *map[string]*ResPool)
	CreateTree(parent *ResPool, ID string,
		resPools map[string]*respool.ResourcePoolConfig,
		allNodes map[string]*ResPool) *ResPool
}

// tree implements the Tree interface
type tree struct {
	sync.RWMutex

	store    storage.ResourcePoolStore
	metrics  *Metrics
	root     *ResPool
	resPools map[string]*respool.ResourcePoolConfig
	// Hashmap of [ID] = Node, having all Nodes
	allNodes map[string]*ResPool
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
		allNodes: make(map[string]*ResPool),
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
		log.WithField("Error", err).Error("GetAllResourcePools failed")
		return err
	}
	t.resPools = resPools
	if len(resPools) == 0 {
		log.Warnf("There are no resource pools existing")
		return nil
	}
	// Initializing the respoolTree
	t.root = t.initializeResourceTree()
	return nil
}

// Stop will stop the respool tree
func (t *tree) Stop() error {
	// TODO: Need to be done
	return nil
}

// initializeResourceTree will initialize all the resource pools from Storage
func (t *tree) initializeResourceTree() *ResPool {
	log.Info("Initializing Resource Tree")
	if t.resPools == nil {
		return nil
	}
	rootResPoolConfig := respool.ResourcePoolConfig{
		Name:   "root",
		Parent: nil,
		Policy: respool.SchedulingPolicy_PriorityFIFO,
	}
	t.resPools[RootResPoolID] = &rootResPoolConfig
	root := t.CreateTree(nil, RootResPoolID, t.resPools, t.allNodes)
	return root
}

// CreateTree function will take the Parent node and create the tree underneath
func (t *tree) CreateTree(
	parent *ResPool,
	ID string,
	resPools map[string]*respool.ResourcePoolConfig,
	allNodes map[string]*ResPool) *ResPool {
	node := NewRespool(ID, parent, resPools[ID])
	allNodes[ID] = node
	node.SetParent(parent)
	childs := t.getChildResPools(ID, resPools)
	var childNodes = list.New()
	// TODO: We need to detect cycle here.
	for child := range childs {
		childNode := t.CreateTree(node, child, resPools, allNodes)
		childNodes.PushBack(childNode)
	}
	node.SetChildren(childNodes)
	return node
}

// printTree will print the whole Resource Pool Tree in BFS manner
func (t *tree) printTree(root *ResPool) {
	var queue list.List
	queue.PushBack(root)
	for queue.Len() != 0 {
		n := queue.Front()
		queue.Remove(n)
		nodeVar := n.Value.(*ResPool)
		log.WithField("Node", nodeVar.ID).Info()
		nodeVar.logNodeResources(nodeVar)
		children := nodeVar.GetChildren()
		for e := children.Front(); e != nil; e = e.Next() {
			queue.PushBack(e.Value.(*ResPool))
		}
	}
}

// getChildResPools will return map[respoolid] = respoolConfig
func (t *tree) getChildResPools(parentID string,
	resPools map[string]*respool.ResourcePoolConfig) map[string]*respool.ResourcePoolConfig {
	childs := make(map[string]*respool.ResourcePoolConfig)
	for respool, respoolConf := range resPools {
		if respoolConf.Parent.GetValue() == parentID {
			childs[respool] = respoolConf
		}
	}
	return childs
}

// getRoot will return the root node for the resource pool tree
func (t *tree) getRoot() *ResPool {
	// TODO: Need to clone the tree
	return t.root
}

// GetAllNodes returns all the leaf nodes in the tree
func (t *tree) GetAllNodes(leafOnly bool) *list.List {
	// TODO: we need to merge GetAllNodes with GetAllLeafNodes
	t.RLock()
	defer t.RUnlock()
	nodesList := new(list.List)
	for _, n := range t.allNodes {
		if !leafOnly || n.Isleaf() {
			nodesList.PushBack(n)
		}
	}
	return nodesList
}

// SetAllNodes sets all nodes in the tree
func (t *tree) SetAllNodes(nodes *map[string]*ResPool) {
	t.Lock()
	defer t.Unlock()
	t.allNodes = *nodes
}

// LookupResPool returns the resource pool for the given resource pool ID
func (t *tree) LookupResPool(ID *respool.ResourcePoolID) (*ResPool, error) {
	t.RLock()
	defer t.RUnlock()
	if val, ok := t.allNodes[ID.Value]; ok {
		return val, nil
	}
	return nil, fmt.Errorf("Resource pool (%s) not found", ID.Value)
}
