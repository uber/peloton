package respool

import (
	"peloton/api/respool"
	"sync"

	"code.uber.internal/infra/peloton/storage"
	"container/list"
	"fmt"
	log "github.com/Sirupsen/logrus"
)

// InitTree will be initializing the respool tree
func InitTree(
	store storage.ResourcePoolStore,
	metrics *Metrics) *Tree {

	service := Tree{
		resPoolTree: nil,
		store:       store,
	}
	service.allNodes = make(map[string]*ResPool)
	return &service
}

// Tree will be storing the Tree for respools
type Tree struct {
	sync.RWMutex
	resPoolTree *ResPool
	store       storage.ResourcePoolStore
	resPools    map[string]*respool.ResourcePoolConfig
	// Hashmap of [ID] = Node, having all Nodes
	allNodes map[string]*ResPool
}

// StartResPool will start the respool tree
func (r *Tree) StartResPool() error {
	resPools, err := r.store.GetAllResourcePools()
	if err != nil {
		log.WithField("Error", err).Error("GetAllResourcePools failed")
		return err
	}
	r.resPools = resPools
	if len(resPools) == 0 {
		log.Warnf("There are no resource pools existing")
		return nil
	}
	// Initializing the respoolTree
	r.resPoolTree = r.initializeResourceTree()
	return nil
}

// StopResPool will stop the respool tree
func (r *Tree) StopResPool() {
	// TODO: Need to be done
}

// initializeResourceTree will initialize all the resource pools from Storage
func (r *Tree) initializeResourceTree() *ResPool {
	log.Info("Initializing Resource Tree")
	if r.resPools == nil {
		return nil
	}
	rootResPoolConfig := respool.ResourcePoolConfig{
		Name:   "root",
		Parent: nil,
		Policy: respool.SchedulingPolicy_PriorityFIFO,
	}
	r.resPools[RootResPoolID] = &rootResPoolConfig
	root := r.CreateTree(nil, RootResPoolID, r.resPools, r.allNodes)
	return root
}

// CreateTree function will take the Parent node and create the tree underneath
func (r *Tree) CreateTree(
	parent *ResPool,
	ID string,
	resPools map[string]*respool.ResourcePoolConfig,
	allNodes map[string]*ResPool) *ResPool {
	node := NewRespool(ID, parent, resPools[ID])
	allNodes[ID] = node
	node.SetParent(parent)
	childs := r.getChildResPools(ID, resPools)
	var childNodes = list.New()
	// TODO: We need to detect cycle here.
	for child := range childs {
		childNode := r.CreateTree(node, child, resPools, allNodes)
		childNodes.PushBack(childNode)
	}
	node.SetChildren(childNodes)
	return node
}

// printTree will print the whole Resource Pool Tree in BFS manner
func (r *Tree) printTree(root *ResPool) {
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
func (r *Tree) getChildResPools(parentID string,
	resPools map[string]*respool.ResourcePoolConfig) map[string]*respool.ResourcePoolConfig {
	childs := make(map[string]*respool.ResourcePoolConfig)
	for respool, respoolConf := range resPools {
		if respoolConf.Parent.GetValue() == parentID {
			childs[respool] = respoolConf
		}
	}
	return childs
}

// GetResPoolRoot will return the root node for the resource pool tree
func (r *Tree) GetResPoolRoot() *ResPool {
	// TODO: Need to clone the tree
	return r.resPoolTree
}

// GetAllNodes returns all the nodes in the tree
func (r *Tree) GetAllNodes() *list.List {
	r.RLock()
	defer r.Unlock()
	list := new(list.List)
	for _, n := range r.allNodes {
		list.PushBack(n)
	}
	return list
}

// GetAllLeafNodes returns all the leaf nodes in the tree
func (r *Tree) GetAllLeafNodes() *list.List {
	// TODO: we need to merge GetAllNodes with GetAllLeafNodes
	r.RLock()
	defer r.RUnlock()
	list := new(list.List)
	for _, n := range r.allNodes {
		if n.Isleaf() {
			list.PushBack(n)
		}
	}
	return list
}

// SetAllNodes sets all nodes in the tree
func (r *Tree) SetAllNodes(nodes *map[string]*ResPool) {
	r.Lock()
	defer r.Unlock()
	r.allNodes = *nodes
}

// LookupResPool returns the resource pool for the given resource pool ID
func (r *Tree) LookupResPool(ID respool.ResourcePoolID) (*ResPool, error) {
	r.RLock()
	defer r.RUnlock()
	if val, ok := r.allNodes[ID.Value]; ok {
		return val, nil
	}
	return nil, fmt.Errorf("Not able to find Respool %s ", ID.Value)
}
