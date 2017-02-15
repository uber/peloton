package respool

import (
	"peloton/api/respool"

	"code.uber.internal/infra/peloton/master/metrics"
	rmconfig "code.uber.internal/infra/peloton/resmgr/config"
	"code.uber.internal/infra/peloton/storage"

	"container/list"

	log "github.com/Sirupsen/logrus"
)

// InitTree will be initializing the respool tree
func InitTree(
	config *rmconfig.Config,
	store storage.ResourcePoolStore,
	metrics *metrics.Metrics) *Tree {

	service := Tree{
		resPoolTree: nil,
		store:       store,
	}
	service.allNodes = make(map[string]*Node)
	return &service
}

// Tree will be storing the Tree for respools
type Tree struct {
	resPoolTree *Node
	store       storage.ResourcePoolStore
	resPools    map[string]*respool.ResourcePoolConfig
	// Hashmap of [ID] = Node, having all Nodes
	allNodes map[string]*Node
}

// StartResPool will start the respool tree
func (r *Tree) StartResPool() {
	resPools, err := r.store.GetAllResourcePools()
	if err != nil {
		log.WithField("Error", err).Error("GetAllResourcePools failed")
		return
	}
	r.resPools = resPools
	// Initializing the respoolTree
	r.resPoolTree = r.initializeResourceTree()
}

// StopResPool will stop the respool tree
func (r *Tree) StopResPool() {
	// TODO: Need to be done
}

// initializeResourceTree will initialize all the resource pools from Storage
func (r *Tree) initializeResourceTree() *Node {
	log.Info("Initializing Resource Tree")
	if r.resPools == nil {
		return nil
	}
	root := r.createTree(nil, RootResPoolID, r.resPools, r.allNodes)
	r.printTree(root)
	return root
}

// createTree function will take the Parent node and create the tree underneath
func (r *Tree) createTree(
	parent *Node,
	ID string,
	resPools map[string]*respool.ResourcePoolConfig,
	allNodes map[string]*Node) *Node {
	node := NewNode(ID, parent, resPools[ID])
	allNodes[ID] = node
	node.SetParent(parent)
	childs := r.getChildResPools(ID, resPools)
	var childNodes = list.New()
	// TODO: We need to detect cycle here.
	for child := range childs {
		childNode := r.createTree(node, child, resPools, allNodes)
		childNodes.PushBack(childNode)
	}
	node.SetChildren(childNodes)
	return node
}

// printTree will print the whole Resource Pool Tree in BFS manner
func (r *Tree) printTree(root *Node) {
	var queue list.List
	queue.PushBack(root)
	for queue.Len() != 0 {
		n := queue.Front()
		queue.Remove(n)
		nodeVar := n.Value.(*Node)
		log.WithField("Node", nodeVar.ID).Info()
		nodeVar.logNodeResources(nodeVar)
		children := nodeVar.GetChildren()
		for e := children.Front(); e != nil; e = e.Next() {
			queue.PushBack(e.Value.(*Node))
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
func (r *Tree) GetResPoolRoot() *Node {
	return r.resPoolTree
}
