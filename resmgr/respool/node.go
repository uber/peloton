package respool

import (
	"container/list"
	"peloton/api/respool"

	log "github.com/Sirupsen/logrus"
)

// Node this is the struct which will be holding the resource pool
type Node struct {
	// TODO: We need to add synchronization support
	ID              string
	children        *list.List
	parent          *Node
	name            string
	resourceConfigs map[string]*respool.ResourceConfig
	respoolConfig   *respool.ResourcePoolConfig
}

// NewNode will intializing the resource pool node and return that
func NewNode(
	ID string,
	parent *Node,
	respoolConfig *respool.ResourcePoolConfig) *Node {
	result := Node{
		children:        list.New(),
		ID:              ID,
		parent:          parent,
		resourceConfigs: make(map[string]*respool.ResourceConfig),
		respoolConfig:   respoolConfig,
	}
	result.initResources(respoolConfig)
	return &result
}

// SetParent will be setting the parent for the resource pool
func (n *Node) SetParent(parent *Node) {
	n.parent = parent
}

// SetChildren will be setting the children for the resource pool
func (n *Node) SetChildren(children *list.List) {
	n.children = children
}

// GetChildren will be getting the children for the resource pool
func (n *Node) GetChildren() *list.List {
	return n.children
}

// initResources will initializing the resource config under resource pool
func (n *Node) initResources(config *respool.ResourcePoolConfig) {
	resList := config.GetResources()
	for _, res := range resList {
		n.resourceConfigs[res.Kind] = res
	}
}

// logNodeResources will be printing the resources for the resource pool
func (n *Node) logNodeResources(node *Node) {
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
func (n *Node) SetResources(resources *respool.ResourceConfig) {
	if resources == nil {
		return
	}
	n.resourceConfigs[resources.Kind] = resources
}

// Isleaf will tell us if this resource pool is leaf or not
func (n *Node) Isleaf() bool {
	return (n.GetChildren().Len() == 0)
}
