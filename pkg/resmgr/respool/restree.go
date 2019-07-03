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
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/respool"

	"github.com/uber/peloton/pkg/common"
	rc "github.com/uber/peloton/pkg/resmgr/common"
	"github.com/uber/peloton/pkg/storage"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

// Tree defines the interface for a Resource Pool Tree
type Tree interface {
	// Start initializes the respool tree by loading all resource pools
	// from DB. This should be called when a
	// resource manager gains the leadership.
	Start() error

	// Stop resets a respool tree when a resource manager lost the
	// leadership.
	Stop() error

	// Get returns a respool node by the given ID
	Get(ID *peloton.ResourcePoolID) (ResPool, error)

	// GetByPath returns the respool node by the given path
	GetByPath(path *respool.ResourcePoolPath) (ResPool, error)

	// GetAllNodes returns all respool nodes or all leaf respool nodes.
	GetAllNodes(leafOnly bool) *list.List

	// Upsert add/update a resource pool poolConfig to the tree
	Upsert(ID *peloton.ResourcePoolID, resPoolConfig *respool.ResourcePoolConfig) error

	// UpdatedChannel is written to whenever the resource tree is changed. There
	// may be only one event for multiple updates.
	// TODO: Redo package imports, such that a method Calculator.SuggestRefresh
	// can be used instead, without breaking circular imports.
	UpdatedChannel() <-chan struct{}

	// Delete deletes the resource pool from the tree
	Delete(ID *peloton.ResourcePoolID) error
}

// tree implements the Tree interface
type tree struct {
	sync.RWMutex // Mutes for synchronization for tree

	preemptionConfig rc.PreemptionConfig

	respoolOps ormobjects.ResPoolOps // resource pool related operations
	metrics    *Metrics              // Metrics object for reporting
	root       ResPool
	// map of [ID] = ResPool
	resPools    map[string]ResPool // Map of all the respools in Tree indexed by ID
	jobStore    storage.JobStore   // Keeping the jobstore object
	taskStore   storage.TaskStore  // Keeping Task store object within tree
	scope       tally.Scope        // Parent scope for the metrics
	updatedChan chan struct{}      // Channel to update all the changes in tree
}

// NewTree will initializing the respool tree
func NewTree(
	scope tally.Scope,
	respoolOps ormobjects.ResPoolOps,
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	cfg rc.PreemptionConfig,
) Tree {
	return &tree{
		respoolOps:       respoolOps,
		root:             nil,
		metrics:          NewMetrics(scope),
		resPools:         make(map[string]ResPool),
		jobStore:         jobStore,
		taskStore:        taskStore,
		scope:            scope.SubScope("restree"),
		updatedChan:      make(chan struct{}, 1),
		preemptionConfig: cfg,
	}
}

// Start initializes the in-memory respool tree by loading resource pools
// from storage.
func (t *tree) Start() error {
	resPoolConfigs, err := t.respoolOps.GetAll(context.Background())
	if err != nil {
		log.WithError(err).Error(
			"failed to get resource pool configs from store")
		return err
	}

	// Initializing the respoolTree
	t.root, err = t.initTree(resPoolConfigs)
	if err != nil {
		log.WithError(err).Error("failed to initialize resource tree")
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
		return nil, errors.New("resource pool configs cannot be nil")
	}

	if len(resPoolConfigs) == 0 {
		// We should not return from here
		log.Warn("There are no existing resource pools")
	}

	// initialize root resource pool config
	resPoolConfigs[common.RootResPoolID] = &respool.ResourcePoolConfig{
		Name:   common.RootResPoolID,
		Parent: nil,
		Policy: respool.SchedulingPolicy_PriorityFIFO,
	}

	root, err := t.buildTree(common.RootResPoolID, nil, resPoolConfigs)
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
	node, err := NewRespool(t.scope, ID, parent, resPoolConfigs[ID], t.preemptionConfig)
	if err != nil {
		log.WithError(err).Error("failed to create resource pool")
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

// GetAllNodes returns all the nodes in the tree based on the supplied isLeaf
// argument
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

// Get returns resource pool config for the given resource pool
func (t *tree) Get(ID *peloton.ResourcePoolID) (ResPool, error) {
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
		return nil, errors.Wrapf(err, "unable to find resource pool with "+
			"path:%s", path.Value)
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
func (t *tree) Upsert(ID *peloton.ResourcePoolID, resPoolConfig *respool.ResourcePoolConfig) error {
	// acquire RW lock
	t.Lock()
	defer t.Unlock()

	parentID := resPoolConfig.Parent

	// check if parent exits
	parent, err := t.lookupResPool(parentID)
	if err != nil {
		return errors.Wrap(err, "parent does not exists")
	}

	// check if already exists, and log
	resourcePool, _ := t.lookupResPool(ID)

	if resourcePool != nil {
		// update existing respool
		log.WithFields(log.Fields{
			"respool_ID": ID.Value,
		}).Debug("Updating resource pool")

		// TODO update only if leaf node ???
		resourcePool.SetResourcePoolConfig(resPoolConfig)
	} else {
		// add resource pool
		log.WithFields(log.Fields{
			"respool_ID": ID.Value,
		}).Debug("Adding resource pool")

		resourcePool, err = NewRespool(t.scope, ID.Value, parent,
			resPoolConfig, t.preemptionConfig)

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
func (t *tree) lookupResPool(ID *peloton.ResourcePoolID) (ResPool, error) {
	if val, ok := t.resPools[ID.Value]; ok {
		return val, nil
	}
	return nil, fmt.Errorf("resource pool (%s) not found", ID.Value)
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

	return nil, errors.Errorf("resource pool (%s) not found", nodes)
}

func (t *tree) Delete(respoolID *peloton.ResourcePoolID) error {
	t.Lock()
	defer t.Unlock()

	// Lookup the resource pool.
	resPool, err := t.lookupResPool(respoolID)
	if err != nil {
		return err
	}
	// Get the parent.
	parent := resPool.Parent()

	if parent == nil {
		return fmt.Errorf("parent is nil")
	}
	children := parent.Children()
	newChildren := list.New()

	for e := children.Front(); e != nil; e = e.Next() {
		child, _ := e.Value.(ResPool)
		if child.ID() != respoolID.Value {
			newChildren.PushBack(child)
		}
	}
	// Updating the parent's children.
	parent.SetChildren(newChildren)

	// Delete all children from the internal list.
	for e := resPool.Children().Front(); e != nil; e = e.Next() {
		child, _ := e.Value.(ResPool)
		delete(t.resPools, child.ID())
	}
	// delete the node itself
	delete(t.resPools, respoolID.Value)

	return nil
}
