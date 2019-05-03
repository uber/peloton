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

package entitlement

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	uat "github.com/uber-go/atomic"
	"github.com/uber-go/tally"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pb_res "github.com/uber/peloton/.gen/peloton/api/v0/respool"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/util"
	res_common "github.com/uber/peloton/pkg/resmgr/common"
	"github.com/uber/peloton/pkg/resmgr/respool"
	"github.com/uber/peloton/pkg/resmgr/scalar"
)

// Calculator is responsible for calculating the entitlements for all the
// leaf resource pools based on the demand, free resources and share.
type Calculator struct {
	lock sync.Mutex

	// stores the current state of the calculation
	runningState int32
	// the hierarchy of resource pools
	resPoolTree respool.Tree
	// calculationPeriod defines how often to calculate the entitlements
	calculationPeriod time.Duration
	// chan to stop the calculation
	stopChan chan struct{}
	// client to get the cluster capacity
	hostMgrClient hostsvc.InternalHostServiceYARPCClient
	// map of cluster capacity keyed by the resource type
	clusterCapacity map[string]float64
	// map of cluster slack capacity keyed by the resource type
	clusterSlackCapacity map[string]float64
	// This atomic boolean helps to identify if previous run is
	// complete or still not done
	isRunning uat.Bool
	metrics   *metrics
}

// NewCalculator initializes the entitlement Calculator
func NewCalculator(
	calculationPeriod time.Duration,
	parent tally.Scope,
	hostMgrClient hostsvc.InternalHostServiceYARPCClient,
	tree respool.Tree) *Calculator {

	return &Calculator{
		resPoolTree:          tree,
		runningState:         res_common.RunningStateNotStarted,
		calculationPeriod:    calculationPeriod,
		stopChan:             make(chan struct{}, 1),
		hostMgrClient:        hostMgrClient,
		clusterCapacity:      make(map[string]float64),
		clusterSlackCapacity: make(map[string]float64),
		metrics:              newMetrics(parent.SubScope("Calculator")),
	}
}

// Start starts the entitlement calculation in a goroutine
func (c *Calculator) Start() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.runningState == res_common.RunningStateRunning {
		log.Warn("Entitlement Calculator is already running, " +
			"no action will be performed")
		c.metrics.calculationDuplicate.Inc(1)
		return nil
	}

	started := make(chan int, 1)
	go func() {
		defer atomic.StoreInt32(&c.runningState, res_common.RunningStateNotStarted)
		atomic.StoreInt32(&c.runningState, res_common.RunningStateRunning)

		log.Info("Starting Entitlement Calculation")
		close(started)

		ticker := time.NewTicker(c.calculationPeriod)
		defer ticker.Stop()
		for {
			if err := c.calculateEntitlement(context.Background()); err != nil {
				c.metrics.calculationFailed.Inc(1)
				log.WithError(err)
			}

			select {
			case <-c.stopChan:
				log.Info("Exiting Entitlement Calculator")
				return
			case <-ticker.C:
			case <-c.resPoolTree.UpdatedChannel():
			}
		}
	}()
	// Wait until go routine is started
	<-started
	return nil
}

// calculateEntitlement runs one entitlement calculation cycle.
func (c *Calculator) calculateEntitlement(ctx context.Context) error {
	log.Info("calculating entitlement")
	// Checking is previous transitions are complete
	isRunning := c.isRunning.Load()
	if isRunning {
		return errors.New("calculation already running")
	}

	defer c.metrics.calculationDuration.Start().Stop()

	// Changing value by that we block rest
	// of the runs
	c.isRunning.Swap(true)
	// Making Calculator done
	defer c.isRunning.Swap(false)

	rootResPool, err := c.resPoolTree.Get(&peloton.ResourcePoolID{
		Value: common.RootResPoolID,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to get root resource pool")
	}

	// Updating cluster capacity
	if err = c.updateClusterCapacity(ctx, rootResPool); err != nil {
		return errors.Wrapf(err, "failed to update cluster capacity")
	}
	// Invoking the demand calculation
	rootResPool.CalculateDemand()
	// Invoking the slack demand calculation
	rootResPool.CalculateSlackDemand()
	// Invoking the Allocation calculation
	rootResPool.CalculateTotalAllocatedResources()
	// Calculate Total Entitlement for non-revocable resources root respool's children
	c.setEntitlementForChildren(rootResPool)
	// Calculate entitlement for revocable resources and
	// set Slack and Non-Slack Entitlement for root respool's children
	// based on the previous entitlement calculation
	c.setSlackAndNonSlackEntitlementForChildren(rootResPool)

	return nil
}

// getChildShare returns the combined share of all the children of the provided
// resource pool.
func (c *Calculator) getChildShare(resp respool.ResPool, kind string) float64 {
	if resp == nil {
		return 0
	}

	children := resp.Children()

	totalShare := float64(0)
	for e := children.Front(); e != nil; e = e.Next() {
		n := e.Value.(respool.ResPool)
		totalShare += n.Resources()[kind].Share
	}
	return totalShare
}

// demandExist returns true if demand exists for any resource kind
func (c *Calculator) demandExist(
	demands map[string]*scalar.Resources,
	kind string) bool {
	for _, resource := range demands {
		if resource.Get(kind) > util.ResourceEpsilon {
			return true
		}
	}
	return false
}

func (c *Calculator) updateClusterCapacity(
	ctx context.Context,
	rootResPool respool.ResPool) error {
	// Calling the hostmgr for getting total capacity of the cluster
	totalResources, slackTotalResources, err := c.getTotalCapacity(ctx)
	if err != nil {
		return errors.Wrapf(err, "failed to get total cluster capacity")
	}

	rootResourcePoolConfig := rootResPool.ResourcePoolConfig()
	if rootResourcePoolConfig == nil {
		log.Error("root resource pool have invalid config")
		return errors.New("root resource pool have invalid config")
	}

	for _, res := range totalResources {
		// Setting the root resource information for
		// rootResourcePool
		c.clusterCapacity[res.Kind] = res.Capacity
	}

	for _, res := range slackTotalResources {
		// Setting the root resource information for
		// rootResourcePool
		c.clusterSlackCapacity[res.Kind] = res.Capacity
	}

	rootres := rootResourcePoolConfig.Resources
	if rootres == nil {
		log.
			WithField("root_resource_pool_config", rootResPool.ResourcePoolConfig().String()).
			Info("root resource pool resources is nil")
		rootres = []*pb_res.ResourceConfig{
			{
				Kind:        common.CPU,
				Reservation: c.clusterCapacity[common.CPU],
				Limit:       c.clusterCapacity[common.CPU],
			},
			{
				Kind:        common.GPU,
				Reservation: c.clusterCapacity[common.GPU],
				Limit:       c.clusterCapacity[common.GPU],
			},
			{
				Kind:        common.DISK,
				Reservation: c.clusterCapacity[common.DISK],
				Limit:       c.clusterCapacity[common.DISK],
			},
			{
				Kind:        common.MEMORY,
				Reservation: c.clusterCapacity[common.MEMORY],
				Limit:       c.clusterCapacity[common.MEMORY],
			},
		}
		rootResourcePoolConfig.Resources = rootres
	} else {
		// update the reservation and limit to the cluster capacity
		for _, resource := range rootres {
			resource.Reservation =
				c.clusterCapacity[resource.Kind]
			resource.Limit =
				c.clusterCapacity[resource.Kind]
		}
	}

	rootResPool.SetResourcePoolConfig(rootResourcePoolConfig)
	rootResPool.SetEntitlement(
		&scalar.Resources{
			CPU:    c.clusterCapacity[common.CPU],
			MEMORY: c.clusterCapacity[common.MEMORY],
			DISK:   c.clusterCapacity[common.DISK],
			GPU:    c.clusterCapacity[common.GPU],
		})
	rootResPool.SetSlackEntitlement(
		&scalar.Resources{
			CPU: c.clusterSlackCapacity[common.CPU],
		})
	log.WithField("root resource ", rootres).Info("Updating root resources")
	return nil
}

// getTotalCapacity returns the total capacity for physical and slack resources
// of the cluster
func (c *Calculator) getTotalCapacity(
	ctx context.Context) ([]*hostsvc.Resource, []*hostsvc.Resource, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	request := &hostsvc.ClusterCapacityRequest{}

	response, err := c.hostMgrClient.ClusterCapacity(ctx, request)
	if err != nil {
		log.
			WithField("error", err).
			Error("ClusterCapacity failed")
		return nil, nil, err
	}

	log.
		WithField("response", response).
		Debug("ClusterCapacity returned")

	if respErr := response.GetError(); respErr != nil {
		log.
			WithField("error", respErr).
			Error("ClusterCapacity error")
		return nil, nil, errors.New(respErr.String())
	}
	return response.PhysicalResources, response.PhysicalSlackResources, nil
}

// Stop stops Entitlement process
func (c *Calculator) Stop() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.runningState == res_common.RunningStateNotStarted {
		log.Warn("Entitlement Calculator is already stopped, no" +
			" action will be performed")
		return nil
	}

	log.Info("Stopping Entitlement Calculator")
	c.stopChan <- struct{}{}

	// Wait for entitlement Calculator to be stopped
	for {
		runningState := atomic.LoadInt32(&c.runningState)
		if runningState == res_common.RunningStateRunning {
			time.Sleep(10 * time.Millisecond)
		} else {
			break
		}
	}
	log.Info("Entitlement Calculator Stopped")
	return nil
}
