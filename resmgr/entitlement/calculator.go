package entitlement

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	uat "github.com/uber-go/atomic"
	"github.com/uber-go/tally"

	"go.uber.org/yarpc"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pb_res "code.uber.internal/infra/peloton/.gen/peloton/api/v0/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"code.uber.internal/infra/peloton/common"
	res_common "code.uber.internal/infra/peloton/resmgr/common"
	"code.uber.internal/infra/peloton/resmgr/respool"
	"code.uber.internal/infra/peloton/resmgr/scalar"
	"code.uber.internal/infra/peloton/util"
)

// Calculator defines the interface of Entitlement calculator
type Calculator interface {
	// Start starts the entitlement calculator goroutines
	Start() error
	// Stop stops the entitlement calculator goroutines
	Stop() error
}

// calculator implements the Calculator interface
type calculator struct {
	sync.Mutex
	runningState      int32
	resPoolTree       respool.Tree
	calculationPeriod time.Duration
	stopChan          chan struct{}
	hostMgrClient     hostsvc.InternalHostServiceYARPCClient
	clusterCapacity   map[string]float64
	// This atomic boolean helps to identify if previous run is
	// complete or still not done
	isRunning uat.Bool
	metrics   *Metrics
}

// Singleton object for calculator
var calc *calculator

// InitCalculator initializes the entitlement calculator
func InitCalculator(
	d *yarpc.Dispatcher,
	calculationPeriod time.Duration,
	parent tally.Scope,
	hostMgrClient hostsvc.InternalHostServiceYARPCClient) {

	if calc != nil {
		log.Warning("entitlement calculator has already " +
			"been initialized")
		return
	}

	calc = &calculator{
		resPoolTree:       respool.GetTree(),
		runningState:      res_common.RunningStateNotStarted,
		calculationPeriod: calculationPeriod,
		stopChan:          make(chan struct{}, 1),
		hostMgrClient:     hostMgrClient,
		clusterCapacity:   make(map[string]float64),
		metrics:           NewMetrics(parent.SubScope("calculator")),
	}
	log.Info("entitlement calculator is initialized")
}

// GetCalculator returns the Calculator instance
func GetCalculator() Calculator {
	if calc == nil {
		log.Fatalf("Entitlement Calculator is not initialized")
	}
	return calc
}

// Start starts the entitlement calculation in a goroutine
func (c *calculator) Start() error {
	c.Lock()
	defer c.Unlock()

	if c.runningState == res_common.RunningStateRunning {
		log.Warn("Entitlement calculator is already running, " +
			"no action will be performed")
		c.metrics.EntitlementCalculationMissed.Inc(1)
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
				log.Error(err)
			}

			select {
			case <-c.stopChan:
				log.Info("Exiting Entitlement calculator")
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

// calculateEntitlement calculates the entitlement
func (c *calculator) calculateEntitlement(ctx context.Context) error {
	log.Info("calculating entitlement")
	// Checking is previous transitions are complete
	isRunning := c.isRunning.Load()
	if isRunning {
		log.Debug("previous instance of entitlement " +
			"calculator is running, skipping this run")
		return errors.New("previous instance of entitlement " +
			"calculator is running, skipping this run")
	}

	// Changing value by that we block rest
	// of the runs
	c.isRunning.Swap(true)
	// Making calculator done
	defer c.isRunning.Swap(false)

	rootResPool, err := c.resPoolTree.Get(&peloton.ResourcePoolID{
		Value: common.RootResPoolID,
	})
	if err != nil {
		log.WithError(err)
		log.Error("Root resource pool is not found")
		return err
	}

	// Updating cluster capacity
	err = c.updateClusterCapacity(ctx, rootResPool)
	if err != nil {
		return err
	}
	// Invoking the demand calculation
	rootResPool.CalculateDemand()
	// Invoking the Allocation calculation
	rootResPool.CalculateTotalAllocatedResources()
	// Setting Entitlement for root respool's children
	c.setEntitlementForChildren(rootResPool)

	return nil
}

func (c *calculator) setEntitlementForChildren(resp respool.ResPool) {
	if resp == nil {
		return
	}

	childs := resp.Children()

	entitlement := resp.GetEntitlement().Clone()
	assignments := make(map[string]*scalar.Resources)
	totalShare := make(map[string]float64)
	demands := make(map[string]*scalar.Resources)
	log.WithField("respool", resp.Name()).
		Debug("Starting Entitlement cycle for respool")

	// Setting entitlement is a 3 phase process
	// 1. Calculate assignments based on reservation
	// 2. Distribute rest of the free resources based on share and demand
	// 3. Once the demand is zero , distribute remaining based on share

	// This is the first phase for assignment
	c.calculateAssignmentsFromReservation(resp, demands, entitlement, assignments, totalShare)

	// This is second phase for distributing remaining resources
	c.distributeRemainingResources(resp, demands, entitlement, assignments, totalShare)

	// This is the third phase for the entitlement cycle. here after all the
	// assigmenets based on demand, rest of the resources are being
	// distributed in all the resource pools.
	c.distributeUnclaimedResources(resp, entitlement, assignments)

	// Now setting entitlement for all the children and call
	// for their children recursively
	for e := childs.Front(); e != nil; e = e.Next() {
		n := e.Value.(respool.ResPool)

		n.SetEntitlementResources(assignments[n.ID()])
		log.WithFields(log.Fields{
			"respool_ID":   n.ID(),
			"respool_name": n.Name(),
			"entitlement":  assignments[n.ID()],
		}).Info("Setting the entitlement for ResPool")

		// Calling the function recursively
		// for all the children for the respool passed
		// to this function
		c.setEntitlementForChildren(n)
	}
}

// calculateAssignmentsFromReservation calculates assigments
// based on demand and reservation
// as well as return the remaining entitlement for redistribution
func (c *calculator) calculateAssignmentsFromReservation(resp respool.ResPool,
	demands map[string]*scalar.Resources,
	entitlement *scalar.Resources,
	assignments map[string]*scalar.Resources,
	totalShare map[string]float64) {
	// First Pass: In the first pass of children we get the demand recursively
	// calculated And then compare with respool reservation and
	// choose the min of these two
	// assignment := min(demand,reservation) if the resource type is ELASTIC.
	// If the resourceType is Static then we need to make assignment to
	// equal to reservation.
	// We also measure the free entitlement by that we can distribute
	// it with fair share. We also need to keep track of the total share
	// of the kind of resources which demand is more then the resrevation
	// As we can ignore the other whose demands are reached as they dont
	// need to get the fare share
	childs := resp.Children()
	cloneEntitlement := entitlement.Clone()
	for e := childs.Front(); e != nil; e = e.Next() {
		assignment := new(scalar.Resources)
		n := e.Value.(respool.ResPool)

		resConfigMap := n.Resources()

		c.calculateDemandForRespool(n, demands)

		demand := demands[n.ID()]
		// Now checking if demand is less then reservation or not
		// Taking the assignement to the min(demand,reservation)
		for kind, cfg := range resConfigMap {
			// Checking the reservation type of the resource pool
			// If resource type is reservation type static then
			// entitlement will always be greater than equal to
			// reservation irrespective of the demand. Otherwise
			// Based on the demand assignment := min(demand,reservation)
			if cfg.Type == pb_res.ReservationType_STATIC {
				assignment.Set(kind, cfg.Reservation)
			} else {
				assignment.Set(kind, math.Min(demand.Get(kind), cfg.Reservation))
			}
			if demand.Get(kind) > cfg.Reservation {
				totalShare[kind] += cfg.Share
				demand.Set(kind, demand.Get(kind)-cfg.Reservation)
			} else {
				demand.Set(kind, 0)
			}
		}

		cloneEntitlement = cloneEntitlement.Subtract(assignment)
		assignments[n.ID()] = assignment
		demands[n.ID()] = demand
		log.WithFields(log.Fields{
			"respool":        n.Name(),
			"limited_demand": demand,
			"assignment":     assignment,
			"entitlement":    cloneEntitlement,
		}).Info("First pass completed for respool")
	}
	entitlement.Copy(cloneEntitlement)
}

// calculateDemandForRespool calculates the demand based on number of
// tasks waiting in the queue as well as current allocation.
// This also caps the demand based on the limit of the resource pool
// for that type of resource
func (c *calculator) calculateDemandForRespool(n respool.ResPool,
	demands map[string]*scalar.Resources,
) {
	// Demand is pending tasks + already allocated
	demand := n.GetDemand()
	allocation := n.GetTotalAllocatedResources()
	demand = demand.Add(allocation)
	demands[n.ID()] = demand
	log.WithFields(log.Fields{
		"respool_ID":   n.ID(),
		"respool_name": n.Name(),
		"demand":       demand,
	}).Info("Demand for resource pool")

	resConfig := n.Resources()
	// Caping the demand with Limit for resource pool
	// If demand is less then limit then we use demand for
	// Entitlement calculation otherwise we use limit as demand
	// to cap the allocation till limit.
	limitedDemand := demand
	for kind, res := range resConfig {
		limitedDemand.Set(kind, math.Min(demand.Get(kind), res.GetLimit()))
		log.WithFields(log.Fields{
			"respool_ID":     n.ID(),
			"respool_name":   n.Name(),
			"kind":           kind,
			"actual_demand":  demand.Get(kind),
			"limit":          res.Limit,
			"limited_demand": limitedDemand.Get(kind),
		}).Info("Limited Demand for resource pool")
	}

	// Setting the demand to cap at limit for entitlement calculation
	demands[n.ID()] = limitedDemand
}

// distributeRemainingResources distributes the remianing entitlement based
// on demand and share of the resourcepool.
func (c *calculator) distributeRemainingResources(resp respool.ResPool,
	demands map[string]*scalar.Resources,
	entitlement *scalar.Resources,
	assignments map[string]*scalar.Resources,
	totalShare map[string]float64,
) {
	childs := resp.Children()
	for _, kind := range []string{common.CPU, common.GPU,
		common.MEMORY, common.DISK} {
		remaining := *entitlement
		log.WithFields(log.Fields{
			"kind":       kind,
			"remianing ": remaining.Get(kind),
		}).Debug("Remaining resources before second pass")
		// Second Pass: In the second pass we will distribute the
		// rest of the resources to the resource pools which have
		// higher demand than the reservation.
		// It will also cap the fair share to demand and redistribute the
		// rest of the entitlement to others.
		for remaining.Get(kind) > util.ResourceEpsilon && c.demandExist(demands, kind) {
			log.WithField("remaining", remaining.Get(kind)).Debug("Remaining resources")
			remainingShare := totalShare[kind]
			for e := childs.Front(); e != nil; e = e.Next() {
				n := e.Value.(respool.ResPool)
				log.WithFields(log.Fields{
					"respool":   n.Name(),
					"remaining": remaining.Get(kind),
					"kind":      kind,
					"demand":    demands[n.ID()].Get(kind),
				}).Debug("Evaluating respool")
				if remaining.Get(kind) < util.ResourceEpsilon {
					break
				}

				if demands[n.ID()].Get(kind) < util.ResourceEpsilon {
					continue
				}

				value := float64(n.Resources()[kind].Share * entitlement.Get(kind))
				value = float64(value / totalShare[kind])
				log.WithField("value", value).Debug(" value to evaluate ")

				// Checking if demand is less then the current share
				// if yes then cap it to demand and distribute rest
				// to others
				if value > demands[n.ID()].Get(kind) {
					value = demands[n.ID()].Get(kind)
					remainingShare = remainingShare - n.Resources()[kind].Share
					demands[n.ID()].Set(kind, 0)
				} else {
					demands[n.ID()].Set(
						kind, float64(demands[n.ID()].Get(kind)-value))
				}
				if remaining.Get(kind) > value {
					remaining.Set(kind, float64(remaining.Get(kind)-value))
				} else {
					// Caping the fare share with the remaining
					// resources for resource kind
					value = remaining.Get(kind)
					remaining.Set(kind, float64(0))
				}
				log.WithField("value", value).Debug(" value after evaluation ")
				value += assignments[n.ID()].Get(kind)
				assignments[n.ID()].Set(kind, value)
				log.WithFields(log.Fields{
					"respool":    n.Name(),
					"assignment": value,
				}).Debug("Setting assignment for this respool")
			}
			*entitlement = remaining
			totalShare[kind] = remainingShare
		}
	}
}

// distributeUnclaimedResources is the THIRD PHASE of the
// entitlement calculation. Third phase is to distribute the
// free resources in the cluster which is remaining after
// distribution to all the resource pools which had demand.
// This is in anticipation of respools can get some more workloads
// in between two entitlement cycles
func (c *calculator) distributeUnclaimedResources(
	resp respool.ResPool,
	entitlement *scalar.Resources,
	assignments map[string]*scalar.Resources,
) {
	childs := resp.Children()
	for _, kind := range []string{common.CPU, common.GPU,
		common.MEMORY, common.DISK} {
		// Third pass : Now all the demand is been satisfied
		// we need to distribute the rest of the entitlement
		// to all the nodes for the anticipation of some work
		// load and not starve everybody till next cycle
		if entitlement.Get(kind) > util.ResourceEpsilon {
			totalChildShare := c.getChildShare(resp, kind)
			for e := childs.Front(); e != nil; e = e.Next() {
				n := e.Value.(respool.ResPool)
				value := float64(n.Resources()[kind].Share *
					entitlement.Get(kind))
				value = float64(value / totalChildShare)
				value += assignments[n.ID()].Get(kind)

				// We need to cap the limit here for free resources
				// as we can not give more then limit to resource pool
				if value > n.Resources()[kind].GetLimit() {
					assignments[n.ID()].Set(kind, n.Resources()[kind].GetLimit())
				} else {
					assignments[n.ID()].Set(kind, value)
				}
			}
		}
	}
}

// getChildShare returns the combined share of the childrens
func (c *calculator) getChildShare(resp respool.ResPool, kind string) float64 {
	if resp == nil {
		return 0
	}

	childs := resp.Children()

	totalshare := float64(0)
	for e := childs.Front(); e != nil; e = e.Next() {
		n := e.Value.(respool.ResPool)
		totalshare += n.Resources()[kind].Share
	}
	return totalshare
}

// demandExist returns true if demand exists for any resource kind
func (c *calculator) demandExist(
	demands map[string]*scalar.Resources,
	kind string) bool {
	for _, resource := range demands {
		if resource.Get(kind) > util.ResourceEpsilon {
			return true
		}
	}
	return false
}

func (c *calculator) updateClusterCapacity(ctx context.Context, rootResPool respool.ResPool) error {
	// Calling the hostmgr for getting total capacity of the cluster
	totalResources, err := c.getTotalCapacity(ctx)
	if err != nil {
		return err
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

	rootres := rootResourcePoolConfig.Resources
	if rootres == nil {
		log.WithField("root", rootResPool).Info("res pool have nil resource config")
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
	rootResPool.SetEntitlement(c.clusterCapacity)
	log.WithField(" root resource ", rootres).Info("Updating root resources")
	return nil
}

// getTotalCapacity returns the total capacity if the cluster
func (c *calculator) getTotalCapacity(ctx context.Context) ([]*hostsvc.Resource, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	request := &hostsvc.ClusterCapacityRequest{}

	response, err := c.hostMgrClient.ClusterCapacity(ctx, request)
	if err != nil {
		log.WithField("error", err).Error("ClusterCapacity failed")
		return nil, err
	}

	log.WithField("response", response).Debug("ClusterCapacity returned")

	if respErr := response.GetError(); respErr != nil {
		log.WithField("error", respErr).Error("ClusterCapacity error")
		return nil, errors.New(respErr.String())
	}
	return response.PhysicalResources, nil
}

// Stop stops Entitlement process
func (c *calculator) Stop() error {
	c.Lock()
	defer c.Unlock()

	if c.runningState == res_common.RunningStateNotStarted {
		log.Warn("Entitlement calculator is already stopped, no" +
			" action will be performed")
		return nil
	}

	log.Info("Stopping Entitlement Calculator")
	c.stopChan <- struct{}{}

	// Wait for entitlement calculator to be stopped
	for {
		runningState := atomic.LoadInt32(&c.runningState)
		if runningState == res_common.RunningStateRunning {
			time.Sleep(10 * time.Millisecond)
		} else {
			break
		}
	}
	log.Info("Entitlement calculator Stopped")
	return nil
}
