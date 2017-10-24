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

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	res "code.uber.internal/infra/peloton/.gen/peloton/api/respool"
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
	parent tally.Scope) {

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
		hostMgrClient: hostsvc.NewInternalHostServiceYARPCClient(
			d.ClientConfig(
				common.PelotonHostManager)),
		clusterCapacity: make(map[string]float64),
		metrics:         NewMetrics(parent.SubScope("calculator")),
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
		started <- 0

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
		Value: respool.RootResPoolID,
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
	rootResPool.CalculateAllocation()
	// Setting Entitlement for root respool's children
	c.setEntitlementForChildren(rootResPool)

	return nil
}

func (c *calculator) setEntitlementForChildren(resp respool.ResPool) {
	if resp == nil {
		return
	}

	childs := resp.Children()

	entitlement := resp.GetEntitlement()
	assignments := make(map[string]*scalar.Resources)
	totalShare := make(map[string]float64)
	demands := make(map[string]*scalar.Resources)

	// First Pass: In the first pass of children we get the demand recursively
	// calculated And then compare with respool reservation and
	// choose the min of these two
	// assignment := min(demand,reservation)
	// We also measure the free entitlement by that we can distribute
	// it with fair share. We also need to keep track of the total share
	// of the kind of resources which demand is more then the resrevation
	// As we can ignore the other whose demands are reached as they dont
	// need to get the fare share
	for e := childs.Front(); e != nil; e = e.Next() {
		assignment := new(scalar.Resources)
		n := e.Value.(respool.ResPool)

		// Demand is pending tasks + already allocated
		demand := n.GetDemand()
		allocation := n.GetAllocation()
		demand = demand.Add(allocation)
		demands[n.ID()] = demand
		log.WithFields(log.Fields{
			"respool_ID":   n.ID(),
			"respool_name": n.Name(),
			"demand":       demand,
		}).Info("Demand for resource pool")

		resConfig := n.Resources()
		for kind, res := range resConfig {
			assignment.Set(kind, math.Min(demand.Get(kind), res.Reservation))
			if demand.Get(kind) > res.Reservation {
				totalShare[kind] += res.Share
				demand.Set(kind, demand.Get(kind)-res.Reservation)
			} else {
				demand.Set(kind, 0)
			}
		}

		entitlement = entitlement.Subtract(assignment)
		assignments[n.ID()] = assignment
		demands[n.ID()] = demand
	}

	for _, kind := range []string{common.CPU, common.GPU,
		common.MEMORY, common.DISK} {
		remaining := *entitlement

		// Second Pass: In the second pass we will distribute the
		// rest of the resources to the resource pools which have
		// higher demand then reservation
		// It will also cap the fair share to demand and redistribute the
		// rest of the entitlement to others.
		for remaining.Get(kind) > util.ResourceEpsilon && c.isDemandExist(demands, kind) {
			remainingShare := totalShare[kind]
			for e := childs.Front(); e != nil; e = e.Next() {
				n := e.Value.(respool.ResPool)

				if remaining.Get(kind) < util.ResourceEpsilon {
					break
				}

				if demands[n.ID()].Get(kind) < util.ResourceEpsilon {
					continue
				}

				value := float64(n.Resources()[kind].Share * entitlement.Get(kind))
				value = float64(value / totalShare[kind])

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
				value += assignments[n.ID()].Get(kind)
				assignments[n.ID()].Set(kind, value)
			}
			*entitlement = remaining
			totalShare[kind] = remainingShare
		}

		// Third pass : Now all the demand is been satisfied
		// we need to distribute the rest of the entitlement
		// to all the nodes for the anticipation of some work
		// load and not starve everybody till next cycle
		if entitlement.Get(kind) > 0 {
			totalChildShare := c.getChildShare(resp, kind)
			for e := childs.Front(); e != nil; e = e.Next() {
				n := e.Value.(respool.ResPool)
				value := float64(n.Resources()[kind].Share *
					entitlement.Get(kind))
				value = float64(value / totalChildShare)
				value += assignments[n.ID()].Get(kind)
				assignments[n.ID()].Set(kind, value)
			}
		}
	}

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

// isDemandExist returns true if demand exists for particular kind
func (c *calculator) isDemandExist(
	demands map[string]*scalar.Resources,
	kind string) bool {
	for _, res := range demands {
		if res.Get(kind) > 0 {
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
		rootres = []*res.ResourceConfig{
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
		for _, resource := range rootres {
			resource.Reservation =
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
