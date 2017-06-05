package entitlement

import (
	"container/list"
	"context"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	uat "github.com/uber-go/atomic"

	"go.uber.org/yarpc"

	res "code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/resmgr/respool"
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
	hostMgrClient     hostsvc.InternalHostServiceYarpcClient
	clusterCapacity   map[string]float64
	// This atomic boolean helps to identify if previous run is
	// complete or still not done
	isRunning uat.Bool
}

// Singleton object for calculator
var calc *calculator

// InitCalculator initializes the entitlement calculator
func InitCalculator(
	d *yarpc.Dispatcher,
	calculationPeriod time.Duration) {

	if calc != nil {
		log.Warning("entitlement calculator has already " +
			"been initialized")
		return
	}

	calc = &calculator{
		resPoolTree:       respool.GetTree(),
		runningState:      runningStateNotStarted,
		calculationPeriod: calculationPeriod,
		stopChan:          make(chan struct{}, 1),
		hostMgrClient: hostsvc.NewInternalHostServiceYarpcClient(
			d.ClientConfig(
				common.PelotonHostManager)),
		clusterCapacity: make(map[string]float64),
	}
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

	if c.runningState == runningStateRunning {
		log.Warn("Entitlement calculator is already running, " +
			"no action will be performed")
		return nil
	}

	started := make(chan int, 1)
	go func() {
		defer atomic.StoreInt32(&c.runningState, runningStateNotStarted)
		atomic.StoreInt32(&c.runningState, runningStateRunning)

		log.Info("Starting Entitlement Calculation")
		started <- 0

		for {
			timer := time.NewTimer(c.calculationPeriod)
			select {
			case <-c.stopChan:
				log.Info("Exiting Task Scheduler")
				return
			case <-timer.C:
				if err := c.calculateEntitlement(context.Background()); err != nil {
					log.Error(err)
				}
			}
			timer.Stop()
		}
	}()
	// Wait until go routine is started
	<-started
	return nil
}

// calculateEntitlement calculates the entitlement
func (c *calculator) calculateEntitlement(ctx context.Context) error {
	// Checking is previous transitions are complete
	inRunning := c.isRunning.Load()
	if inRunning {
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

	rootResPool, err := c.resPoolTree.Get(&res.ResourcePoolID{
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

	// We need to find out the reservation for the children for root
	// That will be equal to cluster's reservation in total
	totalReservation, err := rootResPool.GetChildReservation()
	if err != nil {
		log.Error(err)
		return err
	}

	log.WithField("total_reservation", totalReservation).Debug("Total reservation")

	totalFreeResources := c.getTotalFreeResources(totalReservation)

	log.WithField("free_resources", totalFreeResources).Debug("Free Resources")

	//TODO: getting only child nodes , need to travel to hierarichy
	childNodes := c.resPoolTree.GetAllNodes(true)
	// Calculating the entitlement and setting to each resource pool
	err = c.setEntitlementforNodes(childNodes, totalFreeResources)
	if err != nil {
		return err
	}

	return nil
}

// setEntitlementforNodes calculates and set the entitlement for nodes
func (c *calculator) setEntitlementforNodes(
	nodes *list.List,
	freeResources map[string]float64) error {
	if nodes == nil {
		return errors.New("nodes list is nil, nothing to set")
	}

	// Traversing for getting the total share for each resource type
	for _, kind := range []string{common.CPU, common.GPU,
		common.MEMORY, common.DISK} {
		c.setEntitlementforType(nodes, freeResources[kind], kind)
	}
	return nil
}

func (c *calculator) setEntitlementforType(
	nodes *list.List,
	freeResources float64,
	resourceKind string) error {
	var totalShare float64
	for e := nodes.Front(); e != nil; e = e.Next() {
		n := e.Value.(respool.ResPool)
		for kind, res := range n.Resources() {
			if kind == resourceKind {
				totalShare += res.Share
			}
		}
	}

	// calculating pernode entitlement and setting it
	for e := nodes.Front(); e != nil; e = e.Next() {
		n := e.Value.(respool.ResPool)
		var entitlement float64
		for kind, res := range n.Resources() {
			if kind != resourceKind {
				continue
			}
			fareShare := freeResources * res.Share
			fareShare = fareShare / totalShare
			entitlement = res.Reservation + fareShare
			log.WithFields(log.Fields{
				"Node":        n.ID(),
				"reservation": res.Reservation,
				"fareshare":   fareShare,
				"Entitlement": entitlement,
				"Kind":        resourceKind,
			}).Debug("Reservation and fare share for node")
			n.SetEntitlementByKind(resourceKind, entitlement)
		}
	}
	return nil
}

// getTotalFreeResources gets the free resources in cluster
func (c *calculator) getTotalFreeResources(
	reservation map[string]float64,
) map[string]float64 {
	freeResources := make(map[string]float64)
	for kind, val := range reservation {
		free := c.clusterCapacity[kind] - val
		if free < 0 {
			free = 0
		}
		freeResources[kind] = free
	}
	return freeResources
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
			},
			{
				Kind:        common.GPU,
				Reservation: c.clusterCapacity[common.GPU],
			},
			{
				Kind:        common.DISK,
				Reservation: c.clusterCapacity[common.DISK],
			},
			{
				Kind:        common.MEMORY,
				Reservation: c.clusterCapacity[common.MEMORY],
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
	return response.Resources, nil
}

// Stop stops Entitlement process
func (c *calculator) Stop() error {
	c.Lock()
	defer c.Unlock()

	if c.runningState == runningStateNotStarted {
		log.Warn("Entitlement calculator is already stopped, no" +
			" action will be performed")
		return nil
	}

	log.Info("Stopping Entitlement Calculator")
	c.stopChan <- struct{}{}

	// Wait for entitlement calculator to be stopped
	for {
		runningState := atomic.LoadInt32(&c.runningState)
		if runningState == runningStateRunning {
			time.Sleep(10 * time.Millisecond)
		} else {
			break
		}
	}
	log.Info("Entitlement calculator Stopped")
	return nil
}
