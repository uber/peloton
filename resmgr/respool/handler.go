package respool

import (
	"context"
	"sync/atomic"

	"code.uber.internal/infra/peloton/master/metrics"
	rmconfig "code.uber.internal/infra/peloton/resmgr/config"
	"code.uber.internal/infra/peloton/storage"
	log "github.com/Sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"

	"peloton/api/respool"
)

const (
	runningStateNotStarted = 0
	runningStateRunning    = 1
)

// InitServiceHandler initializes the resource pool manager
func InitServiceHandler(
	d yarpc.Dispatcher,
	config *rmconfig.ResMgrConfig,
	store storage.ResourcePoolStore,
	metrics *metrics.Metrics) *ServiceHandler {

	// Initializing Resource Pool Tree
	resPoolTree := InitTree(config, store, metrics)

	handler := ServiceHandler{
		store:        store,
		metrics:      metrics,
		config:       config,
		dispatcher:   d,
		runningState: runningStateNotStarted,
		resPoolTree:  resPoolTree,
	}
	log.Info("Resource Manager created")
	return &handler
}

// ServiceHandler implements peloton.api.respool.ResourceManager
type ServiceHandler struct {
	store        storage.ResourcePoolStore
	metrics      *metrics.Metrics
	config       *rmconfig.ResMgrConfig
	dispatcher   yarpc.Dispatcher
	runningState int32
	resPoolTree  *Tree
}

// CreateResourcePool will create resource pool
func (m *ServiceHandler) CreateResourcePool(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *respool.CreateRequest) (*respool.CreateResponse, yarpc.ResMeta, error) {

	log.Info("CreateResourcePool Called")

	resPoolID := req.Id
	resPoolConfig := req.Config

	log.WithField("config", resPoolConfig).Infof("respool.CreateResourcePool called: %v", req)
	// Add metrics

	err := m.store.CreateResourcePool(resPoolID, resPoolConfig, "peloton")
	if err != nil {
		// Add failure metrics
		return &respool.CreateResponse{
			AlreadyExists: &respool.ResourcePoolAlreadyExists{
				Id:      resPoolID,
				Message: err.Error(),
			},
		}, nil, nil
	}
	return &respool.CreateResponse{
		Result: resPoolID,
	}, nil, nil
}

// GetResourcePool will get resource pool
func (m *ServiceHandler) GetResourcePool() {
	// TODO
}

// DeleteResourcePool will delete resource pool
func (m *ServiceHandler) DeleteResourcePool() {
	// TODO
}

// UpdateResourcePool will update resource pool
func (m *ServiceHandler) UpdateResourcePool() {
	// TODO
}

// Registerprocs will register all api's for end points
func (m *ServiceHandler) registerProcs(d yarpc.Dispatcher) {
	json.Register(d, json.Procedure("ResourceManager.CreateResourcePool", m.CreateResourcePool))
	log.Info("CreateResourcePool Registered ")
	/* TODO: Will have to implement these api's
	json.Register(d, json.Procedure("ResourceManager.GetResourcePool", m.GetResourcePool))
	json.Register(d, json.Procedure("ResourceManager.DeleteResourcePool", m.DeleteResourcePool))
	json.Register(d, json.Procedure("ResourceManager.UpdateResourcePool", m.UpdateResourcePool))
	*/

}

// Start will start resource manager
func (m *ServiceHandler) Start() {

	if m.runningState == runningStateRunning {
		log.Warn("Resource Manager is already running, no action will be performed")
		return
	}

	atomic.StoreInt32(&m.runningState, runningStateRunning)

	log.Info("Registering the respool procedures")
	m.registerProcs(m.dispatcher)
	m.resPoolTree.StartResPool()
}

// Stop will stop resource manager
func (m *ServiceHandler) Stop() {
	if m.runningState == runningStateNotStarted {
		log.Warn("Resource Manager is already stopped, no action will be performed")
		return
	}
	atomic.StoreInt32(&m.runningState, runningStateNotStarted)
}
