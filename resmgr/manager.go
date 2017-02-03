package resmgr

import (
	"context"
	"sync/atomic"

	"code.uber.internal/infra/peloton/master/metrics"
	rmconfig "code.uber.internal/infra/peloton/resmgr/config"
	"code.uber.internal/infra/peloton/storage"
	log "github.com/Sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"

	"peloton/resmgr"
)

const (
	runningStateNotStarted = 0
	runningStateRunning    = 1
)

// InitManager initializes the resource pool manager
func InitManager(
	d yarpc.Dispatcher,
	config *rmconfig.Config,
	store storage.ResourcePoolStore,
	metrics *metrics.Metrics) *ResourceManager {
	handler := ResourceManager{
		store:        store,
		metrics:      metrics,
		config:       config,
		dispatcher:   d,
		runningState: runningStateNotStarted,
	}
	log.Info("Resource Manager created")
	return &handler
}

// ResourceManager is Resource Manager
type ResourceManager struct {
	store        storage.ResourcePoolStore
	metrics      *metrics.Metrics
	config       *rmconfig.Config
	dispatcher   yarpc.Dispatcher
	runningState int32
}

// CreateResourcePool will create resource pool
func (m *ResourceManager) CreateResourcePool(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *resmgr.CreateRequest) (*resmgr.CreateResponse, yarpc.ResMeta, error) {

	log.Info("CreateResourcePool Called")

	resPoolID := req.Id
	resPoolConfig := req.Config

	log.WithField("config", resPoolConfig).Infof("resmgr.CreateResourcePool called: %v", req)
	// Add metrics

	err := m.store.CreateResourcePool(resPoolID, resPoolConfig, "peloton")
	if err != nil {
		// Add failure metrics
		return &resmgr.CreateResponse{
			AlreadyExists: &resmgr.ResourcePoolAlreadyExists{
				Id:      resPoolID,
				Message: err.Error(),
			},
		}, nil, nil
	}
	return &resmgr.CreateResponse{
		Result: resPoolID,
	}, nil, nil
}

// GetResourcePool will get resource pool
func (m *ResourceManager) GetResourcePool() {
	// TODO
}

// DeleteResourcePool will delete resource pool
func (m *ResourceManager) DeleteResourcePool() {
	// TODO
}

// UpdateResourcePool will update resource pool
func (m *ResourceManager) UpdateResourcePool() {
	// TODO
}

// Registerprocs will register all api's for end points
func (m *ResourceManager) registerProcs(d yarpc.Dispatcher) {
	json.Register(d, json.Procedure("ResourceManager.CreateResourcePool", m.CreateResourcePool))
	log.Info("CreateResourcePool Registered ")
	/* TODO: Will have to implement these api's
	json.Register(d, json.Procedure("ResourceManager.GetResourcePool", m.GetResourcePool))
	json.Register(d, json.Procedure("ResourceManager.DeleteResourcePool", m.DeleteResourcePool))
	json.Register(d, json.Procedure("ResourceManager.UpdateResourcePool", m.UpdateResourcePool))
	*/

}

// Start will start resource manager
func (m *ResourceManager) Start() {

	if m.runningState == runningStateRunning {
		log.Warn("Resource Manager is already running, no action will be performed")
		return
	}

	atomic.StoreInt32(&m.runningState, runningStateRunning)

	log.Info("Registering the procedures")
	m.registerProcs(m.dispatcher)
}

// Stop will stop resource manager
func (m *ResourceManager) Stop() {
	if m.runningState == runningStateNotStarted {
		log.Warn("Resource Manager is already stopped, no action will be performed")
		return
	}
	atomic.StoreInt32(&m.runningState, runningStateNotStarted)
}
