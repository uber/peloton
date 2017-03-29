package respool

import (
	"context"
	"sync/atomic"

	"code.uber.internal/infra/peloton/storage"

	log "github.com/Sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"

	"peloton/api/respool"
)

const (
	runningStateNotStarted = 0
	runningStateRunning    = 1
)

// ServiceHandler defines the interface of respool service handler to
// be called by leader election callbacks.
type ServiceHandler interface {
	Start() error
	Stop() error
}

// serviceHandler implements peloton.api.respool.ResourcePoolService
type serviceHandler struct {
	store        storage.ResourcePoolStore
	metrics      *Metrics
	dispatcher   yarpc.Dispatcher
	runningState int32
	resPoolTree  Tree
}

// Singleton service handler for ResourcePoolService
var handler *serviceHandler

// InitServiceHandler initializes the handler for ResourcePoolService
func InitServiceHandler(
	d yarpc.Dispatcher,
	parent tally.Scope,
	store storage.ResourcePoolStore) {

	if handler != nil {
		log.Warning("Resource pool service handler has already been initialized")
		return
	}

	scope := parent.SubScope("respool")
	metrics := NewMetrics(scope)

	// Initializing Resource Pool Tree
	InitTree(scope, store)

	handler = &serviceHandler{
		store:        store,
		metrics:      metrics,
		dispatcher:   d,
		runningState: runningStateNotStarted,
		resPoolTree:  GetTree(),
	}
	log.Info("ResourcePoolService handler created")
}

// GetServiceHandler returns the handler of ResourcePoolService. This
// function assumes the handler has been initialized as part of the
// InitEventHandler function.
func GetServiceHandler() ServiceHandler {
	if handler == nil {
		log.Fatal("ResourcePoolService handler is not initialized")
	}
	return handler
}

// CreateResourcePool will create resource pool
func (h *serviceHandler) CreateResourcePool(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *respool.CreateRequest) (*respool.CreateResponse, yarpc.ResMeta, error) {

	h.metrics.APICreateResourcePool.Inc(1)
	log.WithField("request", req).Info("CreateResourcePool called")

	resPoolID := req.Id
	resPoolConfig := req.Config
	err := h.store.CreateResourcePool(resPoolID, resPoolConfig, "peloton")

	// TODO  differentiate between n/w errors vs other data errors
	if err != nil {
		h.metrics.CreateResourcePoolFail.Inc(1)
		return &respool.CreateResponse{
			Error: &respool.CreateResponse_Error{
				AlreadyExists: &respool.ResourcePoolAlreadyExists{
					Id:      resPoolID,
					Message: err.Error(),
				},
			},
		}, nil, nil
	}

	// TODO: update in-memory respool tree structure

	h.metrics.CreateResourcePoolSuccess.Inc(1)
	return &respool.CreateResponse{
		Result: resPoolID,
	}, nil, nil
}

// GetResourcePool will get resource pool
func (h *serviceHandler) GetResourcePool(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *respool.GetRequest) (*respool.GetResponse, yarpc.ResMeta, error) {

	h.metrics.APIGetResourcePool.Inc(1)
	log.WithField("request", req).Info("GetResourcePool called")

	resPoolID := req.Id
	resPool, err := h.resPoolTree.Get(resPoolID)
	if err != nil {
		h.metrics.GetResourcePoolFail.Inc(1)
		return &respool.GetResponse{
			Error: &respool.GetResponse_Error{
				NotFound: &respool.ResourcePoolNotFound{
					Id:      resPoolID,
					Message: "resource pool not found",
				},
			},
		}, nil, nil
	}
	h.metrics.GetResourcePoolSuccess.Inc(1)
	return &respool.GetResponse{
		Poolinfo: resPool.toResourcePoolInfo(),
	}, nil, nil
}

// DeleteResourcePool will delete resource pool
func (h *serviceHandler) DeleteResourcePool(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *respool.DeleteRequest) (*respool.DeleteResponse, yarpc.ResMeta, error) {

	log.WithField("request", req).Info("DeleteResourcePool called")

	log.Fatal("Not implemented")
	return &respool.DeleteResponse{}, nil, nil
}

// UpdateResourcePool will update resource pool
func (h *serviceHandler) UpdateResourcePool(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *respool.UpdateRequest) (*respool.UpdateResponse, yarpc.ResMeta, error) {

	log.WithField("request", req).Info("UpdateResourcePool called")

	log.Fatal("Not implemented")
	return &respool.UpdateResponse{}, nil, nil
}

// Registerprocs will register all api's for end points
func (h *serviceHandler) registerProcs(d yarpc.Dispatcher) {
	d.Register(json.Procedure("ResourceManager.CreateResourcePool", h.CreateResourcePool))
	d.Register(json.Procedure("ResourceManager.GetResourcePool", h.GetResourcePool))
	d.Register(json.Procedure("ResourceManager.DeleteResourcePool", h.DeleteResourcePool))
	d.Register(json.Procedure("ResourceManager.UpdateResourcePool", h.UpdateResourcePool))
}

// Start will start resource manager
func (h *serviceHandler) Start() error {

	if h.runningState == runningStateRunning {
		log.Warn("Resource pool service is already running")
		return nil
	}

	atomic.StoreInt32(&h.runningState, runningStateRunning)

	log.Info("Registering the respool procedures")
	h.registerProcs(h.dispatcher)

	err := h.resPoolTree.Start()
	return err
}

// Stop will stop resource manager
func (h *serviceHandler) Stop() error {
	if h.runningState == runningStateNotStarted {
		log.Warn("Resource Manager is already stopped, no action will be performed")
		return nil
	}
	atomic.StoreInt32(&h.runningState, runningStateNotStarted)
	return nil
}
