package respool

import (
	"context"
	"sync"
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
	sync.Mutex
	store                  storage.ResourcePoolStore
	metrics                *Metrics
	dispatcher             yarpc.Dispatcher
	runningState           int32
	resPoolTree            Tree
	resPoolConfigValidator Validator
}

// Singleton service handler for ResourcePoolService
var handler *serviceHandler

// InitServiceHandler initializes the handler for ResourcePoolService
func InitServiceHandler(
	d yarpc.Dispatcher,
	parent tally.Scope,
	store storage.ResourcePoolStore) {

	if handler != nil {
		log.Warning(
			`Resource pool service handler has already
			been initialized`,
		)
		return
	}

	scope := parent.SubScope("respool")
	metrics := NewMetrics(scope)

	// Initializing Resource Pool Tree
	InitTree(scope, store)

	// Initialize Resource Pool Config Validator
	resPoolConfigValidator, err := NewResourcePoolConfigValidator(GetTree())

	if err != nil {
		log.Fatalf(
			`Error initializing resource pool
			config validator: %v`,
			err,
		)
	}

	handler = &serviceHandler{
		store:                  store,
		metrics:                metrics,
		dispatcher:             d,
		runningState:           runningStateNotStarted,
		resPoolTree:            GetTree(),
		resPoolConfigValidator: resPoolConfigValidator,
	}
	log.Info("ResourcePoolService handler created")
}

// GetServiceHandler returns the handler of ResourcePoolService. This
// function assumes the handler has been initialized as part of the
// InitEventHandler function.
func GetServiceHandler() ServiceHandler {
	if handler == nil {
		log.Fatal(
			"ResourcePoolService handler is not initialized",
		)
	}
	return handler
}

// CreateResourcePool will create resource pool
func (h *serviceHandler) CreateResourcePool(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *respool.CreateRequest) (
	*respool.CreateResponse,
	yarpc.ResMeta,
	error) {

	h.Lock()
	defer h.Unlock()

	h.metrics.APICreateResourcePool.Inc(1)
	log.WithField(
		"request",
		req,
	).Info("CreateResourcePool called")

	resPoolID := req.GetId()
	resPoolConfig := req.GetConfig()

	resourcePoolConfigData := ResourcePoolConfigData{
		ID:                                resPoolID,
		ResourcePoolConfig:                resPoolConfig,
		SkipRootChildResourceConfigChecks: true,
	}

	// perform validation on resource pool resPoolConfig
	if err := h.resPoolConfigValidator.Validate(
		resourcePoolConfigData,
	); err != nil {
		h.metrics.CreateResourcePoolFail.Inc(1)
		log.WithError(
			err,
		).Infof(
			"Error validating respoolID: %s in store",
			resPoolID.Value,
		)
		return &respool.CreateResponse{
			Error: &respool.CreateResponse_Error{
				InvalidResourcePoolConfig: &respool.InvalidResourcePoolConfig{
					Id:      resPoolID,
					Message: err.Error(),
				},
			},
		}, nil, nil
	}

	// TODO T808419 - handle parent of the new_resource_pool_config
	// already has tasks added running, drain, distinguish?

	// insert persistent store
	if err := h.store.CreateResourcePool(
		resPoolID,
		resPoolConfig,
		"peloton",
	); err != nil {
		h.metrics.CreateResourcePoolFail.Inc(1)
		log.WithError(err).Infof(
			"Error creating respoolID: %s in store",
			resPoolID.Value,
		)
		return &respool.CreateResponse{
			Error: &respool.CreateResponse_Error{
				// TODO  differentiate between n/w errors vs other data errors
				AlreadyExists: &respool.ResourcePoolAlreadyExists{
					Id:      resPoolID,
					Message: err.Error(),
				},
			},
		}, nil, nil
	}

	// update the in-memory data structure
	if err := h.resPoolTree.Upsert(resPoolID, resPoolConfig); err != nil {
		// rollback i.e. delete the current version if any errors
		h.metrics.CreateResourcePoolFail.Inc(1)
		log.WithError(
			err,
		).Infof(
			"Error creating respoolID: %s in memory tree",
			resPoolID.Value,
		)

		if err := h.store.DeleteResourcePool(resPoolID); err != nil {
			log.WithError(
				err,
			).Infof(
				`Error rolling back respoolID:
				%s in store`,
				resPoolID.Value,
			)
			h.metrics.CreateResourcePoolRollbackFail.Inc(1)
			return &respool.CreateResponse{}, nil, err
		}
	}

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

	resPoolID := req.GetId()

	if resPoolID == nil {
		//TODO temporary solution to unblock,
		// fix with new naming convention
		resPoolID = &respool.ResourcePoolID{
			Value: RootResPoolID,
		}
	}

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
		Poolinfo: resPool.ToResourcePoolInfo(),
	}, nil, nil
}

// DeleteResourcePool will delete resource pool
func (h *serviceHandler) DeleteResourcePool(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *respool.DeleteRequest) (
	*respool.DeleteResponse,
	yarpc.ResMeta,
	error) {

	h.Lock()
	defer h.Unlock()

	log.WithField(
		"request",
		req,
	).Info("DeleteResourcePool called")

	log.Fatal("Not implemented")
	return &respool.DeleteResponse{}, nil, nil
}

// UpdateResourcePool will update resource pool
func (h *serviceHandler) UpdateResourcePool(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *respool.UpdateRequest) (
	*respool.UpdateResponse,
	yarpc.ResMeta,
	error) {

	h.Lock()
	defer h.Unlock()

	log.WithField(
		"request",
		req,
	).Info("UpdateResourcePool called")

	resPoolID := req.GetId()
	resPoolConfig := req.GetConfig()

	resourcePoolConfigData := ResourcePoolConfigData{
		ID:                 resPoolID,
		ResourcePoolConfig: resPoolConfig,
	}

	// perform validation on resource pool resPoolConfig
	if err := h.resPoolConfigValidator.Validate(resourcePoolConfigData); err != nil {
		h.metrics.UpdateResourcePoolFail.Inc(1)
		log.WithError(
			err,
		).Infof(
			"Error validating respoolID: %s in store",
			resPoolID.Value,
		)
		return &respool.UpdateResponse{
			Error: &respool.UpdateResponse_Error{
				InvalidResourcePoolConfig: &respool.InvalidResourcePoolConfig{
					Id:      resPoolID,
					Message: err.Error(),
				},
			},
		}, nil, nil
	}

	// needed for rollback
	existingResPool, err := h.resPoolTree.Get(resPoolID)
	if err != nil {
		h.metrics.UpdateResourcePoolFail.Inc(1)
		log.WithError(
			err,
		).Infof(
			"Error fetching respoolID: %s",
			resPoolID.Value,
		)
		return &respool.UpdateResponse{
			Error: &respool.UpdateResponse_Error{
				// TODO  differentiate between n/w errors vs other data errors
				NotFound: &respool.ResourcePoolNotFound{
					Id:      resPoolID,
					Message: err.Error(),
				},
			},
		}, nil, nil
	}

	// update persistent store
	if err := h.store.UpdateResourcePool(resPoolID, resPoolConfig); err != nil {
		h.metrics.UpdateResourcePoolFail.Inc(1)
		log.WithError(
			err,
		).Infof(
			"Error updating respoolID: %s in store",
			resPoolID.Value,
		)
		return &respool.UpdateResponse{
			Error: &respool.UpdateResponse_Error{
				// TODO  differentiate between n/w errors
				// vs other data errors
				NotFound: &respool.ResourcePoolNotFound{
					Id:      resPoolID,
					Message: err.Error(),
				},
			},
		}, nil, nil
	}

	// update the in-memory data structure
	if err := h.resPoolTree.Upsert(resPoolID, resPoolConfig); err != nil {
		// rollback to a previous version if any errors
		h.metrics.UpdateResourcePoolFail.Inc(1)
		log.WithError(
			err,
		).Info(
			"Error updating respoolID: %s in memory tree",
			resPoolID.Value,
		)

		// update with existing
		if err := h.store.UpdateResourcePool(
			resPoolID,
			existingResPool.ResourcePoolConfig(),
		); err != nil {
			log.WithError(
				err,
			).Infof(
				`Error rolling back respoolID:
				%s in store`,
				resPoolID.Value,
			)
			h.metrics.UpdateResourcePoolRollbackFail.Inc(1)
			return &respool.UpdateResponse{}, nil, err
		}
	}

	h.metrics.UpdateResourcePoolSuccess.Inc(1)
	return &respool.UpdateResponse{}, nil, nil
}

// Registerprocs will register all api's for end points
func (h *serviceHandler) registerProcs(d yarpc.Dispatcher) {
	d.Register(
		json.Procedure(
			"ResourceManager.CreateResourcePool",
			h.CreateResourcePool,
		),
	)
	d.Register(
		json.Procedure(
			"ResourceManager.GetResourcePool",
			h.GetResourcePool,
		),
	)
	d.Register(
		json.Procedure(
			"ResourceManager.DeleteResourcePool",
			h.DeleteResourcePool,
		),
	)
	d.Register(
		json.Procedure(
			"ResourceManager.UpdateResourcePool",
			h.UpdateResourcePool,
		),
	)
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
		log.Warn(
			`Resource Manager is already stopped,
			no action will be performed`,
		)
		return nil
	}
	atomic.StoreInt32(&h.runningState, runningStateNotStarted)
	return nil
}
