package respool

import (
	"context"
	"sync"
	"sync/atomic"

	"code.uber.internal/infra/peloton/storage"

	log "github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"

	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
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
	dispatcher             *yarpc.Dispatcher
	runningState           int32
	resPoolTree            Tree
	resPoolConfigValidator Validator
	jobStore               storage.JobStore
	taskStore              storage.TaskStore
}

// Singleton service handler for ResourcePoolService
var handler *serviceHandler

// InitServiceHandler initializes the handler for ResourcePoolService
func InitServiceHandler(
	d *yarpc.Dispatcher,
	parent tally.Scope,
	store storage.ResourcePoolStore,
	jobStore storage.JobStore,
	taskStore storage.TaskStore) {

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

	InitTree(scope, store, jobStore, taskStore)

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
		jobStore:               jobStore,
		taskStore:              taskStore,
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
	req *respool.CreateRequest) (
	*respool.CreateResponse,
	error) {

	h.Lock()
	defer h.Unlock()

	h.metrics.APICreateResourcePool.Inc(1)
	log.WithField(
		"request",
		req,
	).Info("CreateResourcePool called")

	resPoolConfig := req.GetConfig()
	resPoolID := &respool.ResourcePoolID{
		Value: uuid.New(),
	}

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
		}, nil
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
		}, nil
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
			return &respool.CreateResponse{}, err
		}
	}

	h.metrics.CreateResourcePoolSuccess.Inc(1)
	return &respool.CreateResponse{
		Result: resPoolID,
	}, nil
}

// GetResourcePool will get resource pool
func (h *serviceHandler) GetResourcePool(
	ctx context.Context,
	req *respool.GetRequest) (*respool.GetResponse, error) {

	h.metrics.APIGetResourcePool.Inc(1)
	log.WithField("request", req).Info("GetResourcePool called")

	resPoolID := req.GetId()
	includeChildPools := req.GetIncludeChildPools()

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
		}, nil
	}

	resPoolInfo := resPool.ToResourcePoolInfo()

	childPoolInfos := make([]*respool.ResourcePoolInfo, 0)

	if includeChildPools &&
		resPoolInfo.Children != nil &&
		len(resPoolInfo.Children) > 0 {

		for _, childID := range resPoolInfo.Children {
			childPool, err := h.resPoolTree.Get(childID)
			if err != nil {
				h.metrics.GetResourcePoolFail.Inc(1)
				return &respool.GetResponse{
					Error: &respool.GetResponse_Error{
						NotFound: &respool.ResourcePoolNotFound{
							Id:      childID,
							Message: "resource pool not found",
						},
					},
				}, nil

			}
			childPoolInfos = append(childPoolInfos, childPool.ToResourcePoolInfo())
		}
	}

	h.metrics.GetResourcePoolSuccess.Inc(1)
	return &respool.GetResponse{
		Poolinfo:   resPool.ToResourcePoolInfo(),
		ChildPools: childPoolInfos,
	}, nil
}

// DeleteResourcePool will delete resource pool
func (h *serviceHandler) DeleteResourcePool(
	ctx context.Context,
	req *respool.DeleteRequest) (
	*respool.DeleteResponse,
	error) {

	h.Lock()
	defer h.Unlock()

	h.metrics.APIDeleteResourcePool.Inc(1)
	log.WithField(
		"request",
		req,
	).Info("DeleteResourcePool called")

	log.Fatal("Not implemented")
	return &respool.DeleteResponse{}, nil
}

// UpdateResourcePool will update resource pool
func (h *serviceHandler) UpdateResourcePool(
	ctx context.Context,
	req *respool.UpdateRequest) (
	*respool.UpdateResponse,
	error) {

	h.Lock()
	defer h.Unlock()

	h.metrics.APIUpdateResourcePool.Inc(1)
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
		}, nil
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
		}, nil
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
		}, nil
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
			return &respool.UpdateResponse{}, err
		}
	}

	h.metrics.UpdateResourcePoolSuccess.Inc(1)
	return &respool.UpdateResponse{}, nil
}

// LookupResourcePoolID returns the resource pool ID for a given resource pool path
func (h *serviceHandler) LookupResourcePoolID(ctx context.Context,
	req *respool.LookupRequest) (
	*respool.LookupResponse,
	error) {

	h.metrics.APILookupResourcePoolID.Inc(1)
	log.WithField(
		"request",
		req,
	).Info("LookupResourcePoolID called")

	path := req.Path

	validator, err := NewResourcePoolConfigValidator(nil)
	if err != nil {
		log.Fatalf(
			`Error initializing resource pool
			config validator: %v`,
			err,
		)
	}
	validator.Register([]ResourcePoolConfigValidatorFunc{ValidateResourcePoolPath})
	resourcePoolConfigData := ResourcePoolConfigData{
		Path: path,
	}
	err = validator.Validate(resourcePoolConfigData)
	if err != nil {
		log.WithField("path", path).
			WithError(err).Error("failed validating resource path")
		h.metrics.LookupResourcePoolIDFail.Inc(1)
		return &respool.LookupResponse{
			Error: &respool.LookupResponse_Error{
				InvalidPath: &respool.InvalidResourcePoolPath{
					Path:    path,
					Message: err.Error(),
				},
			},
		}, nil
	}

	resPool, err := h.resPoolTree.GetByPath(path)
	if err != nil {
		log.WithField("path", path).
			WithError(err).Error("failed finding resource path")
		h.metrics.LookupResourcePoolIDFail.Inc(1)
		return &respool.LookupResponse{
			Error: &respool.LookupResponse_Error{
				NotFound: &respool.ResourcePoolPathNotFound{
					Path:    path,
					Message: "resource pool not found",
				},
			},
		}, nil
	}

	h.metrics.LookupResourcePoolIDSuccess.Inc(1)

	return &respool.LookupResponse{
		Id: &respool.ResourcePoolID{
			Value: resPool.ID(),
		},
	}, nil
}

// Query returns the matching resource pools by default returns all
func (h *serviceHandler) Query(
	ctx context.Context,
	req *respool.QueryRequest) (
	*respool.QueryResponse,
	error) {

	h.metrics.APIQueryResourcePools.Inc(1)
	log.WithField(
		"request",
		req,
	).Info("Query called")

	var resourcePoolInfos []*respool.ResourcePoolInfo

	// TODO use query request to read filters
	nodeList := h.resPoolTree.GetAllNodes(false)
	if nodeList != nil {
		for n := nodeList.Front(); n != nil; n = n.Next() {
			resPoolNode, _ := n.Value.(ResPool)
			resourcePoolInfos = append(
				resourcePoolInfos,
				resPoolNode.ToResourcePoolInfo(),
			)
		}
	}

	h.metrics.QueryResourcePoolsSuccess.Inc(1)
	return &respool.QueryResponse{
		ResourcePools: resourcePoolInfos,
	}, nil
}

// registerProcs will register all API's for end points
func (h *serviceHandler) registerProcs(d *yarpc.Dispatcher) {
	d.Register(respool.BuildResourceManagerYarpcProcedures(h))
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
