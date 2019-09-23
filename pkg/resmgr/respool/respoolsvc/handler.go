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

package respoolsvc

import (
	"context"
	"sync"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/respool"

	"github.com/uber/peloton/pkg/common"
	res "github.com/uber/peloton/pkg/resmgr/respool"
	"github.com/uber/peloton/pkg/resmgr/scalar"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
)

const (
	resPoolNotFoundErrString  = "resource pool not found"
	resPoolDeleteErrString    = "resource pool could not be deleted"
	resPoolIsBusyErrString    = "resource pool is busy"
	resPoolIsNotLeafErrString = "resource pool is not leaf"
)

// ServiceHandler implements peloton.api.respool.ResourcePoolService
type ServiceHandler struct {
	sync.Mutex

	// respool store
	resPoolOps ormobjects.ResPoolOps
	// The in-memory resource pool tree
	resPoolTree res.Tree
	// validator to validate the mutations
	resPoolConfigValidator res.Validator
	// handler metrics
	metrics *res.Metrics
}

// InitServiceHandler returns a new handler for ResourcePoolService.
func InitServiceHandler(
	d *yarpc.Dispatcher,
	parent tally.Scope,
	tree res.Tree,
	resPoolOps ormobjects.ResPoolOps,
) *ServiceHandler {

	scope := parent.SubScope("respool")
	metrics := res.NewMetrics(scope)

	// Initialize Resource Pool Config Validator.
	resPoolConfigValidator, err := res.NewResourcePoolConfigValidator(tree)

	if err != nil {
		log.Fatalf(
			`Error initializing resource pool
			config validator: %v`,
			err,
		)
	}

	handler := &ServiceHandler{
		metrics:                metrics,
		resPoolTree:            tree,
		resPoolConfigValidator: resPoolConfigValidator,
		resPoolOps:             resPoolOps,
	}

	d.Register(respool.BuildResourceManagerYARPCProcedures(handler))

	return handler
}

// CreateResourcePool will create resource pool.
func (h *ServiceHandler) CreateResourcePool(
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
	resPoolID := &peloton.ResourcePoolID{
		Value: uuid.New(),
	}

	resourcePoolConfigData := res.ResourcePoolConfigData{
		ID:                 resPoolID,
		ResourcePoolConfig: resPoolConfig,
	}

	// perform validation on resource pool resPoolConfig.
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

	// TODO Handle parent of the new_resource_pool_config
	// already has tasks added running, drain, distinguish?

	// insert persistent store.
	if err := h.resPoolOps.Create(ctx, resPoolID, resPoolConfig, "peloton"); err != nil {
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

	// update the in-memory data structure.
	if err := h.resPoolTree.Upsert(resPoolID, resPoolConfig); err != nil {
		// rollback i.e. delete the current version if any errors.
		h.metrics.CreateResourcePoolFail.Inc(1)
		log.WithError(
			err,
		).Infof(
			"Error creating respoolID: %s in memory tree",
			resPoolID.Value,
		)

		if err := h.resPoolOps.Delete(ctx, resPoolID); err != nil {
			log.WithError(err).
				WithField("respool_id", resPoolID.Value).
				Info("Error rolling back respoolID in store")
			h.metrics.CreateResourcePoolRollbackFail.Inc(1)
			return &respool.CreateResponse{}, err
		}
	}

	h.metrics.CreateResourcePoolSuccess.Inc(1)
	return &respool.CreateResponse{
		Result: resPoolID,
	}, nil
}

// GetResourcePool will get resource pool.
func (h *ServiceHandler) GetResourcePool(
	ctx context.Context,
	req *respool.GetRequest) (*respool.GetResponse, error) {

	h.metrics.APIGetResourcePool.Inc(1)
	log.WithField("request", req).Info("GetResourcePool called")

	resPoolID := req.GetId()
	includeChildPools := req.GetIncludeChildPools()

	if resPoolID == nil {
		//TODO temporary solution to unblock,
		// fix with new naming convention
		resPoolID = &peloton.ResourcePoolID{
			Value: common.RootResPoolID,
		}
	}

	resPool, err := h.resPoolTree.Get(resPoolID)
	if err != nil {
		h.metrics.GetResourcePoolFail.Inc(1)
		return &respool.GetResponse{
			Error: &respool.GetResponse_Error{
				NotFound: &respool.ResourcePoolNotFound{
					Id:      resPoolID,
					Message: resPoolNotFoundErrString,
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
							Message: resPoolNotFoundErrString,
						},
					},
				}, nil

			}
			childPoolInfos = append(childPoolInfos, childPool.ToResourcePoolInfo())
		}
	}

	h.metrics.GetResourcePoolSuccess.Inc(1)
	log.WithFields(log.Fields{
		"pool_info":   resPool.ToResourcePoolInfo(),
		"child_pools": childPoolInfos,
	}).Debug("GetResourcePool Response")

	return &respool.GetResponse{
		Poolinfo:   resPool.ToResourcePoolInfo(),
		ChildPools: childPoolInfos,
	}, nil
}

// DeleteResourcePool will delete resource pool.
func (h *ServiceHandler) DeleteResourcePool(
	ctx context.Context,
	req *respool.DeleteRequest) (
	*respool.DeleteResponse,
	error) {

	h.Lock()
	defer h.Unlock()
	h.metrics.APIDeleteResourcePool.Inc(1)
	lookupReq := &respool.LookupRequest{
		Path: &respool.ResourcePoolPath{
			Value: req.Path.Value,
		},
	}
	lookupRes, err := h.LookupResourcePoolID(ctx, lookupReq)

	if err != nil || lookupRes.GetError() != nil {
		h.metrics.DeleteResourcePoolFail.Inc(1)
		resp := h.getDeleteResponse()
		resp.GetError().NotFound = h.getResPoolNotFoundError(req.Path.Value)
		return resp, nil
	}

	resPoolID := lookupRes.GetId()

	if resPoolID == nil {
		h.metrics.DeleteResourcePoolFail.Inc(1)
		resp := h.getDeleteResponse()
		resp.GetError().NotFound = h.getResPoolNotFoundError(req.Path.Value)
		return resp, nil
	}

	resPool, err := h.resPoolTree.Get(resPoolID)
	if err != nil {
		h.metrics.DeleteResourcePoolFail.Inc(1)
		resp := h.getDeleteResponse()
		resp.GetError().NotFound = h.getResPoolNotFoundError(req.Path.Value)
		return resp, nil
	}

	// As if the resource pool is not leaf, Delete method should
	// not let this operation occur. As delete is only supported for
	// leaf resource pools
	if !resPool.IsLeaf() {
		h.metrics.DeleteResourcePoolFail.Inc(1)
		resp := h.getDeleteResponse()
		resp.GetError().IsNotLeaf = h.getResPoolNotLeafError(resPoolID)
		return resp, nil
	}

	// Get the allocation of the resource pool.
	allocation := resPool.GetTotalAllocatedResources()
	// Get the resource pool demand.
	demand := resPool.GetDemand()

	// We need to check if any tasks are running in the resource pool
	// by looking demand or allocation.
	if !(allocation.LessThanOrEqual(scalar.ZeroResource)) ||
		!(demand.LessThanOrEqual(scalar.ZeroResource)) {
		h.metrics.DeleteResourcePoolFail.Inc(1)
		resp := h.getDeleteResponse()
		resp.GetError().IsBusy = h.getResPoolIsBusyError(resPoolID)
		return resp, nil
	}

	// Deleting the respool from In memory tree.
	if err := h.resPoolTree.Delete(resPoolID); err != nil {
		h.metrics.DeleteResourcePoolFail.Inc(1)
		// Logging and returning error as this is the API Failed
		// We need to log the context in resmgr
		log.WithError(err).WithField("respool", resPoolID).Error("delete Respool failed ")

		resp := h.getDeleteResponse()
		resp.GetError().NotDeleted = h.getResPoolNotDeletedError(resPoolID, err)
		return resp, nil
	}

	// Deleting the resource pool from the DB.
	if err := h.resPoolOps.Delete(ctx, resPoolID); err != nil {
		h.metrics.DeleteResourcePoolFail.Inc(1)
		// Logging and returning error as this is the API Failed
		// We need to log the context in resmgr
		log.WithError(err).WithField("respool", resPoolID).Error("delete Respool failed ")
		resp := h.getDeleteResponse()
		resp.GetError().NotDeleted = h.getResPoolNotDeletedError(resPoolID, err)
		return resp, nil
	}
	h.metrics.DeleteResourcePoolSuccess.Inc(1)

	return &respool.DeleteResponse{
		Error: nil,
	}, nil
}

// getDeleteResponse returns the empty respool DeleteResponse
func (h *ServiceHandler) getDeleteResponse() *respool.DeleteResponse {
	return &respool.DeleteResponse{
		Error: &respool.DeleteResponse_Error{},
	}
}

// getResPoolNotFoundError returns the repool not found error
func (h *ServiceHandler) getResPoolNotFoundError(path string,
) *respool.ResourcePoolPathNotFound {
	return &respool.ResourcePoolPathNotFound{
		Path: &respool.ResourcePoolPath{
			Value: path,
		},
		Message: resPoolNotFoundErrString,
	}
}

// getResPoolNotLeafError returns the ResPoolNotLeafError
func (h *ServiceHandler) getResPoolNotLeafError(resPoolID *peloton.ResourcePoolID,
) *respool.ResourcePoolIsNotLeaf {
	return &respool.ResourcePoolIsNotLeaf{
		Id:      resPoolID,
		Message: resPoolIsNotLeafErrString,
	}
}

// getResPoolIsBusyError returns the ResPoolIsBusyError
func (h *ServiceHandler) getResPoolIsBusyError(resPoolID *peloton.ResourcePoolID,
) *respool.ResourcePoolIsBusy {
	return &respool.ResourcePoolIsBusy{
		Id:      resPoolID,
		Message: resPoolIsBusyErrString,
	}
}

// getResPoolNotDeletedError returns ResPoolNotDeletedError
func (h *ServiceHandler) getResPoolNotDeletedError(resPoolID *peloton.ResourcePoolID,
	err error) *respool.ResourcePoolNotDeleted {
	return &respool.ResourcePoolNotDeleted{
		Id:      resPoolID,
		Message: err.Error() + " " + resPoolDeleteErrString,
	}
}

// UpdateResourcePool will update resource pool.
func (h *ServiceHandler) UpdateResourcePool(
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

	if !req.GetForce() {
		resourcePoolConfigData := res.ResourcePoolConfigData{
			ID:                 resPoolID,
			ResourcePoolConfig: resPoolConfig,
		}
		// perform validation on resource pool resPoolConfig.
		err := h.resPoolConfigValidator.Validate(resourcePoolConfigData)
		if err != nil {
			h.metrics.UpdateResourcePoolFail.Inc(1)
			log.WithError(
				err,
			).WithField(
				"respool_id", resPoolID.GetValue()).
				Info(
					"Error validating resource pool:")
			return &respool.UpdateResponse{
				Error: &respool.UpdateResponse_Error{
					InvalidResourcePoolConfig: &respool.InvalidResourcePoolConfig{
						Id:      resPoolID,
						Message: err.Error(),
					},
				},
			}, nil
		}
	}

	// needed for rollback.
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

	// update persistent store.
	if err := h.resPoolOps.Update(ctx, resPoolID, resPoolConfig); err != nil {
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

	// update the in-memory data structure.
	if err := h.resPoolTree.Upsert(resPoolID, resPoolConfig); err != nil {
		// rollback to a previous version if any errors.
		h.metrics.UpdateResourcePoolFail.Inc(1)
		log.WithError(
			err,
		).Infof(
			"Error updating respoolID: %s in memory tree",
			resPoolID.Value,
		)

		// update with existing.
		if err := h.resPoolOps.Update(
			ctx,
			resPoolID,
			existingResPool.ResourcePoolConfig(),
		); err != nil {
			log.WithError(err).
				Infof("Error rolling back respoolID: %s in store",
					resPoolID.Value)
			h.metrics.UpdateResourcePoolRollbackFail.Inc(1)
			return &respool.UpdateResponse{}, err
		}
	}

	h.metrics.UpdateResourcePoolSuccess.Inc(1)
	return &respool.UpdateResponse{}, nil
}

// LookupResourcePoolID returns the resource pool ID for a given resource pool
// path.
func (h *ServiceHandler) LookupResourcePoolID(ctx context.Context,
	req *respool.LookupRequest) (
	*respool.LookupResponse,
	error) {

	h.metrics.APILookupResourcePoolID.Inc(1)
	log.WithField(
		"request",
		req,
	).Info("LookupResourcePoolID called")

	path := req.Path

	validator, err := res.NewResourcePoolConfigValidator(nil)
	if err != nil {
		log.Fatalf(
			`Error initializing resource pool
			config validator: %v`,
			err,
		)
	}
	validator.Register([]res.ResourcePoolConfigValidatorFunc{res.ValidateResourcePoolPath})
	resourcePoolConfigData := res.ResourcePoolConfigData{
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
					Message: resPoolNotFoundErrString,
				},
			},
		}, nil
	}

	if resPool == nil {
		return &respool.LookupResponse{
			Id: nil,
		}, nil
	}

	h.metrics.LookupResourcePoolIDSuccess.Inc(1)

	return &respool.LookupResponse{
		Id: &peloton.ResourcePoolID{
			Value: resPool.ID(),
		},
	}, nil
}

// Query returns the matching resource pools by default returns all.
func (h *ServiceHandler) Query(
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
			resPoolNode, _ := n.Value.(res.ResPool)
			resourcePoolInfos = append(
				resourcePoolInfos,
				resPoolNode.ToResourcePoolInfo(),
			)
		}
	}

	h.metrics.QueryResourcePoolsSuccess.Inc(1)
	resp := &respool.QueryResponse{
		ResourcePools: resourcePoolInfos,
	}
	log.WithField("response", resp).Debug("Query returned")
	return resp, nil
}
