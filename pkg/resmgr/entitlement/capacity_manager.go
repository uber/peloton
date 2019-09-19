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
	"fmt"
	"time"

	v0_hostsvc "github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	v1_hostsvc "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha/svc"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/api"

	"github.com/pkg/errors"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
)

const _defaultHostMgrTimeout = 10 * time.Second

// ResourceCapacity contains total quantity of various kinds of host resources.
type ResourceCapacity struct {
	// Physical capacity.
	Physical map[string]float64
	// Revocable capacity.
	Slack map[string]float64
}

type v0CapacityManager struct {
	// v0 client to get the v0 cluster capacity.
	hostManagerV0 v0_hostsvc.InternalHostServiceYARPCClient
}

type v1AlphaCapacityManager struct {
	// v1 client to get the v1Alpha cluster capacity.
	hostManagerV1 v1_hostsvc.HostManagerServiceYARPCClient
}

// CapacityManager interface defines method to get cluster capacity.
type CapacityManager interface {
	GetCapacity(
		ctx context.Context,
	) (
		// total capacity of the cluster
		total map[string]float64,
		// slack capacity of the cluster
		slack map[string]float64,
		err error,
	)
	// Return capacity of each host-pool.
	GetHostPoolCapacity(context.Context) (
		map[string]*ResourceCapacity,
		error,
	)
}

// newv1AlphaCapacityManager returns an instance of the v1Alpha capacity manager.
func newV1AlphaCapacityManager(
	dispatcher *yarpc.Dispatcher,
) CapacityManager {
	return &v1AlphaCapacityManager{
		hostManagerV1: v1_hostsvc.NewHostManagerServiceYARPCClient(
			dispatcher.ClientConfig(
				common.PelotonHostManager),
		),
	}
}

// newV0CapacityManager returns an instance of the v0 capacity manager.
func newV0CapacityManager(
	dispatcher *yarpc.Dispatcher,
) CapacityManager {
	return &v0CapacityManager{
		hostManagerV0: v0_hostsvc.NewInternalHostServiceYARPCClient(
			dispatcher.ClientConfig(
				common.PelotonHostManager),
		),
	}
}

// getCapacityManager gets hostmgr API specific capacity manager instance.
func getCapacityManager(
	version api.Version,
	dispatcher *yarpc.Dispatcher,
) CapacityManager {
	if version.IsV1() {
		return newV1AlphaCapacityManager(dispatcher)
	} else {
		return newV0CapacityManager(dispatcher)
	}
}

// GetCapacity implements the GetCapacity method for v1Alpha capacity manager.
func (c *v1AlphaCapacityManager) GetCapacity(
	ctx context.Context,
) (
	total map[string]float64,
	slack map[string]float64,
	err error,
) {
	total = make(map[string]float64)
	slack = make(map[string]float64)

	ctx, cancel := context.WithTimeout(ctx, _defaultHostMgrTimeout)
	defer cancel()

	request := &v1_hostsvc.ClusterCapacityRequest{}
	response, err := c.hostManagerV1.ClusterCapacity(ctx, request)
	if err != nil {
		return nil, nil, errors.Wrap(err, "v1Alpha ClusterCapacity failed: ")
	}

	// Convert response to scalar capacity map.
	for _, res := range response.GetCapacity() {
		total[res.Kind] = res.Capacity
	}
	for _, res := range response.GetSlackCapacity() {
		slack[res.Kind] = res.Capacity
	}

	return total, slack, nil
}

// GetHostPoolCapacity implements GetHostPoolCapacity method for
// v1Alpha capacity manager.
func (c *v1AlphaCapacityManager) GetHostPoolCapacity(context.Context) (
	map[string]*ResourceCapacity,
	error,
) {
	return nil, yarpcerrors.UnimplementedErrorf("")
}

// GetCapacity implements the GetCapacity method for v0 capacity manager.
func (c *v0CapacityManager) GetCapacity(
	ctx context.Context,
) (
	total map[string]float64,
	slack map[string]float64,
	err error,
) {
	total = make(map[string]float64)
	slack = make(map[string]float64)

	ctx, cancel := context.WithTimeout(ctx, _defaultHostMgrTimeout)
	defer cancel()

	request := &v0_hostsvc.ClusterCapacityRequest{}
	response, err := c.hostManagerV0.ClusterCapacity(ctx, request)
	if err != nil {
		return nil, nil, errors.Wrap(err, "v0 ClusterCapacity failed: ")
	}

	if respErr := response.GetError(); respErr != nil {
		return nil, nil,
			fmt.Errorf("v0 ClusterCapacity failed: %s", respErr.String())
	}

	// Convert response to scalar capacity map.
	for _, res := range response.GetPhysicalResources() {
		total[res.Kind] = res.Capacity
	}
	for _, res := range response.GetPhysicalSlackResources() {
		slack[res.Kind] = res.Capacity
	}

	return total, slack, nil
}

// GetHostPoolCapacity implements GetHostPoolCapacity method for
// v0 capacity manager.
func (c *v0CapacityManager) GetHostPoolCapacity(ctx context.Context) (
	map[string]*ResourceCapacity,
	error,
) {
	ctx, cancel := context.WithTimeout(ctx, _defaultHostMgrTimeout)
	defer cancel()

	request := &v0_hostsvc.GetHostPoolCapacityRequest{}
	response, err := c.hostManagerV0.GetHostPoolCapacity(ctx, request)
	if err != nil {
		return nil, errors.Wrap(err, "v0 GetHostPoolCapacity failed: ")
	}

	result := make(map[string]*ResourceCapacity)
	for _, pool := range response.GetPools() {
		rc := &ResourceCapacity{
			Physical: make(map[string]float64),
			Slack:    make(map[string]float64),
		}
		for _, res := range pool.GetPhysicalCapacity() {
			rc.Physical[res.GetKind()] = res.GetCapacity()
		}
		for _, res := range pool.GetSlackCapacity() {
			rc.Slack[res.GetKind()] = res.GetCapacity()
		}
		result[pool.GetPoolName()] = rc
	}
	return result, nil
}
