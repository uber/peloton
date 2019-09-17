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

package resizer

import (
	"time"

	cqos "github.com/uber/peloton/.gen/qos/v1alpha1"

	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/hostmgr/hostpool/manager"

	"github.com/uber-go/tally"
)

// Constants
const (
	// default interval for resizing the partition.
	_resizeInterval = 30 * time.Second

	// default number of hosts to move between pools per resize operation.
	_moveBatchSize = 10

	// default threshold for cqos score per host after which the host is considered hot.
	_cqosScoreThresholdPerHost = 60

	// A pool is considered cold if the % of hot hosts in the pool is below this threshold.
	_poolHotMinThreshold = 40

	// A pool is considered hot if the % of hot hosts in the pool is above this threshold.
	_poolHotMaxThreshold = 60
)

type PoolStatus int

const (
	Hot PoolStatus = iota + 1
	Medium
	Cold
)

type resizer struct {
	lf lifecycle.LifeCycle

	// HostPool hostPoolManager service
	hostPoolManager manager.HostPoolManager

	cqosClient cqos.QoSAdvisorServiceYARPCClient

	// TODO: add metrics.
}

// NewResizer creates a new resizer instance.
func NewResizer(
	manager manager.HostPoolManager,
	cqosClient cqos.QoSAdvisorServiceYARPCClient,
	scope tally.Scope,
) resizer {

	return resizer{
		lf:              lifecycle.NewLifeCycle(),
		hostPoolManager: manager,
		cqosClient:      cqosClient,
	}
}

func (r *resizer) run() {
	ticker := time.NewTicker(_resizeInterval)

	for {
		select {
		case <-ticker.C:
			r.runOnce()
		case <-r.lf.StopCh():
			ticker.Stop()
			r.lf.StopComplete()
			return
		}
	}
}

func (r *resizer) runOnce() {
	// get host metrics map.

	// get the stateless pool to resize.

	// get the batch pool to resize.

	// resize if needed based on stateless pool status.
}

// Start starts the resizer.
func (r *resizer) Start() {
	if !r.lf.Start() {
		// already started, skip the action.
		return
	}

	go r.run()
}

// Stop stops the resizer.
func (r *resizer) Stop() {
	r.lf.Stop()
}
