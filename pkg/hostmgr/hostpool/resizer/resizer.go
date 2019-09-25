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
	"context"
	"sync"
	"time"

	cqos "github.com/uber/peloton/.gen/qos/v1alpha1"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/hostmgr/hostpool"
	"github.com/uber/peloton/pkg/hostmgr/hostpool/hostmover"
	"github.com/uber/peloton/pkg/hostmgr/hostpool/manager"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

type PoolStatus int

const (
	NeedHosts PoolStatus = iota + 1
	OptimumHosts
	ExcessHosts
)

type resizer struct {
	// resizer lifecycle manager.
	lf lifecycle.LifeCycle

	// host pool manager to get pool information.
	hostPoolManager manager.HostPoolManager

	// cqos client to get host metrics.
	cqosClient cqos.QoSAdvisorServiceYARPCClient

	// host mover instance to move hosts across pools.
	hostMover hostmover.HostMover

	// resizer metrics.
	metrics *Metrics

	// resizer config.
	config *Config

	// map of <pool-id>:<last time stamp>.
	poolResizeMap sync.Map
}

// NewResizer creates a new resizer instance.
func NewResizer(
	manager manager.HostPoolManager,
	cqosClient cqos.QoSAdvisorServiceYARPCClient,
	hostMover hostmover.HostMover,
	config Config,
	scope tally.Scope,
) *resizer {

	config.normalize()

	return &resizer{
		lf:              lifecycle.NewLifeCycle(),
		hostPoolManager: manager,
		cqosClient:      cqosClient,
		hostMover:       hostMover,
		metrics:         NewMetrics(scope),
		config:          &config,
	}
}

func (r *resizer) run() {
	ticker := time.NewTicker(r.config.ResizeInterval)

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
	ctx, cancel := context.WithTimeout(
		context.Background(),
		_defaultCqosTimeout,
	)
	defer cancel()
	resp, err := r.cqosClient.GetHostMetrics(
		ctx,
		&cqos.GetHostMetricsRequest{},
	)
	if err != nil {
		log.WithError(err).Error("error fetching host metrics")
		r.metrics.GetMetricsFail.Inc(1)
		return
	}
	r.metrics.GetMetrics.Inc(1)

	hosts := resp.GetHosts()
	// get the stateless pool to resize.

	stateless, err := r.hostPoolManager.GetPool(common.StatelessHostPoolID)
	if err != nil {
		log.WithError(err).Error("error fetching stateless pool")
		r.metrics.GetPoolFail.Inc(1)
		return
	}
	shared, err := r.hostPoolManager.GetPool(common.SharedHostPoolID)
	if err != nil {
		log.WithError(err).Error("error fetching shared pool")
		r.metrics.GetPoolFail.Inc(1)
		return
	}

	r.metrics.GetPool.Inc(1)

	statelessHosts := stateless.Hosts()
	currentStatelessHosts := len(statelessHosts)

	if currentStatelessHosts == 0 {
		return
	}

	hotHostCount := 0
	for hostname := range statelessHosts {
		if m, ok := hosts[hostname]; ok {
			if uint32(m.Score) > r.config.CqosThresholdPerHost {
				hotHostCount += 1
			}
		}
	}

	hotHostPercentage := uint32(100 * (hotHostCount / currentStatelessHosts))

	s := r.metrics.RootScope.Tagged(map[string]string{
		"pool": stateless.ID(),
	})

	// TODO: Get the appropriate source/dest pools to move hosts to/from.
	// For now, we assume this:
	// If stateless pool is hot, move hosts from shared to stateless.
	// If stateless pool is cold (and has borrowed hosts), move hosts
	// from stateless to shared pool.
	if hotHostPercentage > r.config.PoolResizeRange.Upper {
		// Stateless pool is hot.
		s.Counter("need_hosts").Inc(1)
		err := r.tryResize(shared, stateless)
		if err != nil {
			return
		}

	} else if hotHostPercentage < r.config.PoolResizeRange.Lower {
		// Stateless pool is cold.
		s.Counter("excess_hosts").Inc(1)
		err := r.tryResize(stateless, shared)
		if err != nil {
			return
		}
	} else {
		s.Counter("optimum_hosts").Inc(1)
		// Since the pool doesn't need resizing, we just remove it from the
		// resize tracker map.
		r.poolResizeMap.Delete(stateless.ID())
	}
}

func (r *resizer) tryResize(srcPool, destPool hostpool.HostPool) error {
	var timeSinceIntf interface{}
	var ok bool

	if timeSinceIntf, ok = r.poolResizeMap.Load(destPool.ID()); !ok {
		// This is the first time the pool has been detected hot/cold.
		r.poolResizeMap.Store(destPool.ID(), time.Now())
		return nil
	}

	timeSince := timeSinceIntf.(time.Time)

	// This pool has been hot/cold since some time. If the pool is in this
	// state for more than a configured threshold duration, resize the pool.
	if time.Since(timeSince) >= r.config.MinWaitBeforeResize {

		// Get the current hosts.
		numSrcHosts := uint32(len(srcPool.Hosts()))
		numDestHosts := uint32(len(destPool.Hosts()))

		if numSrcHosts == 0 {
			s := r.metrics.RootScope.Tagged(map[string]string{
				"pool": srcPool.ID(),
			})
			s.Counter("no_hosts").Inc(1)
			return yarpcerrors.ResourceExhaustedErrorf(
				"no hosts in pool %s", srcPool.ID())
		}

		// TODO: Get desired hosts.
		// If there is a difference between current and desired, it means that
		// a previous move is in progress. If so, we should return and wait for
		// that move to complete.

		srcPoolDesiredCount := int32(numSrcHosts - r.config.MoveBatchSize)
		destPoolDesiredCount := int32(numDestHosts + r.config.MoveBatchSize)

		// TODO: check for min pool size here once it is available as part
		// of the host pool info. Do not reduce the src pool host count below
		// the min pool size.
		if srcPoolDesiredCount <= 0 {
			srcPoolDesiredCount = 0
			destPoolDesiredCount = int32(numDestHosts + numSrcHosts)
		}

		log.WithFields(log.Fields{
			"src_pool":   srcPool.ID(),
			"src_hosts":  srcPoolDesiredCount,
			"dest_pool":  destPool.ID(),
			"dest_hosts": destPoolDesiredCount,
		}).Info("move hosts")

		if err := r.hostMover.MoveHosts(
			context.Background(),
			srcPool.ID(),
			srcPoolDesiredCount,
			destPool.ID(),
			destPoolDesiredCount,
		); err != nil {
			// TODO: handle error gracefully. We would need desired host
			// count to be returned as part of the host pool. Once we have
			// that, we error out here and rest assured that the next run
			// of resizer will take the current and desired host count into
			// account when trying to resize the pools.
			r.metrics.MoveHostsFail.Inc(1)
			return err
		}
		r.metrics.MoveHosts.Inc(1)

		// Now we have resized once, so remove the pool from this map.
		r.poolResizeMap.Delete(destPool.ID())
	}
	return nil
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
