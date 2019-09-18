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
	"time"

	cqos "github.com/uber/peloton/.gen/qos/v1alpha1"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/hostmgr/hostpool/hostmover"
	"github.com/uber/peloton/pkg/hostmgr/hostpool/manager"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
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
}

// NewResizer creates a new resizer instance.
func NewResizer(
	manager manager.HostPoolManager,
	cqosClient cqos.QoSAdvisorServiceYARPCClient,
	hostMover hostmover.HostMover,
	config Config,
	scope tally.Scope,
) resizer {

	config.normalize()

	return resizer{
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
	// get host metrics map.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	resp, err := r.cqosClient.GetHostMetrics(ctx, &cqos.GetHostMetricsRequest{})
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
	r.metrics.GetPool.Inc(1)

	var hotHosts uint32
	hotHosts = 0
	for hostname := range stateless.Hosts() {
		if m, ok := hosts[hostname]; ok {
			if uint32(m.Score) > r.config.CqosThresholdPerHost {
				hotHosts += 1
			}
		}
	}

	s := r.metrics.RootScope.Tagged(map[string]string{
		"pool": stateless.ID(),
	})

	if hotHosts > r.config.PoolResizeRange.Upper {
		// TODO:
		// pool is hot. try to resize.
		s.Counter("need_hosts").Inc(1)
	} else if hotHosts < r.config.PoolResizeRange.Lower {
		// TODO:
		// pool is cold. try to resize.
		s.Counter("excess_hosts").Inc(1)
	}
	s.Counter("optimum_hosts").Inc(1)
	// pool is within the range. No action needed.
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
