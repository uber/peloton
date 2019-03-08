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

package health

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/atomic"
	"github.com/uber-go/tally"

	"github.com/uber/peloton/pkg/common/leader"
)

// Heartbeat is the heartbeat interface
type Heartbeat interface {
	Start()
	Stop()
}

type heartbeat struct {
	sync.Mutex

	Running  atomic.Bool
	stopChan chan struct{}

	metrics           *Metrics
	heartbeatInterval time.Duration
	candidate         leader.Candidate
}

var hb *heartbeat
var onceInitHeartbeat sync.Once

// InitHeartbeat inits heartbeat
func InitHeartbeat(
	parent tally.Scope,
	config Config,
	candidate leader.Candidate) {
	onceInitHeartbeat.Do(func() {
		hb = &heartbeat{
			metrics:           NewMetrics(parent.SubScope("health")),
			heartbeatInterval: config.HeartbeatInterval,
			candidate:         candidate,
		}
		hb.metrics.Init.Inc(1)
		hb.Start()
	})
}

func (*heartbeat) Start() {
	log.Info("Heartbeat start called.")

	hb.Lock()
	defer hb.Unlock()

	if hb.Running.Swap(true) {
		log.Warn("Heartbeater is already running, no-op.")
		return
	}

	go func() {
		defer hb.Running.Store(false)

		for {
			ticker := time.NewTimer(hb.heartbeatInterval)
			select {
			case <-hb.stopChan:
				log.Info("Heartbeater stopped.")
				return
			case t := <-ticker.C:
				log.WithField("tick", t).
					Debug("Emitting heartbeat.")
				hb.metrics.Heartbeat.Update(1)

				// Only send a leader heartbeat metric
				// for the elected leader
				if hb.candidate != nil && hb.candidate.IsLeader() {
					log.WithField("tick", t).
						Debug("Emitting leader metric.")
					hb.metrics.Leader.Update(1)
				} else {
					hb.metrics.Leader.Update(0)
				}
			}
			ticker.Stop()
		}
	}()

	log.Info("Heartbeater started.")
}

func (*heartbeat) Stop() {
	log.Info("Heartbeat stop called.")

	if !hb.Running.Load() {
		log.Warn("Heartbeat is not running, no-op.")
		return
	}

	hb.Lock()
	defer hb.Unlock()

	log.Info("Stopping Heartbeat.")
	hb.stopChan <- struct{}{}

	for hb.Running.Load() {
		time.Sleep(1 * time.Millisecond)
	}

	log.Info("Heartbeat stopped.")
}
