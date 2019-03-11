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

package prune

import (
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/atomic"
	"github.com/uber-go/tally"

	"github.com/uber/peloton/pkg/hostmgr/offer/offerpool"
)

// placingHostPruner implements interface HostPruner
type placingHostPruner struct {
	offerPool offerpool.Pool
	scope     tally.Scope
}

// NewPlacingHostPruner initializes the host pruner for an OfferPool which
// prunes expired hosts in Placing state
func NewPlacingHostPruner(pool offerpool.Pool, scope tally.Scope) HostPruner {
	return &placingHostPruner{
		offerPool: pool,
		scope:     scope,
	}
}

// For each host of the offerPool, the hostSummary status gets reset
// from PlacingOffer back to ReadyOffer if the PlacingOffer status has expired
func (h *placingHostPruner) Prune(_ *atomic.Bool) {
	log.Debug("Running placing host pruning")
	prunedHostnames := h.offerPool.ResetExpiredPlacingHostSummaries(time.Now())
	h.scope.Counter("pruned").Inc(int64(len(prunedHostnames)))
	if len(prunedHostnames) > 0 {
		log.WithField("hosts", prunedHostnames).
			WithField("state", "PLACING").
			Warn("Hosts pruned")
	}
}
