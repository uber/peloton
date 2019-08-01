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

package offerpool

import (
	"context"
	"github.com/uber/peloton/pkg/hostmgr/binpacking"

	"github.com/uber-go/atomic"
)

// Refresher is the interface to refresh bin packed host list
type Refresher interface {
	Refresh(_ *atomic.Bool)
}

// refresher implements interface Refresher
type refresher struct {
	offerPool Pool
}

// NewRefresher initializes the refresher for an OfferPool
func NewRefresher(
	pool Pool) Refresher {
	return &refresher{
		offerPool: pool,
	}
}

// Refresh refreshes the bin packed list, we need to run refresh
// asynchronously to reduce the performance penalty.
func (h *refresher) Refresh(_ *atomic.Bool) {
	for _, ranker := range binpacking.GetRankers() {
		ranker.RefreshRanking(
			context.Background(),
			h.offerPool.GetHostOfferIndex(),
		)
	}
}
