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

import "time"

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

	// default timeout for which a pool should be in hot/cold state. Pool should
	// be resized only after it has been in the current state for this time.
	_minWaitBeforeResize = 10 * time.Minute

	// Default context timeout for Cqos Service API calls.
	_defaultCqosTimeout = 20 * time.Second
)

// PoolResizeRange contains the range of number of hot hosts such that that
// helps the resizer decide when to lend or borrow hosts for this pool.
// Lower: if the number of hot hosts in the pool are <= Lower, the pool can lend hosts.
// Upper: if the number of hot hosts in the pool are > Upper, the pool needs to borrow hosts.
// If the number of hot hosts fall within the upper and lower limits, the pool is considered
// at risk of getting hot, and should not give back borrowed hosts.
type PoolResizeRange struct {
	Lower uint32
	Upper uint32
}

type Config struct {
	// Interval at which resizer runs.
	ResizeInterval time.Duration
	// The number of hosts to be moved across pools as part of one resize operation.
	MoveBatchSize uint32

	// Threshold for cqos score per host after which the host is considered hot.
	CqosThresholdPerHost uint32

	// PoolResizeRange contains the range of number of hot hosts such that that
	// helps the resizer decide when to lend or borrow hosts for this pool.
	PoolResizeRange PoolResizeRange

	// Minimum time for which a pool should stay in hot/cold state before it is
	// resized. This will be used to avoid host moves if a pool fluctuates
	// between hot/cold state frequently.
	MinWaitBeforeResize time.Duration
}

func (c *Config) normalize() {
	if c.CqosThresholdPerHost == 0 {
		c.CqosThresholdPerHost = _cqosScoreThresholdPerHost
	}

	if c.MoveBatchSize == 0 {
		c.MoveBatchSize = _moveBatchSize
	}

	if c.MinWaitBeforeResize.Seconds() == 0 {
		c.MinWaitBeforeResize = _minWaitBeforeResize
	}

	if (c.PoolResizeRange.Lower == 0 && c.PoolResizeRange.Upper == 0) ||
		(c.PoolResizeRange.Lower >= c.PoolResizeRange.Upper) {
		c.PoolResizeRange.Lower = _poolHotMinThreshold
		c.PoolResizeRange.Upper = _poolHotMaxThreshold
	}

	if c.ResizeInterval.Seconds() == 0 {
		c.ResizeInterval = _resizeInterval
	}
}
