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

package goalstate

import "time"

const (
	_defaultMaxRetryDelay     = 60 * time.Minute
	_defaultFailureRetryDelay = 10 * time.Second
	_defaultHostWorkerThreads = 10
)

// Config for the goalstate engine.
type Config struct {
	// MaxRetryDelay is the absolute maximum duration between any retry, capping
	// any backoff to this amount.
	MaxRetryDelay time.Duration `yaml:"max_retry_delay"`
	// FailureRetryDelay is the delay for retry, if an operation failed. Backoff
	// will be applied for up to MaxRetryDelay.
	FailureRetryDelay time.Duration `yaml:"failure_retry_delay"`

	NumWorkerHostThreads int `yaml:"host_worker_thread_count"`
}

// normalize configuration by setting unassigned fields to default values.
func (c *Config) normalize() {
	if c.MaxRetryDelay == 0 {
		c.MaxRetryDelay = _defaultMaxRetryDelay
	}
	if c.FailureRetryDelay == 0 {
		c.FailureRetryDelay = _defaultFailureRetryDelay
	}
	if c.NumWorkerHostThreads == 0 {
		c.NumWorkerHostThreads = _defaultHostWorkerThreads
	}
}
