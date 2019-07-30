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

import (
	"time"

	"golang.org/x/time/rate"
)

const (
	_defaultMaxRetryDelay            = 60 * time.Minute
	_defaultFailureRetryDelay        = 10 * time.Second
	_defaultLaunchTimeRetryDuration  = 180 * time.Minute
	_defaultStartTimeRetryDuration   = 180 * time.Minute
	_defaultJobRuntimeUpdateInterval = 1 * time.Second
	_defaultInitialTaskBackoff       = 30 * time.Second
	_defaultMaxTaskBackoff           = 60 * time.Minute

	// Job worker threads should be small because job create and job kill
	// actions create 1000 parallel threads to update the DB, and if too
	// many job worker threads do these operations in parallel, it can
	// lead to DB timeouts due to increased load.
	// Setting it to 50 based on experiments in production.
	_defaultJobWorkerThreads  = 50
	_defaultTaskWorkerThreads = 1000
	// TODO determine the correct value of the number of
	// parallel threads to run job updates.
	_defaultUpdateWorkerThreads = 100
)

// Config for the goalstate engine.
type Config struct {
	// MaxRetryDelay is the absolute maximum duration between any retry, capping
	// any backoff to this abount.
	MaxRetryDelay time.Duration `yaml:"max_retry_delay"`
	// FailureRetryDelay is the delay for retry, if an operation failed. Backoff
	// will be applied for up to MaxRetryDelay.
	FailureRetryDelay time.Duration `yaml:"failure_retry_delay"`

	// LaunchTimeout is the timeout value for the LAUNCHED state.
	// If no update is received from Mesos within this timeout value,
	// the task will be re-queued to the resource manager for placement
	// with a new mesos task id.
	LaunchTimeout time.Duration `yaml:"launch_timeout"`
	// StartTimeout is the timeout value for the STARTING state.
	// If no update is received from Mesos within this timeout value,
	// the task will be re-queued to the resource manager for placement
	// with a new mesos task id.
	StartTimeout time.Duration `yaml:"start_timeout"`

	// JobRuntimeUpdateInterval is the interval at which batch jobs runtime updater is run.
	JobBatchRuntimeUpdateInterval time.Duration `yaml:"job_batch_runtime_update_interval"`
	// JobServiceRuntimeUpdateInterval is the interval at which service jobs runtime updater is run.
	JobServiceRuntimeUpdateInterval time.Duration `yaml:"job_service_runtime_update_interval"`

	// NumWorkerJobThreads is the number of worker threads in the pool
	// serving the job goal state engine. This number indicates the maximum
	// number of jobs which can be parallely processed by the goal state engine.
	NumWorkerJobThreads int `yaml:"job_worker_thread_count"`
	// NumWorkerTaskThreads is the number of worker threads in the pool
	// serving the task goal state engine. This number indicates the maximum
	// number of tasks which can be parallely processed by the goal state engine.
	NumWorkerTaskThreads int `yaml:"task_worker_thread_count"`
	// NumWorkerJobThreads is the number of worker threads in the pool
	// serving the job update goal state engine. This number indicates
	// the maximum number of job updates which can be parallely processed
	// by the goal state engine.
	NumWorkerUpdateThreads int `yaml:"update_worker_thread_count"`

	// InitialTaskBackoff defines the initial back-off delay to recreate
	// failed tasks. Back off is calculated as
	// min(InitialTaskBackOff * 2 ^ (failureCount - 1), MaxBackoff).
	// Default to 30s.
	InitialTaskBackoff time.Duration `yaml:"initial_task_backoff"`

	// InitialTaskBackoff defines the max back-off delay to recreate
	// failed tasks. Back off is calculated as
	// min(InitialTaskBackOff * 2 ^ (failureCount - 1), MaxBackoff).
	// Default to 1h.
	MaxTaskBackoff time.Duration `yaml:"max_task_backoff"`

	// RateLimiterConfig defines rate limiter config
	RateLimiterConfig RateLimiterConfig `yaml:"rate_limit"`
}

type RateLimiterConfig struct {
	// ExecutorShutdown rate limit config for executor shutdown call to hostmgr
	ExecutorShutdown TokenBucketConfig `yaml:"executor_shutdown"`
	// TaskKill rate limit config for task stop call to hostmgr
	TaskKill TokenBucketConfig `yaml:"task_kill"`
}

// TokenBucketConfig is the config for rate limiting
type TokenBucketConfig struct {
	// Rate for the token bucket rate limit algorithm,
	// If Rate <=0, there would be no rate limit
	Rate rate.Limit
	// Burst for the token bucket rate limit algorithm,
	// If Burst <=0, there would be no rate limit
	Burst int
}

// normalize configuration by setting unassigned fields to default values.
func (c *Config) normalize() {
	if c.MaxRetryDelay == 0 {
		c.MaxRetryDelay = _defaultMaxRetryDelay
	}
	if c.FailureRetryDelay == 0 {
		c.FailureRetryDelay = _defaultFailureRetryDelay
	}

	if c.LaunchTimeout == 0 {
		c.LaunchTimeout = _defaultLaunchTimeRetryDuration
	}

	if c.StartTimeout == 0 {
		c.StartTimeout = _defaultStartTimeRetryDuration
	}

	if c.JobBatchRuntimeUpdateInterval == 0 {
		c.JobBatchRuntimeUpdateInterval = _defaultJobRuntimeUpdateInterval
	}
	if c.JobServiceRuntimeUpdateInterval == 0 {
		c.JobServiceRuntimeUpdateInterval = _defaultJobRuntimeUpdateInterval
	}

	if c.NumWorkerJobThreads == 0 {
		c.NumWorkerJobThreads = _defaultJobWorkerThreads
	}
	if c.NumWorkerTaskThreads == 0 {
		c.NumWorkerTaskThreads = _defaultTaskWorkerThreads
	}
	if c.NumWorkerUpdateThreads == 0 {
		c.NumWorkerUpdateThreads = _defaultUpdateWorkerThreads
	}

	if c.InitialTaskBackoff == 0 {
		c.InitialTaskBackoff = _defaultInitialTaskBackoff
	}

	if c.MaxTaskBackoff == 0 {
		c.MaxTaskBackoff = _defaultMaxTaskBackoff
	}

	if c.RateLimiterConfig.TaskKill.Rate <= 0 || c.RateLimiterConfig.TaskKill.Burst <= 0 {
		c.RateLimiterConfig.TaskKill.Rate = rate.Inf
	}

	if c.RateLimiterConfig.ExecutorShutdown.Rate <= 0 || c.RateLimiterConfig.ExecutorShutdown.Burst <= 0 {
		c.RateLimiterConfig.ExecutorShutdown.Rate = rate.Inf
	}
}
