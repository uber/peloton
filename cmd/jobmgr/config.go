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

package main

import (
	"github.com/uber/peloton/pkg/auth"
	"github.com/uber/peloton/pkg/common/health"
	"github.com/uber/peloton/pkg/common/leader"
	"github.com/uber/peloton/pkg/common/logging"
	"github.com/uber/peloton/pkg/common/metrics"
	"github.com/uber/peloton/pkg/jobmgr"
	"github.com/uber/peloton/pkg/middleware/inbound"
	storage "github.com/uber/peloton/pkg/storage/config"
)

// Config holds all config to run a peloton-jobmgr server.
type Config struct {
	Metrics      metrics.Config          `yaml:"metrics"`
	Storage      storage.Config          `yaml:"storage"`
	Election     leader.ElectionConfig   `yaml:"election"`
	JobManager   jobmgr.Config           `yaml:"job_manager"`
	Health       health.Config           `yaml:"health"`
	SentryConfig logging.SentryConfig    `yaml:"sentry"`
	Auth         auth.Config             `yaml:"auth"`
	RateLimit    inbound.RateLimitConfig `yaml:"rate_limit"`
	// APILock defines which APIs are read/write APIs,
	// so when lockdown is requested, the correct APIs are locked.
	APILock inbound.APILockConfig `yaml:"api_lock"`
}
