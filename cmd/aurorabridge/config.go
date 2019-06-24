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
	"github.com/uber/peloton/pkg/aurorabridge"
	"github.com/uber/peloton/pkg/auth"
	"github.com/uber/peloton/pkg/common/health"
	"github.com/uber/peloton/pkg/common/leader"
	"github.com/uber/peloton/pkg/common/logging"
	"github.com/uber/peloton/pkg/common/metrics"
	"github.com/uber/peloton/pkg/middleware/inbound"
)

// Config defines aurorabridge configuration.
type Config struct {
	Debug          bool                              `yaml:"debug"`
	HTTPPort       int                               `yaml:"http_port"`
	GRPCPort       int                               `yaml:"grpc_port"`
	Metrics        metrics.Config                    `yaml:"metrics"`
	Health         health.Config                     `yaml:"health"`
	SentryConfig   logging.SentryConfig              `yaml:"sentry"`
	Election       leader.ElectionConfig             `yaml:"election"`
	RespoolLoader  aurorabridge.RespoolLoaderConfig  `yaml:"respool_loader"`
	ServiceHandler aurorabridge.ServiceHandlerConfig `yaml:"service_handler"`
	EventPublisher aurorabridge.EventPublisherConfig `yaml:"event_publisher"`
	Auth           auth.Config                       `yaml:"auth"`
	RateLimit      inbound.RateLimitConfig           `yaml:"rate_limit"`
}
