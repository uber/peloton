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

package config

import (
	"time"

	"github.com/uber/peloton/pkg/auth"
	"github.com/uber/peloton/pkg/common/health"
	"github.com/uber/peloton/pkg/common/leader"
	"github.com/uber/peloton/pkg/common/logging"
	"github.com/uber/peloton/pkg/common/metrics"
)

const (
	// run archiver every 24 hours
	_defaultArchiveInterval = 24 * time.Hour
	// archive jobs over 1 day
	_defaultArchiveStepSize = 24 * time.Hour
	// archive maximum 5000 jobs at a time
	_defaultMaxArchiveEntries = 5000
	// start archiving jobs that have completed 30 days ago
	_defaultArchiveAge = 720 * time.Hour
	// peloton client timeout for Job Query/Delete API
	_defaultPelotonClientTimeout = 20 * time.Second
	// default retry attempts for job query
	_defaultMaxRetryAttemptsJobQuery = 3
	// default backoff for job query
	_defaultRetryIntervalJobQuery = 10 * time.Second
	// default delay when bootstrapping the archiver
	// to account for not overloading jobmgr during recovery
	_defaultBootstrapDelay = 180 * time.Second

	// PelotonArchiver application name
	PelotonArchiver = "peloton-archiver"
)

// Config holds all config to run a peloton-archiver server.
type Config struct {
	Metrics      metrics.Config        `yaml:"metrics"`
	Election     leader.ElectionConfig `yaml:"election"`
	Archiver     ArchiverConfig        `yaml:"archiver"`
	Health       health.Config         `yaml:"health"`
	SentryConfig logging.SentryConfig  `yaml:"sentry"`
	Auth         auth.Config           `yaml:"auth"`
}

// ArchiverConfig contains archiver specific configuration
type ArchiverConfig struct {
	// enabled flag to toggle archiving
	Enable bool `yaml:"enable"`

	// Flag to constraint pod events
	PodEventsCleanup bool `yaml:"pod_events_cleanup"`

	// HTTP port which Archiver is listening on
	HTTPPort int `yaml:"http_port"`

	// gRPC port which Archiver is listening on
	GRPCPort int `yaml:"grpc_port"`

	// Sleep interval between consecutive archiver runs
	// ex: 30m
	ArchiveInterval time.Duration `yaml:"archive_interval"`

	// Maximum number of entries that can be archived on a single run
	MaxArchiveEntries int `yaml:"max_archive_entries"`

	// Minimum age in days of the jobs to be archived, example: (30 * 24)h
	ArchiveAge time.Duration `yaml:"archive_age"`

	// Time duration of how many jobs to archive at a time
	// example: 1h. This means archive jobs within the last 1hour of ArchiveAge
	// per archiver run
	ArchiveStepSize time.Duration `yaml:"archive_step_size"`

	// Peloton client timeout when querying Peloton API
	PelotonClientTimeout time.Duration `yaml:"peloton_client_timeout"`

	// Max number of retry attempts for Job Query API
	MaxRetryAttemptsJobQuery int `yaml:"max_retry_attempts_job_query"`

	// Retry interval for Job Query API
	RetryIntervalJobQuery time.Duration `yaml:"retry_interval_job_query"`

	// Delay for archiver bootstrapping to account for
	// not overloading jobmgr during recovery
	BootstrapDelay time.Duration `yaml:"bootstrap_delay"`

	// Only stream the jobs to external storage if this is set
	// Do not delete the jobs from local storage
	StreamOnlyMode bool `yaml:"stream_only_mode"`

	// Kafka topic used by archiver to stream jobs via filebeat
	KafkaTopic string `yaml:"kafka_topic"`
}

// Normalize configuration by setting unassigned fields to default values.
func (c *ArchiverConfig) Normalize() {
	if c.ArchiveInterval == 0 {
		c.ArchiveInterval = _defaultArchiveInterval
	}
	if c.MaxArchiveEntries == 0 {
		c.MaxArchiveEntries = _defaultMaxArchiveEntries
	}
	if c.ArchiveAge == 0 {
		c.ArchiveAge = _defaultArchiveAge
	}
	if c.PelotonClientTimeout == 0 {
		c.PelotonClientTimeout = _defaultPelotonClientTimeout
	}
	if c.ArchiveStepSize == 0 {
		c.ArchiveStepSize = _defaultArchiveStepSize
	}
	if c.MaxRetryAttemptsJobQuery == 0 {
		c.MaxRetryAttemptsJobQuery = _defaultMaxRetryAttemptsJobQuery
	}
	if c.RetryIntervalJobQuery == 0 {
		c.RetryIntervalJobQuery = _defaultRetryIntervalJobQuery
	}
	if c.BootstrapDelay == 0 {
		c.BootstrapDelay = _defaultBootstrapDelay
	}
}
