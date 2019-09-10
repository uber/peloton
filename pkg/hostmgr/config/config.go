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

	"github.com/uber/peloton/pkg/hostmgr/goalstate"
	"github.com/uber/peloton/pkg/hostmgr/reconcile"
	"github.com/uber/peloton/pkg/hostmgr/watchevent"
)

// Config is Host Manager specific configuration
type Config struct {
	// HTTP port which hostmgr is listening on
	HTTPPort int `yaml:"http_port"`

	// GRPC port which hostmgr is listening on
	GRPCPort int `yaml:"grpc_port"`

	// Time to hold offer for in seconds
	OfferHoldTimeSec int `yaml:"offer_hold_time_sec"`

	// Frequency of running offer pruner
	OfferPruningPeriodSec int `yaml:"offer_pruning_period_sec"`

	// Minimum backoff duration for retrying any Mesos connection.
	MesosBackoffMin time.Duration `yaml:"mesos_backoff_min"`

	// Maximum backoff duration for retrying any Mesos connection.
	MesosBackoffMax time.Duration `yaml:"mesos_backoff_max"`

	// Number of go routines that will ack for status updates to mesos
	TaskUpdateAckConcurrency int `yaml:"taskupdate_ack_concurrency"`

	// Size of the channel buffer of the status updates
	TaskUpdateBufferSize int `yaml:"taskupdate_buffer_size"`

	TaskReconcilerConfig *reconcile.TaskReconcilerConfig `yaml:"task_reconciler"`

	HostmapRefreshInterval time.Duration `yaml:"hostmap_refresh_interval"`

	// Period in sec for running host pruning
	HostPruningPeriodSec time.Duration `yaml:"host_pruning_period_sec"`

	// Period in sec for running host pruning for host in HELD state
	HeldHostPruningPeriodSec time.Duration `yaml:"held_host_pruning_period_sec"`

	// Period for which to wait for host in PLACING state before reset.
	HostPlacingOfferStatusTimeout time.Duration `yaml:"host_placing_offer_status_sec"`

	// Backoff Retry Count to register background worker for Host Manager
	HostMgrBackoffRetryCount int `yaml:"hostmgr_backoff_retry_count"`

	// Backoff Retry Interval in seconds to register background worker for Host Manager
	HostMgrBackoffRetryIntervalSec int `yaml:"hostmgr_backoff_retry_interval_sec"`

	// Host Drainer Period
	HostDrainerPeriod time.Duration `yaml:"host_drainer_period"`

	// Represents scarce resource types such as GPU.
	ScarceResourceTypes []string `yaml:"scarce_resource_types"`

	// Represents slack resource types (revocable resources) such as cpus, mem.
	SlackResourceTypes []string `yaml:"slack_resource_types"`

	// Bin Packing tasks in hosts as much as possible
	BinPacking string `yaml:"bin_packing"`
	// Bin Packing Refresh Interval
	BinPackingRefreshIntervalSec time.Duration `yaml:"bin_packing_refresh_interval"`

	// Watch API specific configuration
	Watch watchevent.Config `yaml:"watch"`

	//Cqos advisor specific configuration
	QoSAdvisorService CqosAdvisorConfig `yaml:"qos_advisor"`

	// EnableHostPool is the config switch to enable host pool logic in Host Manager.
	EnableHostPool bool `yaml:"enable_host_pool"`

	// HostPoolReconcileInterval is the time interval
	// between every host pool reconcile loop.
	HostPoolReconcileInterval time.Duration `yaml:"host_pool_reconcile_interval"`

	// GoalState configuration
	GoalState goalstate.Config `yaml:"goal_state"`
}
