package config

import (
	"time"

	"code.uber.internal/infra/peloton/hostmgr/reconcile"
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
}
