package hostmgr

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
}
