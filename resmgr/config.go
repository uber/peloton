package resmgr

import (
	"time"

	"code.uber.internal/infra/peloton/resmgr/common"
	"code.uber.internal/infra/peloton/resmgr/task"
)

// Config is Resource Manager specific configuration
type Config struct {
	// HTTP port which hostmgr is listening on
	HTTPPort int `yaml:"http_port"`

	// GRPC port which hostmgr is listening on
	GRPCPort int `yaml:"grpc_port"`

	// Period to run task scheduling in seconds
	TaskSchedulingPeriod time.Duration `yaml:"task_scheduling_period"`

	// Period to run entitlement calculator
	EntitlementCaculationPeriod time.Duration `yaml:"entitlement_calculation_period"`

	// Period to run task reconciliation
	TaskReconciliationPeriod time.Duration `yaml:"task_reconciliation_period"`

	// RM Task Config
	RmTaskConfig *task.Config `yaml:"task"`

	// Config for task preemption
	PreemptionConfig *common.PreemptionConfig `yaml:"preemption"`

	// Period to run host drainer
	HostDrainerPeriod time.Duration `yaml:"host_drainer_period"`

	// RecoveryConfig to recover jobs on resmgr restart
	RecoveryConfig *common.RecoveryConfig `yaml:"recovery"`
}
