package common

import "time"

// PreemptionConfig is the container for preemption related config
// TODO merge resmgr config to common
type PreemptionConfig struct {
	// Boolean value to represent if preemption is enabled to run
	Enabled bool

	// Period to process resource pools for preemption.
	TaskPreemptionPeriod time.Duration `yaml:"task_preemption_period"`

	// This count represents the maximum number of times the allocation can
	// be great than entitlement(for a resource pool) without preemption kicking in.
	// If the value exceeds this number then the preemption logic will kick
	// in to reduce the allocation.
	SustainedOverAllocationCount int `yaml:"sustained_over_allocation_count"`
}

// RecoveryConfig is the container for recovery related config
type RecoveryConfig struct {
	// RecoverFromActiveJobs tells the recovery code to use the active_jobs
	// table for recovery instead of materialized view
	RecoverFromActiveJobs bool `yaml:"recover_from_active_jobs"`
}
