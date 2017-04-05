package reconcile

// TaskReconcilerConfig for task reconciler specific configuration
type TaskReconcilerConfig struct {
	// Initial delay before running reconciliation
	InitialReconcileDelaySec int `yaml:"initial_reconcile_delay_sec"`

	// Task reconciliation interval
	ReconcileIntervalSec int `yaml:"reconcile_interval_sec"`

	// Explicit reconcilication call interval between batches.
	ExplicitReconcileBatchIntervalSec int `yaml:"explicit_reconcile_batch_interval_sec"`

	// Explicit reconcile batch size.
	ExplicitReconcileBatchSize int `yaml:"explicit_reconcile_batch_size"`
}
