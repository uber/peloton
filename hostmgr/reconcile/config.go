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
