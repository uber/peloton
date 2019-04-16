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
