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

package task

import "time"

// Config is Resource Manager Task specific configuration
type Config struct {
	// Timeout for rm task in statemachine from launching to ready state
	LaunchingTimeout time.Duration `yaml:"launching_timeout"`
	// Timeout for rm task in statemachine from placing to ready state
	PlacingTimeout time.Duration `yaml:"placing_timeout"`
	// This is the backoff period how much it will backoff
	// in each cycle.
	PlacementRetryBackoff time.Duration `yaml:"placement_retry_backoff"`
	// This is the cycle which is going to repeat
	// after these many attempts.
	PlacementRetryCycle float64 `yaml:"placement_retry_cycle"`
	// This is the policy name for the backoff
	// which is going to dictate the backoff
	PolicyName string `yaml:"backoff_policy_name"`
	// This flag will enable/disable the placement backoff policies
	EnablePlacementBackoff bool `yaml:"enable_placement_backoff"`
	// This flag will enable/disable SLA tracking of tasks
	EnableSLATracking bool `yaml:"enable_sla_tracking"`
}
