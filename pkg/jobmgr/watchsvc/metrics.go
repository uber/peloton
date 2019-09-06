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

package watchsvc

import (
	"github.com/uber-go/tally"
)

// Metrics is a placeholder for all metrics in watch api.
type Metrics struct {
	WatchPodCancel   tally.Counter
	WatchPodOverflow tally.Counter

	WatchJobCancel   tally.Counter
	WatchJobOverflow tally.Counter

	CancelNotFound tally.Counter

	// Time takes to acquire lock in watch processor
	ProcessorLockDuration tally.Timer
}

// NewMetrics returns a new instance of watchsvc.Metrics.
func NewMetrics(scope tally.Scope) *Metrics {
	subScope := scope.SubScope("watch")
	return &Metrics{
		WatchPodCancel:   subScope.Counter("watch_pod_cancel"),
		WatchPodOverflow: subScope.Counter("watch_pod_overflow"),

		WatchJobCancel:   subScope.Counter("watch_job_cancel"),
		WatchJobOverflow: subScope.Counter("watch_job_overflow"),

		CancelNotFound: subScope.Counter("cancel_not_found"),

		ProcessorLockDuration: subScope.Timer("processor_lock_duration"),
	}
}
