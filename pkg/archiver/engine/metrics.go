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

package engine

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters that track internal state
// of archiver engine.
type Metrics struct {
	ArchiverStart             tally.Counter
	ArchiverJobQuerySuccess   tally.Counter
	ArchiverJobQueryFail      tally.Counter
	ArchiverJobDeleteSuccess  tally.Counter
	ArchiverJobDeleteFail     tally.Counter
	ArchiverNoJobsInTimerange tally.Counter

	PodDeleteEventsFail    tally.Counter
	PodDeleteEventsSuccess tally.Counter

	ArchiverRun                tally.Counter
	ArchiverRunDuration        tally.Timer
	PodDeleteEventsRun         tally.Counter
	PodDeleteEventsRunDuration tally.Timer
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	return &Metrics{
		ArchiverStart:             scope.Counter("archiver_start"),
		ArchiverJobQuerySuccess:   scope.Counter("archiver_job_query_success"),
		ArchiverJobQueryFail:      scope.Counter("archiver_job_query_fail"),
		ArchiverJobDeleteSuccess:  scope.Counter("archiver_job_delete_success"),
		ArchiverJobDeleteFail:     scope.Counter("archiver_job_delete_fail"),
		ArchiverNoJobsInTimerange: scope.Counter("archiver_no_jobs_in_timerange"),
		PodDeleteEventsSuccess:    scope.Counter("pod_delete_events_success"),
		PodDeleteEventsFail:       scope.Counter("pod_delete_events_fail"),

		ArchiverRun:                scope.Counter("archiver_run"),
		ArchiverRunDuration:        scope.Timer("archiver_run_duration"),
		PodDeleteEventsRun:         scope.Counter("pod_delete_events_run"),
		PodDeleteEventsRunDuration: scope.Timer("pod_delete_events_run_duration"),
	}
}
