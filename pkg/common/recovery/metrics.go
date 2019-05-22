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

package recovery

import (
	"github.com/uber-go/tally"
)

// Metrics contains counters to track recovery metrics
type Metrics struct {
	// total active jobs from materialized view
	activeJobsMV tally.Gauge
	// total active jobs from active_jobs table
	activeJobs tally.Gauge
	// counter to track successful backfills to active_jobs table
	activeJobsBackfill tally.Counter
	// counter to track failure to backfill to active_jobs table
	activeJobsBackfillFail tally.Counter
	// counter to track recovery failure case when the job is present in
	// mv_jobs_by_state but not in job_runtime
	missingJobRuntime tally.Counter
	// counter to track recovery failure case when the job is present in
	// job_runtime but not in job_config
	missingJobConfig tally.Counter
}

// NewMetrics returns a new Metrics struct.
func NewMetrics(scope tally.Scope) *Metrics {
	return &Metrics{
		activeJobs:             scope.Gauge("active_jobs"),
		activeJobsMV:           scope.Gauge("active_jobs_mv"),
		activeJobsBackfill:     scope.Counter("active_jobs_backfill"),
		activeJobsBackfillFail: scope.Counter("active_jobs_backfill_fail"),
		missingJobRuntime:      scope.Counter("missing_job_runtime"),
		missingJobConfig:       scope.Counter("missing_job_config"),
	}
}
