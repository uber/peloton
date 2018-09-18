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

	ArchiverRunDuration tally.Timer
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

		ArchiverRunDuration: scope.Timer("archiver_run_duration"),
	}
}
