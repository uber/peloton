package hostmgr

import (
	"github.com/uber-go/tally"
)

// Metrics is a placeholder for all metrics in hostmgr.
type Metrics struct {
	LaunchTasks              tally.Counter
	LaunchTasksFail          tally.Counter
	LaunchTasksInvalid       tally.Counter
	LaunchTasksInvalidOffers tally.Counter

	AcquireHostOffers        tally.Counter
	AcquireHostOffersInvalid tally.Counter

	KillTasks     tally.Counter
	KillTasksFail tally.Counter

	ReleaseHostOffers tally.Counter
}

// NewMetrics returns a new instance of hostmgr.Metrics.
func NewMetrics(scope tally.Scope) *Metrics {
	return &Metrics{
		LaunchTasks:              scope.Counter("launch_tasks"),
		LaunchTasksFail:          scope.Counter("launch_tasks_fail"),
		LaunchTasksInvalid:       scope.Counter("launch_tasks_invalid"),
		LaunchTasksInvalidOffers: scope.Counter("launch_tasks_invalid_offers"),

		AcquireHostOffers:        scope.Counter("acquire_host_offers"),
		AcquireHostOffersInvalid: scope.Counter("acquire_host_offers_invalid"),

		KillTasks:     scope.Counter("kill_tasks"),
		KillTasksFail: scope.Counter("kill_tasks_fail"),

		ReleaseHostOffers: scope.Counter("release_host_offers"),
	}
}
