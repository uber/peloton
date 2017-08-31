package tracked

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters that track internal state
// of tracked manager.
type metrics struct {
	queueLength   tally.Gauge
	queuePopDelay tally.Timer
	taskTimer     map[TaskAction]tally.Timer
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func newMetrics(scope tally.Scope) *metrics {
	queueScope := scope.SubScope("queue")
	mgrScope := scope.SubScope("manager")
	return &metrics{
		queueLength:   queueScope.Gauge("length"),
		queuePopDelay: queueScope.Timer("pop_delay"),
		taskTimer: map[TaskAction]tally.Timer{
			NoAction:             mgrScope.Timer("no_action"),
			UntrackAction:        mgrScope.Timer("untrack_action"),
			StartAction:          mgrScope.Timer("start_action"),
			StopAction:           mgrScope.Timer("stop_action"),
			UseGoalVersionAction: mgrScope.Timer("use_goal_version"),
		},
	}
}
