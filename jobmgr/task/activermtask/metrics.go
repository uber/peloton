package activermtask

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters and timers that track
type Metrics struct {
	ActiveTaskQuerySuccess tally.Counter
	ActiveTaskQueryFail    tally.Counter
	UpdaterRMTasksDuraion  tally.Timer
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	activeTaskQuerySuccess := scope.Tagged(map[string]string{"result": "success"})
	activeTaskQueryFailed := scope.Tagged(map[string]string{"result": "fail"})

	return &Metrics{
		ActiveTaskQuerySuccess: activeTaskQuerySuccess.Counter("active_task_query"),
		ActiveTaskQueryFail:    activeTaskQueryFailed.Counter("active_task_query"),
		UpdaterRMTasksDuraion:  scope.Timer("update_rm_tasks_duration"),
	}

}
