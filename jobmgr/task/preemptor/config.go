package preemptor

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters that track internal state
// of task preemptor.
type Metrics struct {
	TaskPreemptSuccess tally.Counter
	TaskPreemptFail    tally.Counter

	GetPreemptibleTasks             tally.Counter
	GetPreemptibleTasksFail         tally.Counter
	GetPreemptibleTasksCallDuration tally.Timer
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	taskSuccessScope := scope.Tagged(map[string]string{"type": "success"})
	taskFailScope := scope.Tagged(map[string]string{"type": "fail"})
	taskAPIScope := scope.SubScope("task_api")
	getTasksToPreemptScope := scope.SubScope("get_preemptible_tasks")

	return &Metrics{
		TaskPreemptSuccess: taskSuccessScope.Counter("preempt"),
		TaskPreemptFail:    taskFailScope.Counter("preempt"),

		GetPreemptibleTasks:             taskAPIScope.Counter("get_preemptible_tasks"),
		GetPreemptibleTasksFail:         taskFailScope.Counter("get_preemptible_tasks"),
		GetPreemptibleTasksCallDuration: getTasksToPreemptScope.Timer("call_duration"),
	}
}
