package deadline

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters that track internal state
// of deadline tracker.
type Metrics struct {
	TaskKillSuccess tally.Counter
	TaskKillFail    tally.Counter
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	taskSuccessScope := scope.Tagged(map[string]string{"result": "success"})
	taskFailScope := scope.Tagged(map[string]string{"result": "fail"})

	return &Metrics{
		TaskKillSuccess: taskSuccessScope.Counter("deadline"),
		TaskKillFail:    taskFailScope.Counter("deadline"),
	}
}
