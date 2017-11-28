package placement

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters that track internal state
// of placement processor.
type Metrics struct {
	GetPlacement              tally.Counter
	GetPlacementFail          tally.Counter
	GetPlacementsCallDuration tally.Timer
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	taskFailScope := scope.Tagged(map[string]string{"result": "fail"})
	taskAPIScope := scope.SubScope("task_api")
	getPlacementScope := scope.SubScope("get_placement")

	return &Metrics{
		GetPlacement:              taskAPIScope.Counter("get_placement"),
		GetPlacementFail:          taskFailScope.Counter("get_placement"),
		GetPlacementsCallDuration: getPlacementScope.Timer("call_duration"),
	}
}
