package taskqueue

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters that track
// internal state of the job service
type Metrics struct {
	APIEnqueue  tally.Counter
	Enqueue     tally.Counter
	EnqueueFail tally.Counter
	APIDequeue  tally.Counter
	Dequeue     tally.Counter
	DequeueFail tally.Counter
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {

	successScope := scope.Tagged(map[string]string{"type": "success"})
	failScope := scope.Tagged(map[string]string{"type": "fail"})
	apiScope := scope.SubScope("api")

	return &Metrics{
		APIEnqueue:  apiScope.Counter("enqueue"),
		Enqueue:     successScope.Counter("enqueue"),
		EnqueueFail: failScope.Counter("enqueue"),
		APIDequeue:  apiScope.Counter("dequeue"),
		Dequeue:     successScope.Counter("dequeue"),
		DequeueFail: failScope.Counter("dequeue"),
	}
}
