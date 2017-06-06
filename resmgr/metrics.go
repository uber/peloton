package resmgr

import "github.com/uber-go/tally"

// Metrics is a placeholder for all metrics in hostmgr.
type Metrics struct {
	APIEnqueueGangs    tally.Counter
	EnqueueGangSuccess tally.Counter
	EnqueueGangFail    tally.Counter

	APIDequeueGangs    tally.Counter
	DequeueGangSuccess tally.Counter
	DequeueGangTimeout tally.Counter

	APISetPlacements    tally.Counter
	SetPlacementSuccess tally.Counter
	SetPlacementFail    tally.Counter

	APIGetPlacements    tally.Counter
	GetPlacementSuccess tally.Counter
	GetPlacementFail    tally.Counter

	PlacementQueueLen tally.Gauge
}

// NewMetrics returns a new instance of resmgr.Metrics.
func NewMetrics(scope tally.Scope) *Metrics {
	successScope := scope.Tagged(map[string]string{"type": "success"})
	failScope := scope.Tagged(map[string]string{"type": "fail"})
	timeoutScope := scope.Tagged(map[string]string{"type": "timeout"})
	apiScope := scope.SubScope("api")

	placement := scope.SubScope("placement")

	return &Metrics{
		APIEnqueueGangs:    apiScope.Counter("enqueue_gangs"),
		EnqueueGangSuccess: successScope.Counter("enqueue_gang"),
		EnqueueGangFail:    failScope.Counter("enqueue_gang"),

		APIDequeueGangs:    apiScope.Counter("dequeue_gangs"),
		DequeueGangSuccess: successScope.Counter("dequeue_tasks"),
		DequeueGangTimeout: timeoutScope.Counter("dequeue_tasks"),

		APISetPlacements:    apiScope.Counter("set_placements"),
		SetPlacementSuccess: successScope.Counter("set_placements"),
		SetPlacementFail:    failScope.Counter("set_placements"),

		APIGetPlacements:    apiScope.Counter("get_placements"),
		GetPlacementSuccess: successScope.Counter("get_placements"),
		GetPlacementFail:    failScope.Counter("get_placements"),

		PlacementQueueLen: placement.Gauge("placement_queue_length"),
	}
}
