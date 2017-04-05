package resmgr

import "github.com/uber-go/tally"

// Metrics is a placeholder for all metrics in hostmgr.
type Metrics struct {
	APIEnqueueTasks    tally.Counter
	EnqueueTaskSuccess tally.Counter
	EnqueueTaskFail    tally.Counter

	APIDequeueTasks    tally.Counter
	DequeueTaskSuccess tally.Counter
	DequeueTaskFail    tally.Counter

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
	apiScope := scope.SubScope("api")

	placement := scope.SubScope("placement")

	return &Metrics{
		APIEnqueueTasks:    apiScope.Counter("enqueue_tasks"),
		EnqueueTaskSuccess: successScope.Counter("enqueue_tasks"),
		EnqueueTaskFail:    failScope.Counter("enqueue_tasks"),

		APIDequeueTasks:    apiScope.Counter("dequeue_tasks"),
		DequeueTaskSuccess: successScope.Counter("dequeue_tasks"),
		DequeueTaskFail:    failScope.Counter("dequeue_tasks"),

		APISetPlacements:    apiScope.Counter("set_placements"),
		SetPlacementSuccess: successScope.Counter("set_placements"),
		SetPlacementFail:    failScope.Counter("set_placements"),

		APIGetPlacements:    apiScope.Counter("get_placements"),
		GetPlacementSuccess: successScope.Counter("get_placements"),
		GetPlacementFail:    failScope.Counter("get_placements"),

		PlacementQueueLen: placement.Gauge("placement_queue_length"),
	}
}
