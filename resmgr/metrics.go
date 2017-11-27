package resmgr

import "github.com/uber-go/tally"

// Metrics is a placeholder for all metrics in resmgr.
type Metrics struct {
	APIEnqueueGangs    tally.Counter
	EnqueueGangSuccess tally.Counter
	EnqueueGangFail    tally.Counter

	APIDequeueGangs    tally.Counter
	DequeueGangSuccess tally.Counter
	DequeueGangTimeout tally.Counter

	APIGetPreemptibleTasks     tally.Counter
	GetPreemptibleTasksSuccess tally.Counter
	GetPreemptibleTasksTimeout tally.Counter

	APISetPlacements    tally.Counter
	SetPlacementSuccess tally.Counter
	SetPlacementFail    tally.Counter

	APIGetPlacements    tally.Counter
	GetPlacementSuccess tally.Counter
	GetPlacementFail    tally.Counter

	RecoverySuccess            tally.Counter
	RecoveryFail               tally.Counter
	RecoverySuccessCount       tally.Counter
	RecoveryFailCount          tally.Counter
	RecoveryEnqueueFailedCount tally.Counter

	PlacementQueueLen tally.Gauge

	Elected tally.Gauge
}

// NewMetrics returns a new instance of resmgr.Metrics.
func NewMetrics(scope tally.Scope) *Metrics {
	successScope := scope.Tagged(map[string]string{"type": "success"})
	failScope := scope.Tagged(map[string]string{"type": "fail"})
	timeoutScope := scope.Tagged(map[string]string{"type": "timeout"})
	apiScope := scope.SubScope("api")
	serverScope := scope.SubScope("server")
	placement := scope.SubScope("placement")
	recovery := scope.SubScope("recovery")

	return &Metrics{
		APIEnqueueGangs:    apiScope.Counter("enqueue_gangs"),
		EnqueueGangSuccess: successScope.Counter("enqueue_gang"),
		EnqueueGangFail:    failScope.Counter("enqueue_gang"),

		APIDequeueGangs:    apiScope.Counter("dequeue_gangs"),
		DequeueGangSuccess: successScope.Counter("dequeue_gangs"),
		DequeueGangTimeout: timeoutScope.Counter("dequeue_gangs"),

		APIGetPreemptibleTasks:     apiScope.Counter("get_preemptible_tasks"),
		GetPreemptibleTasksSuccess: successScope.Counter("get_preemptible_tasks"),
		GetPreemptibleTasksTimeout: timeoutScope.Counter("get_preemptible_tasks"),

		APISetPlacements:    apiScope.Counter("set_placements"),
		SetPlacementSuccess: successScope.Counter("set_placements"),
		SetPlacementFail:    failScope.Counter("set_placements"),

		APIGetPlacements:    apiScope.Counter("get_placements"),
		GetPlacementSuccess: successScope.Counter("get_placements"),
		GetPlacementFail:    failScope.Counter("get_placements"),

		RecoverySuccess:            successScope.Counter("recovery"),
		RecoveryFail:               failScope.Counter("recovery"),
		RecoverySuccessCount:       successScope.Counter("task_count"),
		RecoveryFailCount:          failScope.Counter("task_count"),
		RecoveryEnqueueFailedCount: recovery.Counter("enqueue_task_fail_count"),

		PlacementQueueLen: placement.Gauge("placement_queue_length"),

		Elected: serverScope.Gauge("elected"),
	}
}
