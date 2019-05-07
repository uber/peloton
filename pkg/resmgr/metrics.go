// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

	APILaunchedTasks tally.Counter

	RecoverySuccess             tally.Counter
	RecoveryFail                tally.Counter
	RecoveryRunningSuccessCount tally.Counter
	RecoveryRunningFailCount    tally.Counter
	RecoveryEnqueueFailedCount  tally.Counter
	RecoveryEnqueueSuccessCount tally.Counter
	RecoveryTimer               tally.Timer

	PlacementQueueLen tally.Gauge
	PlacementFailed   tally.Counter

	Elected tally.Gauge
}

// NewMetrics returns a new instance of resmgr.Metrics.
func NewMetrics(scope tally.Scope) *Metrics {
	successScope := scope.Tagged(map[string]string{"result": "success"})
	failScope := scope.Tagged(map[string]string{"result": "fail"})
	timeoutScope := scope.Tagged(map[string]string{"result": "timeout"})
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

		APILaunchedTasks: apiScope.Counter("launched_tasks"),

		RecoverySuccess:             successScope.Counter("recovery"),
		RecoveryFail:                failScope.Counter("recovery"),
		RecoveryRunningSuccessCount: successScope.Counter("task_count"),
		RecoveryRunningFailCount:    failScope.Counter("task_count"),
		RecoveryEnqueueFailedCount:  failScope.Counter("enqueue_task_count"),
		RecoveryEnqueueSuccessCount: successScope.Counter("enqueue_task_count"),
		RecoveryTimer:               recovery.Timer("running_tasks"),

		PlacementQueueLen: placement.Gauge("placement_queue_length"),
		PlacementFailed:   placement.Counter("fail"),

		Elected: serverScope.Gauge("elected"),
	}
}
