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

package metrics

import (
	"github.com/uber-go/tally"
)

// Metrics contains all the metrics relevant to the scheduler
type Metrics struct {
	// Running indicates if the scheduler is currently running or not
	Running tally.Gauge

	// TaskLaunchDispatches counts the number of times we call into
	// the task launcher to request tasks to be launched and it succeeds
	// NOTE: one increment can correspond to multiple mesos
	// tasks being launched
	TaskLaunchDispatches tally.Counter

	// TaskLaunchDispatchesFail counts the number of times we call
	// into the task launcher to request tasks to be launched and it fails
	// NOTE: one increment can correspond to multiple mesos
	// tasks being launched
	TaskLaunchDispatchesFail tally.Counter

	// TasksDequeued is gauge which measures the number of tasks dequeued by the
	// placement engine in a single placement cycle.
	TasksDequeued tally.Gauge

	// OfferStarved indicates the number of times the scheduler
	// attempted to get an Offer to request a task launch, but was
	// returned an empty set.
	OfferStarved tally.Counter

	// OfferGet indicates the number of times the scheduler requested
	// an Offer and it was fulfilled successfully
	OfferGet tally.Counter

	// OfferGet indicates the number of times the scheduler requested
	// an Offer and it failed
	OfferGetFail tally.Counter

	// Launcher metrics

	// LaunchTask is the number of mesos tasks launched. This is a
	// many:1 relationship with offers launched into
	LaunchTask tally.Counter

	// LaunchTaskFail is the number of mesos tasks failed to
	// launch. This is a many:1 relationship with offers
	LaunchTaskFail tally.Counter

	// LaunchOfferAccept is the number of mesos offers that were accepted
	LaunchOfferAccept tally.Counter

	// LaunchOfferAcceptFail is the number of mesos offers that failed
	// to be accepted
	LaunchOfferAcceptFail tally.Counter

	// SetPlacementSuccess counts the number of tasks we put the placement
	// in resource manager
	SetPlacementSuccess tally.Counter

	// SetPlacementFail counts the number of tasks failed to be placed
	SetPlacementFail tally.Counter

	// CreatePlacementDuration is the timer for create placement
	CreatePlacementDuration tally.Timer

	// SetPlacementDuration is the timer for set placement
	SetPlacementDuration tally.Timer

	// Host Metrics

	// HostGet indicates the number of times the scheduler requested
	// hosts and it was fulfilled successfully
	HostGet tally.Counter

	// HostGetFail indicates the number of times the scheduler requested
	// an Host and it failed
	HostGetFail tally.Counter

	// TaskAffinityFail indicates failure on host manager to return
	// host with affinity constraint satisfied.
	TaskAffinityFail tally.Counter
}

// NewMetrics returns a new Metrics struct with all metrics initialized and
// rooted below the given tally scope
func NewMetrics(scope tally.Scope) *Metrics {
	taskScope := scope.SubScope("task")
	offerScope := scope.SubScope("offer")
	hostScope := scope.SubScope("host")
	placementScope := scope.SubScope("placement")

	taskSuccessScope := taskScope.Tagged(map[string]string{"result": "success"})
	taskFailScope := taskScope.Tagged(map[string]string{"result": "fail"})

	offerSuccessScope := offerScope.Tagged(map[string]string{"result": "success"})
	offerFailScope := offerScope.Tagged(map[string]string{"result": "fail"})

	HostSuccessScope := hostScope.Tagged(map[string]string{"result": "success"})
	HostFailScope := hostScope.Tagged(map[string]string{"result": "fail"})

	placementSuccessScope := placementScope.Tagged(map[string]string{"result": "success"})
	placementFailScope := placementScope.Tagged(map[string]string{"result": "fail"})
	placementTimeScope := placementScope.Tagged(map[string]string{"type": "timer"})

	return &Metrics{
		Running:      scope.Gauge("running"),
		OfferStarved: scope.Counter("offer_starved"),

		TaskLaunchDispatches:     taskSuccessScope.Counter("launch_dispatch"),
		TaskLaunchDispatchesFail: taskFailScope.Counter("launch_dispatch"),
		TasksDequeued:            taskScope.Gauge("dequeued"),

		SetPlacementSuccess: placementSuccessScope.Counter("set"),
		SetPlacementFail:    placementFailScope.Counter("set"),

		OfferGet:     offerSuccessScope.Counter("get"),
		OfferGetFail: offerFailScope.Counter("get"),

		LaunchTask:            taskSuccessScope.Counter("launch"),
		LaunchTaskFail:        taskFailScope.Counter("launch"),
		LaunchOfferAccept:     offerSuccessScope.Counter("accept"),
		LaunchOfferAcceptFail: offerFailScope.Counter("accept"),

		CreatePlacementDuration: placementTimeScope.Timer("create_duration"),
		SetPlacementDuration:    placementTimeScope.Timer("set_duration"),

		HostGet:     HostSuccessScope.Counter("get"),
		HostGetFail: HostFailScope.Counter("get"),

		TaskAffinityFail: placementFailScope.Counter("host_limit"),
	}
}
