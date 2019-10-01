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

package mesos

import "github.com/uber-go/tally"

type metrics struct {
	scope tally.Scope

	KillPod     tally.Counter
	KillPodFail tally.Counter

	LaunchPod     tally.Counter
	LaunchPodFail tally.Counter

	DeclineOffers     tally.Counter
	DeclineOffersFail tally.Counter

	// Task Status update metrics.
	TaskUpdateCounter   tally.Counter
	TaskUpdateAck       tally.Counter
	TaskUpdateAckDeDupe tally.Counter

	AgentIDToHostnameMissing tally.Counter
}

func newMetrics(scope tally.Scope) *metrics {
	successScope := scope.Tagged(map[string]string{"result": "success"})
	failScope := scope.Tagged(map[string]string{"result": "fail"})

	return &metrics{
		scope: scope,

		KillPod:                  successScope.Counter("kill_pod"),
		KillPodFail:              failScope.Counter("kill_pod"),
		LaunchPod:                successScope.Counter("launch_pod"),
		LaunchPodFail:            failScope.Counter("launch_pod"),
		TaskUpdateAck:            successScope.Counter("task_update_ack"),
		TaskUpdateAckDeDupe:      successScope.Counter("task_update_ack_dedupe"),
		DeclineOffers:            successScope.Counter("decline_offers"),
		DeclineOffersFail:        failScope.Counter("decline_offers"),
		TaskUpdateCounter:        scope.Counter("task_update"),
		AgentIDToHostnameMissing: scope.Counter("agent_id_to_hostname_missing"),
	}
}
