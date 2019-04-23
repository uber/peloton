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

package common

import (
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
)

// Aurora scheduling tiers.
const (
	Preemptible = "preemptible"
	Revocable   = "revocable"
	Preferred   = "preferred"
)

// MesosHostAttr is the Mesos attribute for hostname.
const MesosHostAttr = "host"

// BridgeJobLabel is the common Peloton job level label for all jobs
// created by aurora bridge.
var BridgeJobLabel = &peloton.Label{
	Key:   "aurora_bridge",
	Value: "com.uber.peloton.internal.aurorabridge",
}

// BridgePodLabel is the common Peloton pod level label for all jobs
// created by aurora bridge.
var BridgePodLabel = &peloton.Label{
	Key:   "aurora_bridge_pod",
	Value: "com.uber.peloton.internal.aurorabridge_pod",
}

// BridgeUpdateLabelKey is the Peloton pod level label key for triggering
// a forced PodSpec change.
const BridgeUpdateLabelKey = "aurora_bridge_update"

// QueryJobsLimit specifies Limit parameter passed to QueryJobs request
const QueryJobsLimit = 1000

// InstanceEventsLimit specifies the limit on number of events per instance
const InstanceEventsLimit = 100

// UpdatesLimit specifies the limit on number of updates to include per job
const UpdatesLimit = 10
