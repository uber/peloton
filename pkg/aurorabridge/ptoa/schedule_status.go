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

package ptoa

import (
	"fmt"
)

import (
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/thrift/aurora/api"
)

// NewScheduleStatus converts Peloton v1 PodState enum
// to Aurora ScheduleStatus enum.
func NewScheduleStatus(s pod.PodState) (*api.ScheduleStatus, error) {
	switch s {
	case pod.PodState_POD_STATE_INVALID:
		// Invalid state.
		return api.ScheduleStatusInit.Ptr(), nil
	case pod.PodState_POD_STATE_INITIALIZED:
		// The pod is being initialized
		return api.ScheduleStatusInit.Ptr(), nil
	case pod.PodState_POD_STATE_PENDING:
		// The pod is pending and waiting for resources
		return api.ScheduleStatusPending.Ptr(), nil
	case pod.PodState_POD_STATE_READY:
		// The pod has been allocated with resources and ready for placement
		return api.ScheduleStatusPending.Ptr(), nil
	case pod.PodState_POD_STATE_PLACING:
		// The pod is being placed to a host based on its resource
		// requirements and constraints
		return api.ScheduleStatusPending.Ptr(), nil
	case pod.PodState_POD_STATE_PLACED:
		// The pod has been assigned to a host matching the resource
		// requirements and constraints
		return api.ScheduleStatusAssigned.Ptr(), nil
	case pod.PodState_POD_STATE_LAUNCHING:
		// The pod is taken from resmgr to be launched
		return api.ScheduleStatusAssigned.Ptr(), nil
	case pod.PodState_POD_STATE_LAUNCHED:
		// The pod is being launched in Job manager
		return api.ScheduleStatusAssigned.Ptr(), nil
	case pod.PodState_POD_STATE_STARTING:
		// Either init containers are starting/running or the main containers
		// in the pod are being started by Mesos agent
		return api.ScheduleStatusStarting.Ptr(), nil
	case pod.PodState_POD_STATE_RUNNING:
		// All containers in the pod are running
		return api.ScheduleStatusRunning.Ptr(), nil
	case pod.PodState_POD_STATE_SUCCEEDED:
		// All containers in the pod terminated with an exit code of zero
		return api.ScheduleStatusFinished.Ptr(), nil
	case pod.PodState_POD_STATE_FAILED:
		// At least on container in the pod terminated with a non-zero exit code
		return api.ScheduleStatusFailed.Ptr(), nil
	case pod.PodState_POD_STATE_LOST:
		// The pod is lost
		return api.ScheduleStatusLost.Ptr(), nil
	case pod.PodState_POD_STATE_KILLING:
		// The pod is being killed
		return api.ScheduleStatusKilling.Ptr(), nil
	case pod.PodState_POD_STATE_KILLED:
		// At least one of the containers in the pod was terminated by the system
		return api.ScheduleStatusKilled.Ptr(), nil
	case pod.PodState_POD_STATE_PREEMPTING:
		// The pod is being preempted by another one on the node
		return api.ScheduleStatusPreempting.Ptr(), nil
	case pod.PodState_POD_STATE_DELETED:
		// The pod is to be deleted after termination
		return api.ScheduleStatusKilled.Ptr(), nil
	default:
		return nil, fmt.Errorf("unknown pod state: %d", s)
	}
}

// convertPodStateStringToScheduleStatus converts Peloton v1 PodState enum
// string to Aurora ScheduleStatus enum, used by conversion from Peloton
// PodEvent to Aurora TaskEvent.
func convertPodStateStringToScheduleStatus(s string) (*api.ScheduleStatus, error) {
	v, ok := pod.PodState_value[s]
	if !ok {
		return nil, fmt.Errorf("unknown task state: %q", s)
	}
	p := pod.PodState(v)
	return NewScheduleStatus(p)
}
