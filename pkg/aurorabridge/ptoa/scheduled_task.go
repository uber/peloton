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

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/common/util"

	"go.uber.org/thriftrw/ptr"
)

// NewScheduledTask creates a ScheduledTask object.
func NewScheduledTask(
	jobSummary *stateless.JobSummary,
	podSpec *pod.PodSpec,
	podEvents []*pod.PodEvent,
) (*api.ScheduledTask, error) {

	if jobSummary == nil {
		return nil, fmt.Errorf("job summary is nil")
	}

	if podSpec == nil {
		return nil, fmt.Errorf("pod spec is nil")
	}

	if len(podEvents) == 0 {
		return nil, fmt.Errorf("pod events is empty")
	}

	return newScheduledTask(jobSummary, podSpec, podEvents)
}

func newScheduledTask(
	jobSummary *stateless.JobSummary,
	podSpec *pod.PodSpec,
	podEvents []*pod.PodEvent,
) (*api.ScheduledTask, error) {
	auroraTaskID := podEvents[0].GetPodId().GetValue()
	auroraSlaveHost := podEvents[0].GetHostname()

	pelotonTaskID, err := util.ParseTaskIDFromMesosTaskID(auroraTaskID)
	if err != nil {
		return nil, fmt.Errorf("parse task id from mesos task id: %s", err)
	}
	_, instanceID, err := util.ParseTaskID(pelotonTaskID)
	if err != nil {
		return nil, fmt.Errorf("parse task id: %s", err)
	}

	ancestorID, err := newAncestorID(podEvents)
	if err != nil {
		return nil, fmt.Errorf("new ancestor id: %s", err)
	}

	auroraStatus, err := convertPodStateStringToScheduleStatus(
		podEvents[0].GetActualState())
	if err != nil {
		return nil, err
	}

	auroraTaskEvents := make([]*api.TaskEvent, 0, len(podEvents))
	// append oldest event first to make it consistent with Aurora's behavior
	for i := len(podEvents) - 1; i >= 0; i-- {
		p := podEvents[i]
		e, err := NewTaskEvent(p)
		if err != nil {
			return nil, fmt.Errorf("new task event: %s", err)
		}
		auroraTaskEvents = append(auroraTaskEvents, e)
	}

	var auroraSlaveID *string
	if agentID := podEvents[0].GetAgentId(); agentID != "" {
		auroraSlaveID = ptr.String(agentID)
	}

	auroraTaskConfig, err := NewTaskConfig(jobSummary, podSpec)
	if err != nil {
		return nil, fmt.Errorf("new task config: %s", err)
	}

	var auroraAssignedPorts map[string]int32
	c := podSpec.GetContainers()
	if len(c) == 0 {
		return nil, fmt.Errorf("pod spec does not have any containers")
	}
	for _, p := range c[0].GetPorts() {
		if auroraAssignedPorts == nil {
			auroraAssignedPorts = make(map[string]int32)
		}
		auroraAssignedPorts[p.GetName()] = int32(p.GetValue())
	}

	return &api.ScheduledTask{
		AssignedTask: &api.AssignedTask{
			TaskId:        &auroraTaskID,
			SlaveHost:     &auroraSlaveHost,
			InstanceId:    ptr.Int32(int32(instanceID)),
			Task:          auroraTaskConfig,
			AssignedPorts: auroraAssignedPorts,
			SlaveId:       auroraSlaveID,
		},
		Status:       auroraStatus,
		TaskEvents:   auroraTaskEvents,
		AncestorId:   ancestorID,
		FailureCount: nil, // TODO(kxu): to be filled
	}, nil
}

// newAncestorID extracts previous pod id from pod events.
func newAncestorID(podEvents []*pod.PodEvent) (*string, error) {
	if len(podEvents) == 0 {
		return nil, fmt.Errorf("empty pod events")
	}

	ppid := podEvents[0].GetPrevPodId().GetValue()
	if ppid == "" {
		return nil, nil
	}

	runID, err := util.ParseRunID(ppid)
	if err != nil {
		return nil, err
	}

	if runID == 0 {
		// in pod events, it may contain run id of 0, we need to
		// manually filter it out
		return nil, nil
	}

	return &ppid, nil
}
