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

	"github.com/uber/peloton/util"

	"go.uber.org/thriftrw/ptr"
)

// NewScheduledTask creates a ScheduledTask object.
func NewScheduledTask(
	jobInfo *stateless.JobInfo,
	podInfo *pod.PodInfo,
	podEvents []*pod.PodEvent,
) (*api.ScheduledTask, error) {
	podSpec := podInfo.GetSpec()

	auroraTaskID := podInfo.GetStatus().GetPodId().GetValue()
	auroraSlaveHost := podInfo.GetStatus().GetHost()

	taskID := podInfo.GetSpec().GetPodName().GetValue()
	_, instanceID, err := util.ParseTaskID(podSpec.GetPodName().GetValue())
	if err != nil {
		return nil, fmt.Errorf("parse task id: %s", err)
	}

	auroraTaskConfig, err := NewTaskConfig(jobInfo, podSpec)
	if err != nil {
		return nil, fmt.Errorf("new task config: %s", err)
	}

	runID, err := util.ParseRunID(auroraTaskID)
	if err != nil {
		return nil, fmt.Errorf("parse task id: %s", err)
	}

	var ancestorID *string
	if runID > 1 {
		ancestorID = ptr.String(fmt.Sprintf("%s-%d", taskID, runID-1))
	}

	auroraStatus, err := NewScheduleStatus(podInfo.GetStatus().GetState())
	if err != nil {
		return nil, fmt.Errorf("new schedule status: %s", err)
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

	auroraAssignedPorts := make(map[string]int32)
	c := podInfo.GetSpec().GetContainers()
	if len(c) == 0 {
		return nil, fmt.Errorf("pod spec does not have any containers")
	}
	for _, p := range c[0].GetPorts() {
		auroraAssignedPorts[p.GetName()] = int32(p.GetValue())
	}

	return &api.ScheduledTask{
		AssignedTask: &api.AssignedTask{
			TaskId:        &auroraTaskID,
			SlaveHost:     &auroraSlaveHost,
			Task:          auroraTaskConfig,
			AssignedPorts: auroraAssignedPorts,
			InstanceId:    ptr.Int32(int32(instanceID)),
			//SlaveId:   nil,
		},
		Status:     auroraStatus,
		TaskEvents: auroraTaskEvents,
		AncestorId: ancestorID,
		//FailureCount: nil,
	}, nil
}
