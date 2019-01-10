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

	"github.com/uber/peloton/aurorabridge/label"
	"github.com/uber/peloton/util"

	"go.uber.org/thriftrw/ptr"
)

// NewScheduledTask creates a ScheduledTask object.
func NewScheduledTask(
	jobInfo *stateless.JobInfo,
	podInfo *pod.PodInfo,
	podEvents []*pod.PodEvent,
) (*api.ScheduledTask, error) {
	auroraTaskID := podInfo.GetStatus().GetPodId().GetValue()
	auroraSlaveHost := podInfo.GetStatus().GetHost()
	auroraTier := NewTaskTier(jobInfo.GetSpec().GetSla())

	auroraJobKey, err := NewJobKey(jobInfo.GetSpec().GetName())
	if err != nil {
		return nil, err
	}

	_, instanceID, err := util.ParseTaskID(podInfo.GetSpec().GetPodName().GetValue())
	if err != nil {
		return nil, fmt.Errorf("parse task id: %s", err)
	}

	auroraStatus, err := NewScheduleStatus(podInfo.GetStatus().GetState())
	if err != nil {
		return nil, fmt.Errorf("new schedule status: %s", err)
	}

	// TODO(kevinxu): make metadata optional?
	auroraMetadata, err := label.ParseAuroraMetadata(podInfo.GetSpec().GetLabels())
	if err != nil {
		return nil, fmt.Errorf("parse aurora metadata: %s", err)
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

	return &api.ScheduledTask{
		AssignedTask: &api.AssignedTask{
			TaskId: &auroraTaskID,
			//SlaveId:   nil,
			SlaveHost: &auroraSlaveHost,
			Task: &api.TaskConfig{
				Job: auroraJobKey,
				//Owner:            nil,
				//IsService:        nil,
				//NumCpus:          nil,
				//RamMb:            nil,
				//DiskMb:           nil,
				//Priority:         nil,
				//MaxTaskFailures:  nil,
				Tier: auroraTier,
				//Resources:        nil,
				//Constraints:      nil,
				//RequestedPorts:   map[string]struct{}{},
				//MesosFetcherUris: nil,
				//TaskLinks:        map[string]string{},
				//ContactEmail:     nil,
				//ExecutorConfig: nil,
				Metadata: auroraMetadata,
				//Container:      nil,
			},
			AssignedPorts: map[string]int32{}, // TODO(kevinxu): how to get assignedPorts?
			InstanceId:    ptr.Int32(int32(instanceID)),
		},
		Status: auroraStatus,
		//FailureCount: nil,
		TaskEvents: auroraTaskEvents,
		AncestorId: nil, // TODO(kevinxu): how to get ancestor id?
	}, nil
}
