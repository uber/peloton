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
	"sort"
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/aurorabridge/atop"
	"github.com/uber/peloton/pkg/aurorabridge/common"
	"github.com/uber/peloton/pkg/aurorabridge/fixture"
	"github.com/uber/peloton/pkg/aurorabridge/label"
	"github.com/uber/peloton/pkg/common/thermos"

	"github.com/stretchr/testify/assert"
	"go.uber.org/thriftrw/ptr"
)

func TestNewScheduledTask(t *testing.T) {
	jobKey := fixture.AuroraJobKey()
	jobID := fixture.PelotonJobID()
	metadata := fixture.AuroraMetadata()
	host := "peloton-host-0"
	hostID := "6a2fe3f4-504c-48e9-b04f-9db7c02aa484-S0"

	sort.Stable(thermos.MetadataByKey(metadata))

	podName1, podID1 := &peloton.PodName{
		Value: jobID.GetValue() + "-0",
	}, &peloton.PodID{
		Value: jobID.GetValue() + "-0-1",
	}
	podName2, podID2 := &peloton.PodName{
		Value: jobID.GetValue() + "-1",
	}, &peloton.PodID{
		Value: jobID.GetValue() + "-1-2",
	}
	ancestorID2 := ptr.String(jobID.GetValue() + "-1-1")
	ml := label.NewAuroraMetadataLabels(metadata)
	kl := label.NewAuroraJobKey(jobKey)

	// pod 1
	j := &stateless.JobSummary{
		Name: atop.NewJobName(jobKey),
	}
	p1 := &pod.PodSpec{
		PodName: podName1,
		Labels:  append([]*peloton.Label{kl}, ml...),
		Containers: []*pod.ContainerSpec{
			{},
		},
	}
	pe1 := []*pod.PodEvent{
		{
			PodId:       podID1,
			Timestamp:   "2019-01-03T22:14:58Z",
			Message:     "",
			ActualState: pod.PodState_POD_STATE_RUNNING.String(),
			PrevPodId:   nil,
			Hostname:    host,
			AgentId:     hostID,
		},
		{
			PodId:       podID1,
			Timestamp:   "2019-01-03T22:14:58Z",
			Message:     "",
			ActualState: pod.PodState_POD_STATE_STARTING.String(),
			PrevPodId:   nil,
			Hostname:    host,
			AgentId:     hostID,
		},
		{
			PodId:       podID1,
			Timestamp:   "2019-01-03T22:14:58Z",
			Message:     "Add hostname and ports",
			ActualState: pod.PodState_POD_STATE_LAUNCHED.String(),
			PrevPodId:   nil,
			Hostname:    host,
			AgentId:     hostID,
		},
		{
			PodId:       podID1,
			Timestamp:   "2019-01-03T22:14:57Z",
			Message:     "Task sent for placement",
			ActualState: pod.PodState_POD_STATE_PENDING.String(),
			PrevPodId:   nil,
		},
		{
			PodId:       podID1,
			Timestamp:   "2019-01-03T22:14:57Z",
			Message:     "",
			ActualState: pod.PodState_POD_STATE_INITIALIZED.String(),
			PrevPodId:   nil,
		},
	}

	s, err := NewScheduledTask(j, p1, pe1)
	assert.NoError(t, err)
	assert.Equal(t, &api.ScheduledTask{
		AssignedTask: &api.AssignedTask{
			TaskId:    ptr.String(podID1.GetValue()),
			SlaveHost: ptr.String(host),
			SlaveId:   ptr.String(hostID),
			Task: &api.TaskConfig{
				Job:       jobKey,
				IsService: ptr.Bool(true),
				Tier:      ptr.String(common.Preemptible),
				Metadata:  metadata,
			},
			InstanceId: ptr.Int32(0),
		},
		Status: api.ScheduleStatusRunning.Ptr(),
		TaskEvents: []*api.TaskEvent{
			{
				Timestamp: ptr.Int64(1546553697000),
				Status:    api.ScheduleStatusInit.Ptr(),
				Message:   ptr.String(""),
				Scheduler: ptr.String("peloton"),
			},
			{
				Timestamp: ptr.Int64(1546553697000),
				Status:    api.ScheduleStatusPending.Ptr(),
				Message:   ptr.String("Task sent for placement"),
				Scheduler: ptr.String("peloton"),
			},
			{
				Timestamp: ptr.Int64(1546553698000),
				Status:    api.ScheduleStatusAssigned.Ptr(),
				Message:   ptr.String("Add hostname and ports"),
				Scheduler: ptr.String("peloton"),
			},
			{
				Timestamp: ptr.Int64(1546553698000),
				Status:    api.ScheduleStatusStarting.Ptr(),
				Message:   ptr.String(""),
				Scheduler: ptr.String("peloton"),
			},
			{
				Timestamp: ptr.Int64(1546553698000),
				Status:    api.ScheduleStatusRunning.Ptr(),
				Message:   ptr.String(""),
				Scheduler: ptr.String("peloton"),
			},
		},
		AncestorId: nil,
	}, s)

	// pod 2
	p2 := &pod.PodSpec{
		PodName: podName2,
		Labels:  append([]*peloton.Label{kl}, ml...),
		Containers: []*pod.ContainerSpec{
			{
				Ports: []*pod.PortSpec{
					{
						Name:  "http",
						Value: 12345,
					},
					{
						Name:  "tchannel",
						Value: 54321,
					},
				},
				Resource: &pod.ResourceSpec{
					CpuLimit:    1.5,
					MemLimitMb:  1024,
					DiskLimitMb: 128,
					GpuLimit:    2.0,
				},
			},
		},
	}
	pe2 := []*pod.PodEvent{
		{
			PodId:       podID2,
			Timestamp:   "2019-01-03T22:15:08Z",
			Message:     "Command terminated with signal Terminated",
			ActualState: pod.PodState_POD_STATE_KILLED.String(),
			PrevPodId:   &peloton.PodID{Value: *ancestorID2},
			Hostname:    host,
			AgentId:     hostID,
		},
		{
			PodId:       podID2,
			Timestamp:   "2019-01-03T22:15:08Z",
			Message:     "Killing the task",
			ActualState: pod.PodState_POD_STATE_KILLING.String(),
			PrevPodId:   &peloton.PodID{Value: *ancestorID2},
			Hostname:    host,
			AgentId:     hostID,
		},
		{
			PodId:       podID2,
			Timestamp:   "2019-01-03T22:15:08Z",
			Message:     "Task stop API request",
			ActualState: pod.PodState_POD_STATE_RUNNING.String(),
			PrevPodId:   &peloton.PodID{Value: *ancestorID2},
			Hostname:    host,
			AgentId:     hostID,
		},
		{
			PodId:       podID2,
			Timestamp:   "2019-01-03T22:14:58Z",
			Message:     "",
			ActualState: pod.PodState_POD_STATE_RUNNING.String(),
			PrevPodId:   &peloton.PodID{Value: *ancestorID2},
			Hostname:    host,
			AgentId:     hostID,
		},
		{
			PodId:       podID2,
			Timestamp:   "2019-01-03T22:14:58Z",
			Message:     "",
			ActualState: pod.PodState_POD_STATE_STARTING.String(),
			PrevPodId:   &peloton.PodID{Value: *ancestorID2},
			Hostname:    host,
			AgentId:     hostID,
		},
		{
			PodId:       podID2,
			Timestamp:   "2019-01-03T22:14:58Z",
			Message:     "Add hostname and ports",
			ActualState: pod.PodState_POD_STATE_LAUNCHED.String(),
			PrevPodId:   &peloton.PodID{Value: *ancestorID2},
			Hostname:    host,
			AgentId:     hostID,
		},
		{
			PodId:       podID2,
			Timestamp:   "2019-01-03T22:14:57Z",
			Message:     "Task sent for placement",
			ActualState: pod.PodState_POD_STATE_PENDING.String(),
			PrevPodId:   &peloton.PodID{Value: *ancestorID2},
		},
		{
			PodId:       podID2,
			Timestamp:   "2019-01-03T22:14:57Z",
			Message:     "",
			ActualState: pod.PodState_POD_STATE_INITIALIZED.String(),
			PrevPodId:   &peloton.PodID{Value: *ancestorID2},
		},
	}

	s, err = NewScheduledTask(j, p2, pe2)
	assert.NoError(t, err)
	assert.Equal(t, &api.ScheduledTask{
		AssignedTask: &api.AssignedTask{
			TaskId:    ptr.String(podID2.GetValue()),
			SlaveHost: ptr.String(host),
			SlaveId:   ptr.String(hostID),
			Task: &api.TaskConfig{
				Job:     jobKey,
				NumCpus: ptr.Float64(1.5),
				RamMb:   ptr.Int64(1024),
				DiskMb:  ptr.Int64(128),
				RequestedPorts: map[string]struct{}{
					"http":     {},
					"tchannel": {},
				},
				IsService: ptr.Bool(true),
				Tier:      ptr.String(common.Preemptible),
				Metadata:  metadata,
				Resources: []*api.Resource{
					{NumCpus: ptr.Float64(1.5)},
					{RamMb: ptr.Int64(1024)},
					{DiskMb: ptr.Int64(128)},
					{NumGpus: ptr.Int64(2)},
					{NamedPort: ptr.String("http")},
					{NamedPort: ptr.String("tchannel")},
				},
			},
			AssignedPorts: map[string]int32{
				"http":     12345,
				"tchannel": 54321,
			},
			InstanceId: ptr.Int32(1),
		},
		Status: api.ScheduleStatusKilled.Ptr(),
		TaskEvents: []*api.TaskEvent{
			{
				Timestamp: ptr.Int64(1546553697000),
				Status:    api.ScheduleStatusInit.Ptr(),
				Message:   ptr.String(""),
				Scheduler: ptr.String("peloton"),
			},
			{
				Timestamp: ptr.Int64(1546553697000),
				Status:    api.ScheduleStatusPending.Ptr(),
				Message:   ptr.String("Task sent for placement"),
				Scheduler: ptr.String("peloton"),
			},
			{
				Timestamp: ptr.Int64(1546553698000),
				Status:    api.ScheduleStatusAssigned.Ptr(),
				Message:   ptr.String("Add hostname and ports"),
				Scheduler: ptr.String("peloton"),
			},
			{
				Timestamp: ptr.Int64(1546553698000),
				Status:    api.ScheduleStatusStarting.Ptr(),
				Message:   ptr.String(""),
				Scheduler: ptr.String("peloton"),
			},
			{
				Timestamp: ptr.Int64(1546553698000),
				Status:    api.ScheduleStatusRunning.Ptr(),
				Message:   ptr.String(""),
				Scheduler: ptr.String("peloton"),
			},
			{
				Timestamp: ptr.Int64(1546553708000),
				Status:    api.ScheduleStatusRunning.Ptr(),
				Message:   ptr.String("Task stop API request"),
				Scheduler: ptr.String("peloton"),
			},
			{
				Timestamp: ptr.Int64(1546553708000),
				Status:    api.ScheduleStatusKilling.Ptr(),
				Message:   ptr.String("Killing the task"),
				Scheduler: ptr.String("peloton"),
			},
			{
				Timestamp: ptr.Int64(1546553708000),
				Status:    api.ScheduleStatusKilled.Ptr(),
				Message:   ptr.String("Command terminated with signal Terminated"),
				Scheduler: ptr.String("peloton"),
			},
		},
		AncestorId: ancestorID2,
	}, s)
}
