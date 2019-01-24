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
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/aurorabridge/atop"
	"github.com/uber/peloton/aurorabridge/fixture"
	"github.com/uber/peloton/aurorabridge/label"

	"github.com/stretchr/testify/assert"
	"go.uber.org/thriftrw/ptr"
)

func TestNewScheduledTask(t *testing.T) {
	jobKey := fixture.AuroraJobKey()
	jobID := fixture.PelotonJobID()
	metadata := fixture.AuroraMetadata()
	host := "peloton-host-0"

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
	ml, err := label.NewAuroraMetadata(metadata)
	assert.NoError(t, err)
	kl := label.NewAuroraJobKey(jobKey)

	// pod 1
	j := &stateless.JobInfo{
		Spec: &stateless.JobSpec{
			Name: atop.NewJobName(jobKey),
		},
	}
	p1 := &pod.PodInfo{
		Spec: &pod.PodSpec{
			PodName: podName1,
			Labels:  []*peloton.Label{ml, kl},
			Containers: []*pod.ContainerSpec{
				&pod.ContainerSpec{},
			},
		},
		Status: &pod.PodStatus{
			PodId: podID1,
			Host:  host,
			State: pod.PodState_POD_STATE_RUNNING,
		},
	}
	pe1 := []*pod.PodEvent{
		{
			Timestamp:   "2019-01-03T22:14:58Z",
			Message:     "",
			ActualState: task.TaskState_RUNNING.String(),
		},
		{
			Timestamp:   "2019-01-03T22:14:58Z",
			Message:     "",
			ActualState: task.TaskState_STARTING.String(),
		},
		{
			Timestamp:   "2019-01-03T22:14:58Z",
			Message:     "Add hostname and ports",
			ActualState: task.TaskState_LAUNCHED.String(),
		},
		{
			Timestamp:   "2019-01-03T22:14:57Z",
			Message:     "Task sent for placement",
			ActualState: task.TaskState_PENDING.String(),
		},
		{
			Timestamp:   "2019-01-03T22:14:57Z",
			Message:     "",
			ActualState: task.TaskState_INITIALIZED.String(),
		},
	}

	s, err := NewScheduledTask(j, p1, pe1)
	assert.NoError(t, err)
	assert.Equal(t, &api.ScheduledTask{
		AssignedTask: &api.AssignedTask{
			TaskId:    ptr.String(podID1.GetValue()),
			SlaveHost: ptr.String(host),
			Task: &api.TaskConfig{
				Job:       jobKey,
				IsService: ptr.Bool(true),
				Tier:      ptr.String("preferred"),
				Metadata:  metadata,
			},
			AssignedPorts: map[string]int32{},
			InstanceId:    ptr.Int32(0),
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
	p2 := &pod.PodInfo{
		Spec: &pod.PodSpec{
			PodName: podName2,
			Labels:  []*peloton.Label{ml, kl},
			Containers: []*pod.ContainerSpec{
				&pod.ContainerSpec{
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
				},
			},
		},
		Status: &pod.PodStatus{
			PodId: podID2,
			Host:  host,
			State: pod.PodState_POD_STATE_KILLED,
		},
	}
	pe2 := []*pod.PodEvent{
		{
			Timestamp:   "2019-01-03T22:15:08Z",
			Message:     "Command terminated with signal Terminated",
			ActualState: task.TaskState_KILLED.String(),
		},
		{
			Timestamp:   "2019-01-03T22:15:08Z",
			Message:     "Killing the task",
			ActualState: task.TaskState_KILLING.String(),
		},
		{
			Timestamp:   "2019-01-03T22:15:08Z",
			Message:     "Task stop API request",
			ActualState: task.TaskState_RUNNING.String(),
		},
		{
			Timestamp:   "2019-01-03T22:14:58Z",
			Message:     "",
			ActualState: task.TaskState_RUNNING.String(),
		},
		{
			Timestamp:   "2019-01-03T22:14:58Z",
			Message:     "",
			ActualState: task.TaskState_STARTING.String(),
		},
		{
			Timestamp:   "2019-01-03T22:14:58Z",
			Message:     "Add hostname and ports",
			ActualState: task.TaskState_LAUNCHED.String(),
		},
		{
			Timestamp:   "2019-01-03T22:14:57Z",
			Message:     "Task sent for placement",
			ActualState: task.TaskState_PENDING.String(),
		},
		{
			Timestamp:   "2019-01-03T22:14:57Z",
			Message:     "",
			ActualState: task.TaskState_INITIALIZED.String(),
		},
	}

	s, err = NewScheduledTask(j, p2, pe2)
	assert.NoError(t, err)
	assert.Equal(t, &api.ScheduledTask{
		AssignedTask: &api.AssignedTask{
			TaskId:    ptr.String(podID2.GetValue()),
			SlaveHost: ptr.String(host),
			Task: &api.TaskConfig{
				Job:       jobKey,
				IsService: ptr.Bool(true),
				Tier:      ptr.String("preferred"),
				Metadata:  metadata,
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
