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

	"github.com/uber/peloton/pkg/aurorabridge/atop"
	"github.com/uber/peloton/pkg/aurorabridge/fixture"
	"github.com/uber/peloton/pkg/aurorabridge/label"

	"github.com/stretchr/testify/assert"
)

func TestNewJobSummary(t *testing.T) {
	jobKey := fixture.AuroraJobKey()
	metadata := fixture.AuroraMetadata()
	instanceCount := uint32(12)

	ml, err := label.NewAuroraMetadata(metadata)
	assert.NoError(t, err)

	j := &stateless.JobInfo{
		Spec: &stateless.JobSpec{
			Name:          atop.NewJobName(jobKey),
			InstanceCount: instanceCount,
		},
	}
	p := &pod.PodSpec{
		Labels:     []*peloton.Label{ml},
		Containers: []*pod.ContainerSpec{{}},
	}

	c, err := NewJobSummary(j, p)
	assert.NoError(t, err)
	assert.NotNil(t, c.GetStats())
	assert.NotNil(t, c.GetJob())
}

func TestNewJobConfiguration(t *testing.T) {
	jobKey := fixture.AuroraJobKey()
	metadata := fixture.AuroraMetadata()
	pendingInstances := uint32(2)
	startingInstances := uint32(4)
	runningInstances := uint32(8)
	failedInstances := uint32(4)
	instanceCount := pendingInstances +
		startingInstances +
		runningInstances +
		failedInstances

	testCases := []struct {
		name          string
		activeOnly    bool
		instanceCount uint32
	}{
		{
			"active only",
			true,
			startingInstances + runningInstances,
		},
		{
			"all task states",
			false,
			instanceCount,
		},
	}

	ml, err := label.NewAuroraMetadata(metadata)
	assert.NoError(t, err)

	j := &stateless.JobInfo{
		Spec: &stateless.JobSpec{
			Name:          atop.NewJobName(jobKey),
			InstanceCount: instanceCount,
			Owner:         "owner",
		},
		Status: &stateless.JobStatus{
			PodStats: map[string]uint32{
				task.TaskState_PENDING.String():  pendingInstances,
				task.TaskState_STARTING.String(): startingInstances,
				task.TaskState_RUNNING.String():  runningInstances,
				task.TaskState_FAILED.String():   failedInstances,
			},
		},
	}
	p := &pod.PodSpec{
		Labels:     []*peloton.Label{ml},
		Containers: []*pod.ContainerSpec{{}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewJobConfiguration(j, p, tc.activeOnly)
			assert.NoError(t, err)
			assert.Equal(t, jobKey, c.GetKey())
			assert.Equal(t, "owner", c.GetOwner().GetUser())
			assert.Equal(t, tc.instanceCount, uint32(c.GetInstanceCount()))
			assert.NotNil(t, c.GetTaskConfig())
		})
	}
}

func TestNewJobStats(t *testing.T) {
	jobStatus := &stateless.JobStatus{
		PodStats: map[string]uint32{
			task.TaskState_INITIALIZED.String(): 1,
			task.TaskState_PENDING.String():     2,

			task.TaskState_READY.String():      3,
			task.TaskState_PLACING.String():    4,
			task.TaskState_PLACED.String():     5,
			task.TaskState_LAUNCHING.String():  6,
			task.TaskState_LAUNCHED.String():   7,
			task.TaskState_STARTING.String():   8,
			task.TaskState_RUNNING.String():    9,
			task.TaskState_KILLING.String():    10,
			task.TaskState_PREEMPTING.String(): 11,

			task.TaskState_SUCCEEDED.String(): 12,
			task.TaskState_KILLED.String():    13,
			task.TaskState_DELETED.String():   14,

			task.TaskState_FAILED.String(): 15,
			task.TaskState_LOST.String():   16,
		},
	}

	jobStats := newJobStats(jobStatus)
	assert.Equal(t, int32(3), jobStats.GetPendingTaskCount())
	assert.Equal(t, int32(63), jobStats.GetActiveTaskCount())
	assert.Equal(t, int32(39), jobStats.GetFinishedTaskCount())
	assert.Equal(t, int32(31), jobStats.GetFailedTaskCount())
}
