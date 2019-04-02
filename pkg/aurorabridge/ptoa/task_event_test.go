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

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/stretchr/testify/assert"
)

// TestNewTaskEvent checks NewTaskEvent returns Aurora TaskEvent correctly
// based on input Peloton v1 PodEvent.
func TestNewTaskEvent(t *testing.T) {
	message := "test-message"
	podEvent := &pod.PodEvent{
		Timestamp:   "2019-01-03T22:14:57Z",
		Message:     message,
		ActualState: pod.PodState_POD_STATE_RUNNING.String(),
	}

	taskEvent, err := NewTaskEvent(podEvent)
	assert.NoError(t, err)
	assert.NotNil(t, taskEvent)
	assert.Equal(t, int64(1546553697000), *taskEvent.Timestamp)
	assert.Equal(t, message, *taskEvent.Message)
	assert.Equal(t, api.ScheduleStatusRunning, *taskEvent.Status)
	assert.Equal(t, "peloton", *taskEvent.Scheduler)
}

// TestNewTaskEventErrorTimestamp checks NewTaskEvent returns error
// when invalid timestamp is passed in.
func TestNewTaskEventErrorTimestamp(t *testing.T) {
	message := "test-message"
	podEvent := &pod.PodEvent{
		Timestamp:   "blah",
		Message:     message,
		ActualState: pod.PodState_POD_STATE_RUNNING.String(),
	}

	taskEvent, err := NewTaskEvent(podEvent)
	assert.Error(t, err)
	assert.Nil(t, taskEvent)
}

// TestNewTaskEventErrorState checks NewTaskEvent returns error
// when invalid state is passed in.
func TestNewTaskEventErrorState(t *testing.T) {
	message := "test-message"
	podEvent := &pod.PodEvent{
		Timestamp:   "2019-01-03T22:14:57Z",
		Message:     message,
		ActualState: "invalid-state",
	}

	taskEvent, err := NewTaskEvent(podEvent)
	assert.Error(t, err)
	assert.Nil(t, taskEvent)
}
