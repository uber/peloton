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

package statusupdate

import (
	"fmt"
	"testing"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	pbhost "github.com/uber/peloton/.gen/peloton/api/v0/host"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	pbeventstream "github.com/uber/peloton/.gen/peloton/private/eventstream"
	v1pbevent "github.com/uber/peloton/.gen/peloton/private/eventstream/v1alpha/event"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func newV0EventPb(
	mesosTaskID string,
	agentID string,
	offset uint64,
	state mesos.TaskState,
	ts time.Time,
	reason mesos.TaskStatus_Reason,
	healthy bool,
	failure string,
) *pbeventstream.Event {
	sec := float64(ts.UnixNano()) / 1e9
	status := &mesos.TaskStatus{
		TaskId: &mesos.TaskID{
			Value: &mesosTaskID,
		},
		State:     &state,
		Healthy:   &healthy,
		Timestamp: &sec,
		AgentId:   &mesos.AgentID{Value: &agentID},
		Reason:    &reason,
		Message:   &failure,
	}
	return &pbeventstream.Event{
		Offset:          offset,
		MesosTaskStatus: status,
		Type:            pbeventstream.Event_MESOS_TASK_STATUS,
	}
}

func TestV0Event(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	taskID := fmt.Sprintf("%s-0", uuid.NewUUID())
	mesosTaskID := taskID + "-" + uuid.NewUUID().String()
	agentID := "testagentid"
	offset := uint64(1)
	healthy := true
	msg := "test failure"
	now := time.Now()
	event, err := NewV0(newV0EventPb(
		mesosTaskID,
		agentID,
		offset,
		mesos.TaskState_TASK_RUNNING,
		now,
		mesos.TaskStatus_REASON_COMMAND_EXECUTOR_FAILED,
		healthy,
		msg))
	assert.NoError(t, err)
	assert.Equal(t, taskID, event.TaskID())
	assert.Equal(t, pbtask.TaskState_RUNNING, event.State())
	assert.Equal(t, msg, event.StatusMsg())
	assert.Equal(t, mesosTaskID, event.MesosTaskID().GetValue())
	assert.Equal(t, agentID, event.AgentID().GetValue())
	assert.NotNil(t, event.MesosTaskStatus())
	assert.Equal(t, offset, event.Offset())
	assert.Equal(t, mesos.TaskStatus_REASON_COMMAND_EXECUTOR_FAILED.String(), event.Reason())
	assert.Equal(t, healthy, event.Healthy())
	assert.Equal(t, msg, event.Message())
	assert.Equal(t, now.UnixNano()/1e9, int64(*event.Timestamp()))
}

func makeHostEvent(hostname string) *pbeventstream.Event {
	ev := &pbeventstream.Event{
		Type:      pbeventstream.Event_HOST_EVENT,
		HostEvent: &pbhost.HostEvent{Hostname: hostname},
	}
	return ev
}

// TestV0EventHostEvent tests NewV0 for HostEvent
func TestV0EventHostEvent(t *testing.T) {
	event, err := NewV0(makeHostEvent("h1"))
	assert.Nil(t, event)
	assert.Error(t, err)
}

func newV1EventPb(
	podID string,
	offset uint64,
	state string,
	ts time.Time,
	hostname string,
	msg string,
	reason string,
	healthy bool,
) *v1pbevent.Event {
	var hs string
	if healthy {
		hs = pbpod.HealthState_HEALTH_STATE_HEALTHY.String()
	}
	return &v1pbevent.Event{
		Offset: offset,
		PodEvent: &pbpod.PodEvent{
			PodId:       &peloton.PodID{Value: podID},
			ActualState: state,
			Timestamp:   ts.Format(time.RFC3339),
			Hostname:    hostname,
			Message:     msg,
			Reason:      reason,
			Healthy:     hs,
		},
	}
}

func TestV1Event(t *testing.T) {
	offset := uint64(2)
	internalTaskID := uuid.NewUUID().String() + "-1"
	podID := fmt.Sprintf("%s-1", internalTaskID)

	state := pbpod.PodState_POD_STATE_RUNNING.String()
	now := time.Now()
	hostname := "localhost"
	msg := "runningmsg"
	reason := "ok"
	healthy := true
	event, err := NewV1(newV1EventPb(
		podID,
		offset,
		state,
		now,
		hostname,
		msg,
		reason,
		healthy,
	))
	assert.NoError(t, err)
	assert.Equal(t, internalTaskID, event.TaskID())
	assert.Equal(t, pbtask.TaskState_RUNNING, event.State())
	assert.Equal(t, msg, event.StatusMsg())
	assert.Equal(t, hostname, event.AgentID().GetValue())
	assert.Nil(t, event.MesosTaskStatus())
	assert.Equal(t, offset, event.Offset())
	assert.Equal(t, reason, event.Reason())
	assert.Equal(t, healthy, event.Healthy())
	assert.Equal(t, msg, event.Message())
	assert.Equal(t, now.UnixNano()/1e9, int64(*event.Timestamp()))

	// podID doesn't conform to peloton podID convention.
	// This should result in error.
	event, err = NewV1(newV1EventPb(
		internalTaskID,
		offset,
		state,
		now,
		hostname,
		msg,
		reason,
		healthy,
	))
	assert.Error(t, err)
}
