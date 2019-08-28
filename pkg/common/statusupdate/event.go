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
	"time"

	pbmesos "github.com/uber/peloton/.gen/mesos/v1"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	pbeventstream "github.com/uber/peloton/.gen/peloton/private/eventstream"
	v1pbevent "github.com/uber/peloton/.gen/peloton/private/eventstream/v1alpha/event"

	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/common/util"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Event provides a common interface to v0 and v1 event.
type Event struct {
	taskID string
	state  pbtask.TaskState
	// either v0event or v1event is set
	v0event *pbeventstream.Event
	v1event *v1pbevent.Event
}

// New an Event from a v0 event.
func NewV0(event *pbeventstream.Event) (*Event, error) {
	return convertV0Event(event)
}

// New an Event from a v1 event.
func NewV1(event *v1pbevent.Event) (*Event, error) {
	return convertV1Event(event)
}

// V0 returns underlying v0 event.
func (sue *Event) V0() *pbeventstream.Event {
	if sue == nil {
		return nil
	}
	return sue.v0event
}

// V1 returns the underlying v1 event.
func (sue *Event) V1() *v1pbevent.Event {
	if sue == nil {
		return nil
	}
	return sue.v1event
}

// TaskID returns task ID of the event.
// It is the peloton task id for v0 event and pod id for v1 event.
func (sue *Event) TaskID() string {
	return sue.taskID
}

// State returns task state.
func (sue *Event) State() pbtask.TaskState {
	return sue.state
}

// StatusMsg returns status message.
func (sue *Event) StatusMsg() string {
	if sue.v0event != nil {
		return sue.v0event.GetMesosTaskStatus().GetMessage()
	}
	return sue.v1event.GetPodEvent().GetMessage()
}

// MesosTaskID returns mesos task ID.
func (sue *Event) MesosTaskID() *pbmesos.TaskID {
	if sue.v0event != nil {
		return sue.v0event.MesosTaskStatus.GetTaskId()
	}
	return &pbmesos.TaskID{Value: &sue.taskID}
}

// AgentID returns agent ID.
func (sue *Event) AgentID() *pbmesos.AgentID {
	if sue.v0event != nil {
		return sue.v0event.MesosTaskStatus.GetAgentId()
	}
	return &pbmesos.AgentID{Value: &sue.v1event.PodEvent.Hostname}
}

// MesosTaskStatus returns mesos task status for v0 event only.
func (sue *Event) MesosTaskStatus() *pbmesos.TaskStatus {
	if sue.v0event != nil {
		return sue.v0event.GetMesosTaskStatus()
	}
	return nil
}

// Offset returns offset.
func (sue *Event) Offset() uint64 {
	if sue.v0event != nil {
		return sue.v0event.Offset
	}
	return sue.v1event.Offset
}

// PodEvent returns pod event for v1 event only.
func (sue *Event) PodEvent() *pbpod.PodEvent {
	if sue.v1event != nil {
		return sue.v1event.PodEvent
	}
	return nil
}

// Reason returns reason.
func (sue *Event) Reason() string {
	if sue.v0event != nil {
		return sue.v0event.GetMesosTaskStatus().GetReason().String()
	}
	return sue.v1event.PodEvent.GetReason()
}

// Healthy returns healthy bit.
func (sue *Event) Healthy() bool {
	if sue.v0event != nil {
		return sue.v0event.GetMesosTaskStatus().GetHealthy()
	}
	return sue.v1event.PodEvent.GetHealthy() == pbpod.HealthState_HEALTH_STATE_HEALTHY.String()
}

// Message returns message.
func (sue *Event) Message() string {
	if sue.v0event != nil {
		return sue.v0event.GetMesosTaskStatus().GetMessage()
	}
	return sue.v1event.PodEvent.GetMessage()
}

// Timestamp returns timestamp in second.
func (sue *Event) Timestamp() *float64 {
	var ts float64
	if sue.v0event != nil {
		ts = sue.v0event.GetMesosTaskStatus().GetTimestamp()
	} else {
		t, err := time.Parse(time.RFC3339, sue.v1event.PodEvent.GetTimestamp())
		if err != nil {
			log.WithField("pod_timestamp", sue.v1event.PodEvent.GetTimestamp()).Error("invalid timestamp")
		} else {
			ts = float64(t.UnixNano()) / 1e9
		}
	}
	return &ts
}

func convertV0Event(event *pbeventstream.Event) (*Event, error) {
	var err error

	updateEvent := &Event{v0event: event}
	if event.Type == pbeventstream.Event_HOST_EVENT {
		return nil, fmt.Errorf("cannot convert host-event to statusupdate event")
	} else if event.Type == pbeventstream.Event_MESOS_TASK_STATUS {
		mesosTaskID := event.MesosTaskStatus.GetTaskId().GetValue()
		updateEvent.taskID, err = util.ParseTaskIDFromMesosTaskID(mesosTaskID)
		if err != nil {
			return nil, errors.Wrap(err,
				fmt.Sprintf(
					"failed to parse taskID from mesosTaskID: %v",
					mesosTaskID,
				),
			)
		}
		updateEvent.state = util.MesosStateToPelotonState(event.MesosTaskStatus.GetState())

		log.WithFields(log.Fields{
			"task_id": updateEvent.taskID,
			"state":   updateEvent.state.String(),
		}).Debug("Adding Mesos Event ")

	} else if event.Type == pbeventstream.Event_PELOTON_TASK_EVENT {
		// Peloton task event is used for task status update from resmgr.
		updateEvent.taskID = event.PelotonTaskEvent.TaskId.Value
		updateEvent.state = event.PelotonTaskEvent.State
		log.WithFields(log.Fields{
			"task_id": updateEvent.taskID,
			"state":   updateEvent.state.String(),
		}).Debug("Adding Peloton Event ")
	} else {
		log.WithFields(log.Fields{
			"task_id": updateEvent.taskID,
			"state":   updateEvent.state.String(),
		}).Error("Unknown Event ")
		return nil, errors.New("Unknown Event ")
	}
	return updateEvent, nil
}

func parsePodState(s string) pbpod.PodState {
	switch s {
	case pbpod.PodState_POD_STATE_LAUNCHED.String():
		return pbpod.PodState_POD_STATE_LAUNCHED
	case pbpod.PodState_POD_STATE_RUNNING.String():
		return pbpod.PodState_POD_STATE_RUNNING
	case pbpod.PodState_POD_STATE_SUCCEEDED.String():
		return pbpod.PodState_POD_STATE_SUCCEEDED
	case pbpod.PodState_POD_STATE_FAILED.String():
		return pbpod.PodState_POD_STATE_FAILED
	case pbpod.PodState_POD_STATE_LOST.String():
		return pbpod.PodState_POD_STATE_LOST
	case pbpod.PodState_POD_STATE_KILLED.String():
		return pbpod.PodState_POD_STATE_KILLED
	}
	return pbpod.PodState_POD_STATE_INVALID
}

func convertV1Event(event *v1pbevent.Event) (*Event, error) {
	podEvent := event.GetPodEvent()
	// At this point, podID and mesosTaskID look the same. So we can just
	// extract taskID from podID using the same way as we do for mesos in
	// case of v0 event.
	taskID, err := util.ParseTaskIDFromMesosTaskID(
		podEvent.GetPodId().GetValue(),
	)
	if err != nil {
		return nil, errors.Wrap(err,
			fmt.Sprintf(
				"failed to parse taskID from podID: %v",
				podEvent.GetPodId().GetValue(),
			),
		)
	}

	return &Event{
		taskID: taskID,
		state: api.ConvertPodStateToTaskState(
			parsePodState(
				podEvent.ActualState),
		),
		v1event: event,
	}, nil
}
