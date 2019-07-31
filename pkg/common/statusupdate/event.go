package statusupdate

import (
	"errors"
	"time"

	pbmesos "github.com/uber/peloton/.gen/mesos/v1"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	pbeventstream "github.com/uber/peloton/.gen/peloton/private/eventstream"
	v1pbevent "github.com/uber/peloton/.gen/peloton/private/eventstream/v1alpha/event"

	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/common/util"

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
func NewV1(event *v1pbevent.Event) *Event {
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
	if event.Type == pbeventstream.Event_MESOS_TASK_STATUS {
		mesosTaskID := event.MesosTaskStatus.GetTaskId().GetValue()
		updateEvent.taskID, err = util.ParseTaskIDFromMesosTaskID(mesosTaskID)
		if err != nil {
			log.WithError(err).
				WithField("task_id", mesosTaskID).
				Error("Fail to parse TaskID for mesosTaskID")
			return nil, err
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
	}
	return pbpod.PodState_POD_STATE_INVALID
}

func convertV1Event(event *v1pbevent.Event) *Event {
	podEvent := event.PodEvent
	return &Event{
		taskID:  podEvent.PodId.Value,
		state:   api.ConvertPodStateToTaskState(parsePodState(podEvent.ActualState)),
		v1event: event,
	}
}
