package atop

import (
	"errors"
	"fmt"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/thrift/aurora/api"
)

var errUnimplemented = errors.New("unimplemented aurora schedule status")

// NewPodState converts Aurora ScheduleStatus enum
// to Peloton PodState enum.
func NewPodState(
	scheduleStatus api.ScheduleStatus) (pod.PodState, error) {
	switch scheduleStatus {
	case api.ScheduleStatusPending:
		// Task is awaiting assignment to a slave.
		return pod.PodState_POD_STATE_PENDING, nil
	case api.ScheduleStatusStarting:
		// Slave has acknowledged receipt of task and is bootstrapping
		// the task.
		return pod.PodState_POD_STATE_STARTING, nil
	case api.ScheduleStatusRunning:
		// The task is running on the slave.
		return pod.PodState_POD_STATE_RUNNING, nil
	case api.ScheduleStatusFinished:
		// The task terminated with an exit code of zero.
		return pod.PodState_POD_STATE_SUCCEEDED, nil
	case api.ScheduleStatusFailed:
		// The task terminated with a non-zero exit code.
		return pod.PodState_POD_STATE_FAILED, nil
	case api.ScheduleStatusKilled:
		// Execution of the task was terminated by the system.
		return pod.PodState_POD_STATE_KILLED, nil
	case api.ScheduleStatusKilling:
		// The task is being forcibly killed.
		return pod.PodState_POD_STATE_KILLING, nil
	case api.ScheduleStatusLost:
		// A fault in the task environment has caused the system to
		// believe the task no longer exists. This can happen, for example,
		// when a slave process disappears.
		return pod.PodState_POD_STATE_LOST, nil
	case api.ScheduleStatusAssigned:
		// Task has been assigned to a slave.
		return pod.PodState_POD_STATE_LAUNCHED, nil
	case api.ScheduleStatusInit:
		// Initial state for a task. A task will remain in this state
		// until it has been persisted.
		return pod.PodState_POD_STATE_INITIALIZED, nil
	case api.ScheduleStatusRestarting:
		// The task is being restarted in response to a user request.
		return pod.PodState_POD_STATE_STARTING, nil
	case api.ScheduleStatusPreempting:
		// The task is being preempted by another task.
		return pod.PodState_POD_STATE_PREEMPTING, nil
	case api.ScheduleStatusThrottled:
		// The task will be rescheduled, but is being throttled for
		// restarting too frequently.
		// TODO(kevinxu): currently we do not have direct mapping
		// for aurora "throttled" state in peloton. since aggregator
		// queries "throttled" state, map to pod state "failed" for now
		// to avoid panic.
		return pod.PodState_POD_STATE_FAILED, nil
	case api.ScheduleStatusDraining:
		// The task is being restarted in response to a host maintenance
		// request.
		return pod.PodState_POD_STATE_KILLING, nil
	default:
		return pod.PodState_POD_STATE_INVALID,
			fmt.Errorf("unknown schedule status: %d", scheduleStatus)
	}
}
