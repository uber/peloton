package scalar

import (
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"

	corev1 "k8s.io/api/core/v1"
)

// PodEventType describes the type of pod event sent by plugin.
type PodEventType int

const (
	// AddPod event type.
	AddPod PodEventType = iota + 1
	// UpdatePod event type.
	UpdatePod
	// DeletePod event type.
	DeletePod
)

// PodEvent contains peloton pod event protobuf, event type and resource version
// for that event.
type PodEvent struct {
	// Peloton internal pod event protobuf.
	Event *pbpod.PodEvent
	// Type of pod event.
	EventType PodEventType
	// Event ID for the event. This should be used for de-duping the event stream.
	EventID string
}

func BuildPodEventFromPod(
	pod *corev1.Pod,
	e PodEventType,
) *PodEvent {
	convertedInitStatuses := make([]*pbpod.ContainerStatus, len(pod.Status.InitContainerStatuses))
	for i := range pod.Status.InitContainerStatuses {
		var icspec *corev1.Container
		if i < len(pod.Spec.InitContainers) {
			icspec = &pod.Spec.InitContainers[i]
		}
		status := &pod.Status.InitContainerStatuses[i]
		convertedInitStatuses[i] = buildContainerStatus(icspec, status)
	}

	convertedContainerStatuses := make([]*pbpod.ContainerStatus, len(pod.Status.ContainerStatuses))
	for i := range pod.Status.ContainerStatuses {
		var cspec *corev1.Container
		if i < len(pod.Spec.Containers) {
			cspec = &pod.Spec.Containers[i]
		}
		status := &pod.Status.ContainerStatuses[i]
		convertedContainerStatuses[i] = buildContainerStatus(cspec, status)
	}

	return &PodEvent{
		Event: &pbpod.PodEvent{
			PodId:               &peloton.PodID{Value: pod.Name},
			ActualState:         buildPodState(pod.Status.Phase, e),
			Timestamp:           time.Now().Format(time.RFC3339),
			AgentId:             pod.Spec.NodeName,
			Hostname:            pod.Spec.NodeName,
			Message:             pod.Status.Message,
			Reason:              pod.Status.Reason,
			Healthy:             buildPodHealthStatus(pod.Status.Conditions),
			InitContainerStatus: convertedInitStatuses,
			ContainerStatus:     convertedContainerStatuses,
		},
		EventType: e,
		EventID:   pod.ResourceVersion,
	}
}

func buildPodState(phase corev1.PodPhase, e PodEventType) string {
	// Manually set the pod state to KILLED because it is not a valid state in
	// K8s. When running pod is deleted, only a delete grace period is added to
	// the pod spec and we get a delete event. No more event is sent when pod
	// exits.
	// NOTE: this is assuming delete grade period is relatively short (10 sec).
	if e == DeletePod && (phase == corev1.PodRunning || phase == corev1.PodPending) {
		return pbpod.PodState_POD_STATE_KILLED.String()
	}
	switch phase {
	case corev1.PodPending:
		return pbpod.PodState_POD_STATE_LAUNCHED.String()
	case corev1.PodRunning:
		return pbpod.PodState_POD_STATE_RUNNING.String()
	case corev1.PodSucceeded:
		return pbpod.PodState_POD_STATE_SUCCEEDED.String()
	case corev1.PodFailed:
		return pbpod.PodState_POD_STATE_FAILED.String()
	case corev1.PodUnknown:
		return pbpod.PodState_POD_STATE_LOST.String()
	}
	return pbpod.PodState_POD_STATE_INVALID.String()
}

// buildPodHealthStatus returns healthy only if default liveness and readiness health check all
// passed, and all custom readiness gates defined in spec also passed.
// See https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-readiness-gate
func buildPodHealthStatus(conditions []corev1.PodCondition) string {
	for _, condition := range conditions {
		if condition.Status == "False" {
			return pbpod.HealthState_HEALTH_STATE_UNHEALTHY.String()
		}
	}
	for _, condition := range conditions {
		if condition.Status == "Unknown" {
			return pbpod.HealthState_HEALTH_STATE_UNKNOWN.String()
		}
	}
	return pbpod.HealthState_HEALTH_STATE_HEALTHY.String()
}

// buildContainerStatus constructs peloton container status from k8s container status.
// TODO: missing ports and termination reason.
func buildContainerStatus(containerSpec *corev1.Container, containerStatus *corev1.ContainerStatus) *pbpod.ContainerStatus {
	var reason, message string
	var startedAt, finishedAt time.Time
	var terminationStatus *pbpod.TerminationStatus

	healthState := pbpod.HealthState_HEALTH_STATE_UNKNOWN
	state := pbpod.ContainerState_CONTAINER_STATE_INVALID

	if containerStatus.State.Waiting != nil {
		reason = containerStatus.State.Waiting.Reason
		message = containerStatus.State.Waiting.Message
		state = pbpod.ContainerState_CONTAINER_STATE_LAUNCHED
	} else if containerStatus.State.Running != nil {
		startedAt = containerStatus.State.Running.StartedAt.Time
		state = pbpod.ContainerState_CONTAINER_STATE_RUNNING
		if containerStatus.Ready {
			healthState = pbpod.HealthState_HEALTH_STATE_HEALTHY
		}
	} else if containerStatus.State.Terminated != nil {
		reason = containerStatus.State.Terminated.Reason
		message = containerStatus.State.Terminated.Message
		startedAt = containerStatus.State.Terminated.StartedAt.Time
		finishedAt = containerStatus.State.Terminated.FinishedAt.Time
		healthState = pbpod.HealthState_HEALTH_STATE_UNHEALTHY
		terminationStatus = &pbpod.TerminationStatus{
			ExitCode: uint32(containerStatus.State.Terminated.ExitCode),
			Signal:   string(containerStatus.State.Terminated.Signal),
			// TODO: Reason
		}

		if containerStatus.State.Terminated.ExitCode == 0 &&
			containerStatus.State.Terminated.Signal == 0 {
			state = pbpod.ContainerState_CONTAINER_STATE_SUCCEEDED
		} else {
			if containerStatus.State.Terminated.Signal != 0 {
				state = pbpod.ContainerState_CONTAINER_STATE_KILLED
			} else {
				state = pbpod.ContainerState_CONTAINER_STATE_FAILED
			}
		}
	}

	var ports map[string]uint32
	if containerSpec != nil {
		for i := range containerSpec.Ports {
			if i == 0 {
				ports = make(map[string]uint32, len(containerSpec.Ports))
			}
			ports[containerSpec.Ports[i].Name] = uint32(containerSpec.Ports[i].ContainerPort)
		}
	}

	return &pbpod.ContainerStatus{
		Name:              containerStatus.Name,
		State:             state,
		Ports:             ports,
		Reason:            reason,
		Message:           message,
		Image:             containerStatus.Image,
		StartTime:         startedAt.Format(time.RFC3339),
		CompletionTime:    finishedAt.Format(time.RFC3339),
		TerminationStatus: terminationStatus,
		FailureCount:      uint32(containerStatus.RestartCount),
		Healthy: &pbpod.HealthStatus{
			State: healthState,
		},
	}
}
