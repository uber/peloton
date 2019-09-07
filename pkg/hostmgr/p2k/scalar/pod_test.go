package scalar

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBuildPodEventFromPodPending(t *testing.T) {
	require := require.New(t)

	k8sContainerStatus := []corev1.ContainerStatus{}
	for i := 0; i < 4; i++ {
		status := corev1.ContainerStatus{
			Name: fmt.Sprintf("container%d", i),
			State: corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{
					Message: fmt.Sprintf("message%d", i),
				},
			},
			Image: fmt.Sprintf("image%d", i),
		}
		k8sContainerStatus = append(k8sContainerStatus, status)
	}

	k8sPodEvent := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod1",
			Namespace: "namespace1",
			Labels: map[string]string{
				"key1": "value1",
			},
			ResourceVersion: "version1",
		},
		Spec: corev1.PodSpec{
			NodeName: "node1",
		},
		Status: corev1.PodStatus{
			Phase:   corev1.PodPending,
			Message: "message1",
			Reason:  "reason1",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionFalse,
				},
			},
			InitContainerStatuses: k8sContainerStatus[:2],
			ContainerStatuses:     k8sContainerStatus[2:],
		},
	}

	expectedPelotonContainerStatus := []*pbpod.ContainerStatus{}
	for i := 0; i < 4; i++ {
		status := &pbpod.ContainerStatus{
			Name:              k8sContainerStatus[i].Name,
			State:             pbpod.ContainerState_CONTAINER_STATE_LAUNCHED,
			Reason:            k8sContainerStatus[i].State.Waiting.Reason,
			Message:           k8sContainerStatus[i].State.Waiting.Message,
			Image:             k8sContainerStatus[i].Image,
			StartTime:         time.Time{}.Format(time.RFC3339),
			CompletionTime:    time.Time{}.Format(time.RFC3339),
			TerminationStatus: nil,
			FailureCount:      uint32(k8sContainerStatus[i].RestartCount),
			Healthy: &pbpod.HealthStatus{
				State: pbpod.HealthState_HEALTH_STATE_UNKNOWN,
			},
		}
		expectedPelotonContainerStatus = append(expectedPelotonContainerStatus, status)
	}

	expectedPelotonEvent := &PodEvent{
		Event: &pbpod.PodEvent{
			PodId:               &peloton.PodID{Value: "pod1"},
			ActualState:         pbpod.PodState_POD_STATE_LAUNCHED.String(),
			Timestamp:           time.Now().Format(time.RFC3339),
			AgentId:             "node1",
			Hostname:            "node1",
			Message:             "message1",
			Reason:              "reason1",
			Healthy:             pbpod.HealthState_HEALTH_STATE_UNHEALTHY.String(),
			InitContainerStatus: expectedPelotonContainerStatus[:2],
			ContainerStatus:     expectedPelotonContainerStatus[2:],
		},
		EventType: AddPod,
		EventID:   "version1",
	}

	output := BuildPodEventFromPod(k8sPodEvent, AddPod)

	require.True(reflect.DeepEqual(expectedPelotonEvent, output))
}

func TestBuildPodEventFromPodRunning(t *testing.T) {
	require := require.New(t)

	ts := time.Now()

	k8sContainerStatus := []corev1.ContainerStatus{}
	for i := 0; i < 4; i++ {
		status := corev1.ContainerStatus{
			Name: fmt.Sprintf("container%d", i),
			State: corev1.ContainerState{
				Running: &corev1.ContainerStateRunning{
					StartedAt: metav1.NewTime(ts),
				},
			},
			Image: fmt.Sprintf("image%d", i),
		}
		k8sContainerStatus = append(k8sContainerStatus, status)
	}

	k8sPodEvent := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod1",
			Namespace: "namespace1",
			Labels: map[string]string{
				"key1": "value1",
			},
			ResourceVersion: "version1",
		},
		Spec: corev1.PodSpec{
			NodeName: "node1",
			Containers: []corev1.Container{
				{
					Ports: []corev1.ContainerPort{
						{
							Name:          "dynamicport",
							ContainerPort: 31234,
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase:   corev1.PodRunning,
			Message: "message1",
			Reason:  "reason1",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
			InitContainerStatuses: k8sContainerStatus[:2],
			ContainerStatuses:     k8sContainerStatus[2:],
		},
	}

	expectedPelotonContainerStatus := []*pbpod.ContainerStatus{}
	expectedPods := map[string]uint32{"dynamicport": 31234}
	for i := 0; i < 4; i++ {
		status := &pbpod.ContainerStatus{
			Name:              k8sContainerStatus[i].Name,
			State:             pbpod.ContainerState_CONTAINER_STATE_RUNNING,
			Reason:            "",
			Message:           "",
			Image:             k8sContainerStatus[i].Image,
			StartTime:         ts.Format(time.RFC3339),
			CompletionTime:    time.Time{}.Format(time.RFC3339),
			TerminationStatus: nil,
			FailureCount:      uint32(k8sContainerStatus[i].RestartCount),
			Healthy: &pbpod.HealthStatus{
				State: pbpod.HealthState_HEALTH_STATE_UNKNOWN,
			},
		}
		if i == 2 {
			// first app container
			status.Ports = expectedPods
		}
		expectedPelotonContainerStatus = append(expectedPelotonContainerStatus, status)
	}

	expectedPelotonEvent := &PodEvent{
		Event: &pbpod.PodEvent{
			PodId:               &peloton.PodID{Value: "pod1"},
			ActualState:         pbpod.PodState_POD_STATE_RUNNING.String(),
			Timestamp:           time.Now().Format(time.RFC3339),
			AgentId:             "node1",
			Hostname:            "node1",
			Message:             "message1",
			Reason:              "reason1",
			Healthy:             pbpod.HealthState_HEALTH_STATE_HEALTHY.String(),
			InitContainerStatus: expectedPelotonContainerStatus[:2],
			ContainerStatus:     expectedPelotonContainerStatus[2:],
		},
		EventType: UpdatePod,
		EventID:   "version1",
	}

	output := BuildPodEventFromPod(k8sPodEvent, UpdatePod)

	require.True(reflect.DeepEqual(expectedPelotonEvent, output))
}

// This case is a little weird - for a pod that terminated successfully, pod
// will be healthy (since it's ready) but containers will be unhealthy (since
// they are no longer running).
func TestBuildPodEventFromPodSucceeded(t *testing.T) {
	require := require.New(t)

	k8sContainerStatus := []corev1.ContainerStatus{}
	for i := 0; i < 4; i++ {
		status := corev1.ContainerStatus{
			Name: fmt.Sprintf("container%d", i),
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					Message: fmt.Sprintf("message%d", i),
				},
			},
			LastTerminationState: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					Message: fmt.Sprintf("message%d", i),
				},
			},
			Image: fmt.Sprintf("image%d", i),
		}
		k8sContainerStatus = append(k8sContainerStatus, status)
	}

	k8sPodEvent := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod1",
			Namespace: "namespace1",
			Labels: map[string]string{
				"key1": "value1",
			},
			ResourceVersion: "version1",
		},
		Spec: corev1.PodSpec{
			NodeName: "node1",
		},
		Status: corev1.PodStatus{
			Phase:   corev1.PodSucceeded,
			Message: "message1",
			Reason:  "reason1",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
			InitContainerStatuses: k8sContainerStatus[:2],
			ContainerStatuses:     k8sContainerStatus[2:],
		},
	}

	expectedPelotonContainerStatus := []*pbpod.ContainerStatus{}
	for i := 0; i < 4; i++ {
		status := &pbpod.ContainerStatus{
			Name:           k8sContainerStatus[i].Name,
			State:          pbpod.ContainerState_CONTAINER_STATE_SUCCEEDED,
			Reason:         k8sContainerStatus[i].State.Terminated.Reason,
			Message:        k8sContainerStatus[i].State.Terminated.Message,
			Image:          k8sContainerStatus[i].Image,
			StartTime:      k8sContainerStatus[i].State.Terminated.StartedAt.Time.Format(time.RFC3339),
			CompletionTime: k8sContainerStatus[i].State.Terminated.FinishedAt.Time.Format(time.RFC3339),
			TerminationStatus: &pbpod.TerminationStatus{
				ExitCode: uint32(k8sContainerStatus[i].State.Terminated.ExitCode),
				Signal:   string(k8sContainerStatus[i].State.Terminated.Signal),
			},
			FailureCount: uint32(k8sContainerStatus[i].RestartCount),
			Healthy: &pbpod.HealthStatus{
				State: pbpod.HealthState_HEALTH_STATE_UNHEALTHY,
			},
		}
		expectedPelotonContainerStatus = append(expectedPelotonContainerStatus, status)
	}

	expectedPelotonEvent := &PodEvent{
		Event: &pbpod.PodEvent{
			PodId:               &peloton.PodID{Value: "pod1"},
			ActualState:         pbpod.PodState_POD_STATE_SUCCEEDED.String(),
			Timestamp:           time.Now().Format(time.RFC3339),
			AgentId:             "node1",
			Hostname:            "node1",
			Message:             "message1",
			Reason:              "reason1",
			Healthy:             pbpod.HealthState_HEALTH_STATE_HEALTHY.String(),
			InitContainerStatus: expectedPelotonContainerStatus[:2],
			ContainerStatus:     expectedPelotonContainerStatus[2:],
		},
		EventType: DeletePod,
		EventID:   "version1",
	}

	output := BuildPodEventFromPod(k8sPodEvent, DeletePod)

	require.True(reflect.DeepEqual(expectedPelotonEvent, output))
}

func TestBuildPodState(t *testing.T) {
	testCases := []struct {
		name  string
		phase corev1.PodPhase
		event PodEventType
		state string
	}{
		{
			"add pending",
			corev1.PodPending,
			AddPod,
			pbpod.PodState_POD_STATE_LAUNCHED.String(),
		},
		{
			"update running",
			corev1.PodRunning,
			UpdatePod,
			pbpod.PodState_POD_STATE_RUNNING.String(),
		},
		{
			"update succeeded",
			corev1.PodSucceeded,
			UpdatePod,
			pbpod.PodState_POD_STATE_SUCCEEDED.String(),
		},
		{
			"update failed",
			corev1.PodFailed,
			UpdatePod,
			pbpod.PodState_POD_STATE_FAILED.String(),
		},
		{
			"update known",
			corev1.PodUnknown,
			UpdatePod,
			pbpod.PodState_POD_STATE_LOST.String(),
		},
		{
			"invalid state",
			corev1.PodPhase("invalid"),
			UpdatePod,
			pbpod.PodState_POD_STATE_INVALID.String(),
		},
		{
			"delete pending",
			corev1.PodPending,
			DeletePod,
			pbpod.PodState_POD_STATE_KILLED.String(),
		},
		{
			"delete running",
			corev1.PodRunning,
			DeletePod,
			pbpod.PodState_POD_STATE_KILLED.String(),
		},
		{
			"delete succeeded",
			corev1.PodSucceeded,
			DeletePod,
			pbpod.PodState_POD_STATE_SUCCEEDED.String(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			require.Equal(tc.state, buildPodState(tc.phase, tc.event))
		})
	}
}
