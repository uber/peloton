package scalar

import (
	"reflect"
	"testing"
	"time"

	"github.com/uber/peloton/pkg/hostmgr/models"
	hmscalar "github.com/uber/peloton/pkg/hostmgr/scalar"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBuildHostEventFromNode(t *testing.T) {
	require := require.New(t)

	node := &corev1.Node{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Node",
			APIVersion: "v1",
		},
		Spec: corev1.NodeSpec{},
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-node",
			CreationTimestamp: metav1.Time{Time: time.Now()},
		},
		Status: corev1.NodeStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("32"),
				corev1.ResourceMemory: resource.MustParse("96Gi"),
			},
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("32"),
				corev1.ResourceMemory: resource.MustParse("96Gi"),
			},
		},
	}

	expectedHostEvent := &HostEvent{
		hostInfo: &HostInfo{
			hostname: "test-node",
			podMap:   map[string]models.HostResources{},
			capacity: models.HostResources{
				NonSlack: hmscalar.Resources{
					CPU: float64(32),
					Mem: float64(
						node.Status.Capacity.Memory().MilliValue()) / 1000000000,
					Disk: getDefaultDiskMbPerHost(),
					GPU:  0,
				},
			},
		},
		eventType: AddHost,
	}

	hostEvent, err := BuildHostEventFromNode(node, AddHost)
	require.Nil(err)
	require.True(reflect.DeepEqual(expectedHostEvent, hostEvent))
}
