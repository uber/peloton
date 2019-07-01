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

package k8s

import (
	"time"

	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func newTestK8sPod(podName string) *corev1.Pod {
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              podName,
			Namespace:         "test_ns",
			CreationTimestamp: metav1.Time{Time: time.Now()},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:            "test_container",
					Image:           "test_image",
					ImagePullPolicy: corev1.PullIfNotPresent,
					Command: []string{
						"./start.sh",
					},
					Ports: []corev1.ContainerPort{
						{
							Name:          "http",
							ContainerPort: 8080,
						},
					},
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceCPU: *resource.NewMilliQuantity(
								int64(1*1000),
								resource.DecimalSI,
							),
							corev1.ResourceMemory: *resource.NewMilliQuantity(
								int64(100*1000000000),
								resource.DecimalSI,
							),
						},
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: *resource.NewMilliQuantity(
								int64(1*1000),
								resource.DecimalSI,
							),
							corev1.ResourceMemory: *resource.NewMilliQuantity(
								int64(100*1000000000),
								resource.DecimalSI,
							),
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{},
	}
}

func newTestK8sNode(nodeName string) *corev1.Node {
	return &corev1.Node{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Node",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              nodeName,
			CreationTimestamp: metav1.Time{Time: time.Now()},
		},
		Spec: corev1.NodeSpec{},
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
}

func newTestPelotonPodSpec(podName string) *pbpod.PodSpec {
	return &pbpod.PodSpec{
		Containers: []*pbpod.ContainerSpec{
			{
				Name: podName,
				Resource: &pbpod.ResourceSpec{
					CpuLimit:   1.0,
					MemLimitMb: 100.0,
				},
				Ports: []*pbpod.PortSpec{
					{
						Name:  "http",
						Value: 8080,
					},
				},
				Image: "test_image",
			},
		},
	}
}
