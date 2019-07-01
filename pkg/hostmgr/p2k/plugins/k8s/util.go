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
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Convert peloton container specs to k8s container specs
func toK8SContainerSpecs(
	containerSpecs []*pbpod.ContainerSpec,
) []corev1.Container {
	var containers []corev1.Container
	for _, c := range containerSpecs {
		containers = append(containers, toK8SContainerSpec(c))
	}
	return containers
}

// Convert peloton container spec to k8s container spec
func toK8SContainerSpec(c *pbpod.ContainerSpec) corev1.Container {
	// TODO:
	// add ports, health check, readiness check, affinity
	var kEnvs []corev1.EnvVar
	for _, e := range c.GetEnvironment() {
		kEnvs = append(kEnvs, corev1.EnvVar{
			Name:  e.GetName(),
			Value: e.GetValue(),
		})
	}

	var ports []corev1.ContainerPort
	for _, p := range c.GetPorts() {
		ports = append(ports, corev1.ContainerPort{
			Name:          p.GetName(),
			ContainerPort: int32(p.GetValue()),
		})
	}

	k8sSpec := corev1.Container{
		Name:  c.GetName(),
		Image: c.GetImage(),
		Env:   kEnvs,
		Ports: ports,
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceCPU: *resource.NewMilliQuantity(
					int64(c.GetResource().GetCpuLimit()*1000),
					resource.DecimalSI,
				),
				corev1.ResourceMemory: *resource.NewMilliQuantity(
					int64(c.GetResource().GetMemLimitMb()*1000000000),
					resource.DecimalSI,
				),
			},
			Requests: corev1.ResourceList{
				corev1.ResourceCPU: *resource.NewMilliQuantity(
					int64(c.GetResource().GetCpuLimit()*1000),
					resource.DecimalSI,
				),
				corev1.ResourceMemory: *resource.NewMilliQuantity(
					int64(c.GetResource().GetMemLimitMb()*1000000000),
					resource.DecimalSI,
				),
			},
		},
	}

	if entrypoint := c.GetEntrypoint().GetValue(); entrypoint != "" {
		k8sSpec.Command = []string{entrypoint}
		k8sSpec.Args = c.Command.Arguments
	}

	return k8sSpec
}

// Convert peloton podspec to k8s podspec.
func toK8SPodSpec(podSpec *pbpod.PodSpec) *corev1.Pod {
	// Create pod template spec and apply configurations to spec.
	labels := make(map[string]string)
	for _, label := range podSpec.GetLabels() {
		labels[label.GetKey()] = label.GetValue()
	}

	termGracePeriod := int64(podSpec.GetKillGracePeriodSeconds())

	podTemp := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: labels,
		},
		Spec: corev1.PodSpec{
			Containers:                    toK8SContainerSpecs(podSpec.GetContainers()),
			InitContainers:                toK8SContainerSpecs(podSpec.GetInitContainers()),
			RestartPolicy:                 "Never",
			TerminationGracePeriodSeconds: &termGracePeriod,
		},
	}

	// Bind node and create pod.
	return &corev1.Pod{
		ObjectMeta: podTemp.ObjectMeta,
		Spec:       podTemp.Spec,
	}
}
