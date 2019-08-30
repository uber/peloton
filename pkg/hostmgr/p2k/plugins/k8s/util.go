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

	"github.com/pborman/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// K8S requires pod spec to contain an image name, default to busybox.
	_defaultImageName = "busybox"
	// K8S enforces minimum mem limit for container to be 4MB. KinD enforces
	// this limit as 100MB.
	_defaultMinMemMb = 100.0
)

// K8S node and pod informers will resync all nodes and pods at this
// interval. This will be used for reconciliation of pods and hostcache.
var _defaultResyncInterval = 30 * time.Second

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

	cname := c.GetName()
	if cname == "" {
		cname = uuid.New()
	}

	cimage := c.GetImage()
	if cimage == "" {
		cimage = _defaultImageName
	}

	memMb := c.GetResource().GetMemLimitMb()
	if memMb < _defaultMinMemMb {
		memMb = _defaultMinMemMb
	}

	volumeMounts := []corev1.VolumeMount{}
	for _, v := range c.GetVolumeMounts() {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      v.GetName(),
			ReadOnly:  v.GetReadOnly(),
			MountPath: v.GetMountPath(),
		})
	}

	k8sSpec := corev1.Container{
		Name:         cname,
		Image:        cimage,
		Env:          kEnvs,
		Ports:        ports,
		VolumeMounts: volumeMounts,
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceCPU: *resource.NewMilliQuantity(
					int64(c.GetResource().GetCpuLimit()*1000),
					resource.DecimalSI,
				),
				corev1.ResourceMemory: *resource.NewMilliQuantity(
					int64(memMb*1000000000),
					resource.DecimalSI,
				),
			},
			Requests: corev1.ResourceList{
				corev1.ResourceCPU: *resource.NewMilliQuantity(
					int64(c.GetResource().GetCpuLimit()*1000),
					resource.DecimalSI,
				),
				corev1.ResourceMemory: *resource.NewMilliQuantity(
					int64(memMb*1000000000),
					resource.DecimalSI,
				),
			},
		},
	}

	if c.GetEntrypoint().GetValue() != "" {
		k8sSpec.Command = []string{c.GetEntrypoint().GetValue()}
		k8sSpec.Args = c.GetEntrypoint().GetArguments()
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
