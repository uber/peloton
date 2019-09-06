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

package atop

import (
	"fmt"
	"path"
	"strings"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/apachemesos"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/volume"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/aurorabridge/common"
	"github.com/uber/peloton/pkg/aurorabridge/label"
	"github.com/uber/peloton/pkg/common/config"
	"github.com/uber/peloton/pkg/common/thermos"
)

// NewPodSpec creates a new PodSpec.
func NewPodSpec(
	t *api.TaskConfig,
	c config.ThermosExecutorConfig,
) (*pod.PodSpec, error) {
	// Taking aurora TaskConfig struct from JobUpdateRequest, and
	// serialize it using Thrift binary protocol. The resulting
	// byte array will be attached to ExecutorInfo.Data.
	//
	// After task placement, jobmgr will deserialize the byte array,
	// combine it with placement info, and generates aurora AssignedTask
	// struct, which will be eventually be consumed by thermos executor.
	//
	// Leaving EncodeTaskConfig at the top since it has the side-effect
	// of sorting all the "list" fields.
	executorData, err := thermos.EncodeTaskConfig(t)
	if err != nil {
		return nil, fmt.Errorf("encode task config: %s", err)
	}

	jobKeyLabel := label.NewAuroraJobKey(t.GetJob())
	metadataLabels := label.NewAuroraMetadataLabels(t.GetMetadata())

	labels := append([]*peloton.Label{
		common.BridgePodLabel,
		jobKeyLabel,
	}, metadataLabels...)

	gpuLimit, err := label.GetUdeployGpuLimit(t.GetMetadata())
	if err != nil {
		return nil, fmt.Errorf("failed to parse udeploy gpu limit label: %s", err)
	}

	constraint, err := NewConstraint(jobKeyLabel, t.GetConstraints())
	if err != nil {
		return nil, fmt.Errorf("new constraint: %s", err)
	}

	volumes, volumeMounts := newVolumes(t.GetContainer())

	image, err := newImage(t.GetContainer())
	if err != nil {
		return nil, err
	}

	return &pod.PodSpec{
		PodName:        nil, // Unused.
		Labels:         labels,
		InitContainers: nil, // Unused.
		Containers: []*pod.ContainerSpec{{
			Name:           "", // Unused.
			Resource:       newResourceSpec(t.GetResources(), gpuLimit),
			LivenessCheck:  nil, // Unused,
			ReadinessCheck: nil, // Unused.
			Ports:          newPortSpecs(t.GetResources()),
			Entrypoint:     newEntryPoint(c),
			Image:          image,
			VolumeMounts:   volumeMounts,
		}},
		Constraint:             constraint,
		RestartPolicy:          nil,   // Unused.
		Volume:                 nil,   // Unused.
		PreemptionPolicy:       nil,   // Unused.
		Controller:             false, // Unused.
		KillGracePeriodSeconds: 0,     // Unused.
		Revocable:              t.GetTier() == common.Revocable,
		Volumes:                volumes,
		MesosSpec:              newMesosPodSpec(t.GetContainer(), c, executorData),
	}, nil
}

func newResourceSpec(rs []*api.Resource, gpuLimit *float64) *pod.ResourceSpec {
	if len(rs) == 0 {
		return nil
	}

	result := &pod.ResourceSpec{}
	for _, r := range rs {
		if r.IsSetNumCpus() {
			result.CpuLimit = r.GetNumCpus()
		}
		if r.IsSetRamMb() {
			result.MemLimitMb = float64(r.GetRamMb())
		}
		if r.IsSetDiskMb() {
			result.DiskLimitMb = float64(r.GetDiskMb())
		}
		if r.IsSetNumGpus() {
			result.GpuLimit = float64(r.GetNumGpus())
		}
		// Note: Aurora API does not include fd_limit.
	}

	if gpuLimit != nil {
		result.GpuLimit = *gpuLimit
	}

	return result
}

func newPortSpecs(rs []*api.Resource) []*pod.PortSpec {
	var result []*pod.PortSpec
	for _, r := range rs {
		if r.IsSetNamedPort() {
			result = append(result, &pod.PortSpec{Name: r.GetNamedPort()})
		}
	}
	return result
}

func newImage(c *api.Container) (string, error) {
	if c == nil {
		return "", nil
	}

	if c.IsSetMesos() {
		if !c.GetMesos().IsSetImage() {
			return "", nil
		}

		if c.GetMesos().GetImage().IsSetDocker() {
			return fmt.Sprintf("%s:%s",
				c.GetMesos().GetImage().GetDocker().GetName(),
				c.GetMesos().GetImage().GetDocker().GetTag(),
			), nil
		}
		return "", fmt.Errorf("invalid mesos image type appc")
	}

	if c.IsSetDocker() {
		return c.GetDocker().GetImage(), nil
	}

	return "", fmt.Errorf("only docker and mesos containerizers are supported")
}

func newEntryPoint(c config.ThermosExecutorConfig) *pod.CommandSpec {
	var b strings.Builder
	b.WriteString("${MESOS_SANDBOX=.}/")
	b.WriteString(path.Base(c.Path))
	b.WriteString(" ")
	b.WriteString(c.Flags)
	command := strings.TrimSpace(b.String())

	result := &pod.CommandSpec{
		Value: command,
	}
	return result
}

func newMesosPodSpec(
	c *api.Container,
	t config.ThermosExecutorConfig,
	executorData []byte,
) *apachemesos.PodSpec {
	if c == nil {
		return nil
	}

	result := &apachemesos.PodSpec{}
	if c.IsSetMesos() {
		result.Type = apachemesos.PodSpec_CONTAINER_TYPE_MESOS
	}

	if c.IsSetDocker() {
		result.Type = apachemesos.PodSpec_CONTAINER_TYPE_DOCKER
		result.DockerParameters = newDockerParameters(
			c.GetDocker().GetParameters())
		result.NetworkSpec = &apachemesos.PodSpec_NetworkSpec{
			Type: apachemesos.PodSpec_NetworkSpec_NETWORK_TYPE_HOST,
		}
	}

	// Fill the URI
	resourcesToFetch := []string{t.Path}
	if t.Resources != "" {
		resourcesToFetch = append(
			resourcesToFetch,
			strings.Split(t.Resources, config.ThermosExecutorDelimiter)...,
		)
	}

	var mesosUris []*apachemesos.PodSpec_URI
	for _, r := range resourcesToFetch {
		mesosUris = append(mesosUris, &apachemesos.PodSpec_URI{
			Value:      r,
			Executable: true,
			// Previously when we were using mesos.v1.CommandInfo.URI,
			// "extract" field was left as unfilled, however, since in
			// the proto definition the field is default to true, the
			// actual value we passed to mesos is actually true.
			//
			// In pod.apachemesos.PodSpec.URI, "extract" field can no
			// longer have a different value, in order to retain
			// the previous behavior and to avoid unnecessary instance
			// restarts, we are hard-code this value to true.
			Extract: true,
		})
	}
	result.Uris = mesosUris

	result.Shell = true

	result.ExecutorSpec = &apachemesos.PodSpec_ExecutorSpec{
		Type:       apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_CUSTOM,
		ExecutorId: config.ThermosExecutorIDPlaceholder,
		Data:       executorData,
	}

	if t.CPU > 0 || t.RAM > 0 {
		result.ExecutorSpec.Resources = &apachemesos.PodSpec_ExecutorSpec_Resources{
			Cpu:   t.CPU,
			MemMb: float64(t.RAM),
		}
	}

	return result
}

func newVolumes(c *api.Container) ([]*volume.VolumeSpec, []*pod.VolumeMount) {
	if c == nil {
		return nil, nil
	}

	var volumes []*volume.VolumeSpec
	var volumeMounts []*pod.VolumeMount

	vs := c.GetMesos().GetVolumes()
	for _, v := range vs {
		volume := &volume.VolumeSpec{
			Name: v.GetHostPath(),
			Type: volume.VolumeSpec_VOLUME_TYPE_HOST_PATH,
			HostPath: &volume.VolumeSpec_HostPathVolumeSource{
				Path: v.GetHostPath(),
			},
		}

		volumeMount := &pod.VolumeMount{
			Name:      v.GetHostPath(),
			ReadOnly:  isReadOnly(v.GetMode()),
			MountPath: v.GetContainerPath(),
		}

		volumes = append(volumes, volume)
		volumeMounts = append(volumeMounts, volumeMount)
	}
	return volumes, volumeMounts
}

func isReadOnly(mode api.Mode) bool {
	return mode == api.ModeRo
}

func newDockerParameters(ps []*api.DockerParameter) []*apachemesos.PodSpec_DockerParameter {
	var result []*apachemesos.PodSpec_DockerParameter
	for _, p := range ps {
		result = append(result, &apachemesos.PodSpec_DockerParameter{
			Key:   p.GetName(),
			Value: p.GetValue(),
		})
	}
	return result
}
