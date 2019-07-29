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

package ptoa

import (
	"fmt"
	"strings"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/apachemesos"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/aurorabridge/label"

	"go.uber.org/thriftrw/ptr"
)

// NewTaskConfig returns aurora task config for a provided peloton pod spec
func NewTaskConfig(
	jobSummary *stateless.JobSummary,
	podSpec *pod.PodSpec,
) (*api.TaskConfig, error) {
	if len(podSpec.GetContainers()) == 0 {
		return nil, fmt.Errorf("pod spec does not contain containers")
	}

	auroraTier := NewTaskTier(jobSummary.GetSla())
	auroraOwner := NewIdentity(jobSummary.GetOwner())

	var auroraPriority *int32
	if sla := jobSummary.GetSla(); sla != nil {
		auroraPriority = ptr.Int32(int32(sla.GetPriority()))
	}

	auroraJobKey, err := NewJobKey(jobSummary.GetName())
	if err != nil {
		return nil, err
	}

	auroraMetadata := label.ParseAuroraMetadata(podSpec.GetLabels())

	auroraContainer, err := newContainer(
		podSpec.GetContainers()[0],
		podSpec.GetMesosSpec(),
	)
	if err != nil {
		return nil, err
	}

	auroraConstraints, err := NewConstraints(podSpec.GetConstraint())
	if err != nil {
		return nil, fmt.Errorf("new constraints: %s", err)
	}

	var numCpus *float64
	var ramMb *int64
	var diskMb *int64
	var requestedPorts map[string]struct{}
	auroraResources := newResources(podSpec.GetContainers()[0])
	for _, r := range auroraResources {
		if r.IsSetNumCpus() {
			numCpus = r.NumCpus
		} else if r.IsSetRamMb() {
			ramMb = r.RamMb
		} else if r.IsSetDiskMb() {
			diskMb = r.DiskMb
		} else if r.IsSetNamedPort() {
			if requestedPorts == nil {
				requestedPorts = make(map[string]struct{})
			}
			requestedPorts[r.GetNamedPort()] = struct{}{}
		}
	}

	return &api.TaskConfig{
		Job:            auroraJobKey,
		Owner:          auroraOwner,
		IsService:      ptr.Bool(true),
		NumCpus:        numCpus,
		RamMb:          ramMb,
		DiskMb:         diskMb,
		RequestedPorts: requestedPorts,
		Tier:           auroraTier,
		Metadata:       auroraMetadata,
		Container:      auroraContainer,
		Resources:      auroraResources,
		Constraints:    auroraConstraints,
		Priority:       auroraPriority,
		//MaxTaskFailures:  nil,
		//MesosFetcherUris: nil,
		//TaskLinks:        map[string]string{},
		//ContactEmail:     nil,
		//ExecutorConfig: nil,
	}, nil
}

// newContainer creates a list of Resource objects.
func newResources(container *pod.ContainerSpec) []*api.Resource {
	var resources []*api.Resource
	if cpuLimit := container.GetResource().GetCpuLimit(); cpuLimit > 0 {
		resources = append(resources, &api.Resource{
			NumCpus: ptr.Float64(cpuLimit),
		})
	}
	if memLimitMb := container.GetResource().GetMemLimitMb(); memLimitMb > 0 {
		resources = append(resources, &api.Resource{
			RamMb: ptr.Int64(int64(memLimitMb)),
		})
	}
	if diskLimitMb := container.GetResource().GetDiskLimitMb(); diskLimitMb > 0 {
		resources = append(resources, &api.Resource{
			DiskMb: ptr.Int64(int64(diskLimitMb)),
		})
	}
	if gpuLimit := container.GetResource().GetGpuLimit(); gpuLimit > 0 {
		resources = append(resources, &api.Resource{
			NumGpus: ptr.Int64(int64(gpuLimit)),
		})
	}
	for _, port := range container.GetPorts() {
		resources = append(resources, &api.Resource{
			NamedPort: ptr.String(port.GetName()),
		})
	}
	return resources
}

// newContainer creates a Container object.
func newContainer(
	container *pod.ContainerSpec,
	mesosPodSpec *apachemesos.PodSpec,
) (*api.Container, error) {
	if mesosPodSpec == nil || container == nil {
		return nil, nil
	}

	var mesosContainer *api.MesosContainer
	var dockerContainer *api.DockerContainer

	switch mesosPodSpec.GetType() {
	case apachemesos.PodSpec_CONTAINER_TYPE_MESOS:
		var err error
		mesosContainer, err = newMesosContainer(
			container.GetImage(),
			container.GetVolumeMounts(),
		)
		if err != nil {
			return nil, err
		}
	case apachemesos.PodSpec_CONTAINER_TYPE_DOCKER:
		dockerContainer = newDockerContainer(
			container.GetImage(),
			mesosPodSpec.GetDockerParameters(),
		)
	}

	return &api.Container{
		Mesos:  mesosContainer,
		Docker: dockerContainer,
	}, nil
}

// newMesosContainer creates a MesosContainer object.
func newMesosContainer(
	image string,
	volumeMounts []*pod.VolumeMount,
) (*api.MesosContainer, error) {
	var avs []*api.Volume
	for _, v := range volumeMounts {
		avs = append(avs, &api.Volume{
			ContainerPath: ptr.String(v.GetMountPath()),
			// Host path is the same as the name
			HostPath: ptr.String(v.GetName()),
			Mode:     newMode(v.GetReadOnly()),
		})
	}

	i, err := newImage(image)
	if err != nil {
		return nil, err
	}

	return &api.MesosContainer{
		Image:   i,
		Volumes: avs,
	}, nil
}

func newImage(image string) (*api.Image, error) {
	if len(image) == 0 {
		return nil, nil
	}

	var docker *api.DockerImage

	// assuming the image name in <repository>:<tag> form
	n := image
	ns := strings.Split(n, ":")
	if len(ns) < 2 {
		return nil, fmt.Errorf(
			"invalid docker image %q: expected <repo>:<tag>", n)
	}
	tag := ns[len(ns)-1]
	repo := strings.Join(ns[:len(ns)-1], ":")

	docker = &api.DockerImage{
		Name: ptr.String(repo),
		Tag:  ptr.String(tag),
	}

	return &api.Image{
		Docker: docker,
	}, nil
}

// newMode converts readOnly boolean to aurora Mode enum.
func newMode(readOnly bool) *api.Mode {
	if readOnly {
		return api.ModeRo.Ptr()
	}
	return api.ModeRw.Ptr()
}

// newDockerContainer create a DockerContainer object.
func newDockerContainer(
	image string,
	parameters []*apachemesos.PodSpec_DockerParameter,
) *api.DockerContainer {
	var aps []*api.DockerParameter
	for _, p := range parameters {
		aps = append(aps, &api.DockerParameter{
			Name:  ptr.String(p.GetKey()),
			Value: ptr.String(p.GetValue()),
		})
	}

	return &api.DockerContainer{
		Image:      ptr.String(image),
		Parameters: aps,
	}
}
