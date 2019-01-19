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

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/aurorabridge/label"

	"go.uber.org/thriftrw/ptr"
)

// NewTaskConfig returns aurora task config for a provided peloton pod spec
func NewTaskConfig(
	jobInfo *stateless.JobInfo,
	podSpec *pod.PodSpec,
) (*api.TaskConfig, error) {
	auroraTier := NewTaskTier(jobInfo.GetSpec().GetSla())

	auroraJobKey, err := NewJobKey(jobInfo.GetSpec().GetName())
	if err != nil {
		return nil, err
	}

	// TODO(kevinxu): make metadata optional?
	auroraMetadata, err := label.ParseAuroraMetadata(podSpec.GetLabels())
	if err != nil {
		return nil, fmt.Errorf("parse aurora metadata: %s", err)
	}

	var auroraContainer *api.Container
	if len(podSpec.GetContainers()) > 0 {
		auroraContainer, err = newContainer(podSpec.GetContainers()[0])
		if err != nil {
			return nil, err
		}
	}

	return &api.TaskConfig{
		Job:       auroraJobKey,
		IsService: ptr.Bool(true),
		Tier:      auroraTier,
		Metadata:  auroraMetadata,
		Container: auroraContainer,
		//Owner:            nil,
		//NumCpus:          nil,
		//RamMb:            nil,
		//DiskMb:           nil,
		//Priority:         nil,
		//MaxTaskFailures:  nil,
		//Resources:        nil,
		//Constraints:      nil,
		//RequestedPorts:   map[string]struct{}{},
		//MesosFetcherUris: nil,
		//TaskLinks:        map[string]string{},
		//ContactEmail:     nil,
		//ExecutorConfig: nil,
	}, nil
}

// newContainer creates a Container object.
func newContainer(container *pod.ContainerSpec) (*api.Container, error) {
	if container.GetContainer() == nil {
		return nil, nil
	}

	var mesosContainer *api.MesosContainer
	var dockerContainer *api.DockerContainer

	switch container.GetContainer().GetType() {
	case mesos.ContainerInfo_MESOS:
		var err error
		mesosContainer, err = newMesosContainer(
			container.GetContainer().GetMesos(),
			container.GetContainer().GetVolumes())
		if err != nil {
			return nil, err
		}
	case mesos.ContainerInfo_DOCKER:
		dockerContainer = newDockerContainer(
			container.GetContainer().GetDocker())
	}

	return &api.Container{
		Mesos:  mesosContainer,
		Docker: dockerContainer,
	}, nil
}

// newMesosContainer creates a MesosContainer object.
func newMesosContainer(
	mesosInfo *mesos.ContainerInfo_MesosInfo,
	volumes []*mesos.Volume,
) (*api.MesosContainer, error) {
	if mesosInfo == nil {
		return nil, nil
	}

	var avs []*api.Volume
	for _, v := range volumes {
		avs = append(avs, &api.Volume{
			ContainerPath: ptr.String(v.GetContainerPath()),
			HostPath:      ptr.String(v.GetHostPath()),
			Mode:          newMode(v.GetMode()),
		})
	}

	i, err := newImage(mesosInfo.GetImage())
	if err != nil {
		return nil, err
	}

	return &api.MesosContainer{
		Image:   i,
		Volumes: avs,
	}, nil
}

func newImage(image *mesos.Image) (*api.Image, error) {
	if image == nil {
		return nil, nil
	}

	var docker *api.DockerImage
	var appc *api.AppcImage

	switch image.GetType() {
	case mesos.Image_DOCKER:
		// assuming the image name in <repository>:<tag> form
		n := image.GetDocker().GetName()
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
	case mesos.Image_APPC:
		appc = &api.AppcImage{
			Name:    ptr.String(image.GetAppc().GetName()),
			ImageId: ptr.String(image.GetAppc().GetId()),
		}
	}

	return &api.Image{
		Docker: docker,
		Appc:   appc,
	}, nil
}

// newMode converts mesos.Volume.Mode enum to aurora Mode enum.
func newMode(m mesos.Volume_Mode) *api.Mode {
	switch m {
	case mesos.Volume_RW:
		return api.ModeRw.Ptr()
	case mesos.Volume_RO:
		return api.ModeRo.Ptr()
	}
	return api.ModeRw.Ptr()
}

// newDockerContainer create a DockerContainer object.
func newDockerContainer(
	dockerInfo *mesos.ContainerInfo_DockerInfo,
) *api.DockerContainer {
	if dockerInfo == nil {
		return nil
	}

	var aps []*api.DockerParameter
	for _, p := range dockerInfo.GetParameters() {
		aps = append(aps, &api.DockerParameter{
			Name:  ptr.String(p.GetKey()),
			Value: ptr.String(p.GetValue()),
		})
	}

	return &api.DockerContainer{
		Image:      ptr.String(dockerInfo.GetImage()),
		Parameters: aps,
	}
}
