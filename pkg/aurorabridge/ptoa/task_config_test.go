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
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/apachemesos"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/aurorabridge/atop"
	"github.com/uber/peloton/pkg/aurorabridge/common"
	"github.com/uber/peloton/pkg/aurorabridge/fixture"
	"github.com/uber/peloton/pkg/aurorabridge/label"

	"github.com/stretchr/testify/assert"
	"go.uber.org/thriftrw/ptr"
)

func TestNewTaskConfig(t *testing.T) {
	jobKey := fixture.AuroraJobKey()
	metadata := fixture.AuroraMetadata()

	ml := label.NewAuroraMetadataLabels(metadata)

	j := &stateless.JobSummary{
		Name: atop.NewJobName(jobKey),
		Sla: &stateless.SlaSpec{
			Priority: 6,
		},
		Owner: "owner",
	}
	p := &pod.PodSpec{
		Labels:     ml,
		Containers: []*pod.ContainerSpec{{}},
	}

	c, err := NewTaskConfig(j, p)
	assert.NoError(t, err)
	assert.Equal(t, &api.TaskConfig{
		Job:       jobKey,
		Owner:     &api.Identity{User: ptr.String("owner")},
		IsService: ptr.Bool(true),
		Tier:      ptr.String(common.Preemptible),
		Metadata:  metadata,
		Priority:  ptr.Int32(6),
	}, c)
}

func TestNewResources(t *testing.T) {
	c := &pod.ContainerSpec{
		Resource: &pod.ResourceSpec{
			CpuLimit:    1.5,
			MemLimitMb:  1024,
			DiskLimitMb: 128,
			GpuLimit:    2.0,
		},
		Ports: []*pod.PortSpec{
			{
				Name:  "http",
				Value: 12345,
			},
			{
				Name:  "tchannel",
				Value: 54321,
			},
		},
	}

	r := newResources(c)
	assert.Equal(t, []*api.Resource{
		{NumCpus: ptr.Float64(1.5)},
		{RamMb: ptr.Int64(1024)},
		{DiskMb: ptr.Int64(128)},
		{NumGpus: ptr.Int64(2)},
		{NamedPort: ptr.String("http")},
		{NamedPort: ptr.String("tchannel")},
	}, r)
}

func TestNewContainer_Mesos(t *testing.T) {
	c := &pod.ContainerSpec{
		Image: "127.0.0.1:5055/test-image:test-tag",
		VolumeMounts: []*pod.VolumeMount{
			{
				MountPath: "/container-path-1",
				Name:      "/host-path-1",
				ReadOnly:  false,
			},
			{
				MountPath: "/container-path-2",
				Name:      "/host-path-2",
				ReadOnly:  true,
			},
		},
	}

	mp := &apachemesos.PodSpec{
		Type: apachemesos.PodSpec_CONTAINER_TYPE_MESOS,
	}

	ac, err := newContainer(c, mp)
	assert.NoError(t, err)
	assert.Equal(t, &api.Container{
		Mesos: &api.MesosContainer{
			Image: &api.Image{
				Docker: &api.DockerImage{
					Name: ptr.String("127.0.0.1:5055/test-image"),
					Tag:  ptr.String("test-tag"),
				},
			},
			Volumes: []*api.Volume{
				{
					ContainerPath: ptr.String("/container-path-1"),
					HostPath:      ptr.String("/host-path-1"),
					Mode:          api.ModeRw.Ptr(),
				},
				{
					ContainerPath: ptr.String("/container-path-2"),
					HostPath:      ptr.String("/host-path-2"),
					Mode:          api.ModeRo.Ptr(),
				},
			},
		},
	}, ac)
}

func TestNewContainer_Docker(t *testing.T) {
	c := &pod.ContainerSpec{
		Image: "127.0.0.1:5055/test-image:test-tag",
	}

	mp := &apachemesos.PodSpec{
		Type: apachemesos.PodSpec_CONTAINER_TYPE_DOCKER,
		DockerParameters: []*apachemesos.PodSpec_DockerParameter{
			{
				Key:   "p1",
				Value: "v1",
			},
		},
	}

	ac, err := newContainer(c, mp)
	assert.NoError(t, err)
	assert.Equal(t, &api.Container{
		Docker: &api.DockerContainer{
			Image: ptr.String("127.0.0.1:5055/test-image:test-tag"),
			Parameters: []*api.DockerParameter{
				{
					Name:  ptr.String("p1"),
					Value: ptr.String("v1"),
				},
			},
		},
	}, ac)
}
