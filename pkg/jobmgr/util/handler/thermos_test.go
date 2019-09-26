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

package handler

import (
	"sort"
	"testing"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/apachemesos"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/volume"
	aurora "github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/common/config"
	"github.com/uber/peloton/pkg/common/thermos"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/thriftrw/ptr"
)

func TestPortSpecByName(t *testing.T) {
	ports := []*pod.PortSpec{
		{Name: "b"},
		{Name: "c"},
		{Name: "b"},
		{Name: "a"},
	}

	sort.Stable(portSpecByName(ports))
	assert.Equal(t, []*pod.PortSpec{
		{Name: "a"},
		{Name: "b"},
		{Name: "b"},
		{Name: "c"},
	}, ports)
}

func TestRequiresThermosConvert(t *testing.T) {
	testCases := []struct {
		name        string
		spec        *pod.PodSpec
		wantConvert bool
		wantErr     string
	}{
		{
			name:        "expect no convert, nil pod spec",
			spec:        nil,
			wantConvert: false,
		},
		{
			name:        "expect no convert, no containers",
			spec:        &pod.PodSpec{PodName: &peloton.PodName{Value: "test-pod"}},
			wantConvert: false,
		},
		{
			name: "expect no convert, non-custom executor",
			spec: &pod.PodSpec{
				Containers: []*pod.ContainerSpec{{}},
				MesosSpec: &apachemesos.PodSpec{
					ExecutorSpec: &apachemesos.PodSpec_ExecutorSpec{
						Type: apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_DEFAULT,
					},
				},
			},
			wantConvert: false,
		},
		{
			name: "expect no convert, custom executor with data",
			spec: &pod.PodSpec{
				Containers: []*pod.ContainerSpec{{}},
				MesosSpec: &apachemesos.PodSpec{
					ExecutorSpec: &apachemesos.PodSpec_ExecutorSpec{
						Type: apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_CUSTOM,
						Data: []byte{1, 2, 3, 4, 5},
					},
				},
			},
			wantConvert: false,
		},
		{
			name: "expect error, missing container image",
			spec: &pod.PodSpec{
				Containers: []*pod.ContainerSpec{{}},
				MesosSpec: &apachemesos.PodSpec{
					ExecutorSpec: &apachemesos.PodSpec_ExecutorSpec{
						Type: apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_CUSTOM,
					},
				},
			},
			wantErr: "container image must be defined",
		},
		{
			name: "expect error, missing container name",
			spec: &pod.PodSpec{
				Containers: []*pod.ContainerSpec{
					{
						Image: "docker-image",
					},
				},
				MesosSpec: &apachemesos.PodSpec{
					ExecutorSpec: &apachemesos.PodSpec_ExecutorSpec{
						Type: apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_CUSTOM,
					},
				},
			},
			wantErr: "container does not have name specified",
		},
		{
			name: "expect error, duplicate container names",
			spec: &pod.PodSpec{
				Containers: []*pod.ContainerSpec{
					{
						Name:  "container-0",
						Image: "docker-image",
					},
					{
						Name:  "container-0",
						Image: "docker-image",
					},
				},
				MesosSpec: &apachemesos.PodSpec{
					ExecutorSpec: &apachemesos.PodSpec_ExecutorSpec{
						Type: apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_CUSTOM,
					},
				},
			},
			wantErr: "duplicate name found in container names",
		},
		{
			name: "expect error, missing volume name",
			spec: &pod.PodSpec{
				Containers: []*pod.ContainerSpec{
					{
						Name:  "container-0",
						Image: "docker-image",
					},
				},
				Volumes: []*volume.VolumeSpec{
					{
						Name: "",
					},
				},
				MesosSpec: &apachemesos.PodSpec{
					ExecutorSpec: &apachemesos.PodSpec_ExecutorSpec{
						Type: apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_CUSTOM,
					},
				},
			},
			wantErr: "volume does not have name specified",
		},
		{
			name: "expect error, duplicate volume names",
			spec: &pod.PodSpec{
				Containers: []*pod.ContainerSpec{
					{
						Name:  "container-0",
						Image: "docker-image",
					},
				},
				Volumes: []*volume.VolumeSpec{
					{
						Name: "test-volume",
						Type: volume.VolumeSpec_VOLUME_TYPE_HOST_PATH,
						HostPath: &volume.VolumeSpec_HostPathVolumeSource{
							Path: "/tmp",
						},
					},
					{
						Name: "test-volume",
						Type: volume.VolumeSpec_VOLUME_TYPE_HOST_PATH,
						HostPath: &volume.VolumeSpec_HostPathVolumeSource{
							Path: "/tmp",
						},
					},
				},
				MesosSpec: &apachemesos.PodSpec{
					ExecutorSpec: &apachemesos.PodSpec_ExecutorSpec{
						Type: apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_CUSTOM,
					},
				},
			},
			wantErr: "duplicate volume name found in pod",
		},
		{
			name: "expect error, invalid volume type",
			spec: &pod.PodSpec{
				Containers: []*pod.ContainerSpec{
					{
						Name:  "container-0",
						Image: "docker-image",
					},
				},
				Volumes: []*volume.VolumeSpec{
					{
						Name: "test-volume",
					},
				},
				MesosSpec: &apachemesos.PodSpec{
					ExecutorSpec: &apachemesos.PodSpec_ExecutorSpec{
						Type: apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_CUSTOM,
					},
				},
			},
			wantErr: "invalid volume type for volume",
		},
		{
			name: "expect error, empty path for host_path volume",
			spec: &pod.PodSpec{
				Containers: []*pod.ContainerSpec{
					{
						Name:  "container-0",
						Image: "docker-image",
					},
				},
				Volumes: []*volume.VolumeSpec{
					{
						Name: "test-volume",
						Type: volume.VolumeSpec_VOLUME_TYPE_HOST_PATH,
					},
				},
				MesosSpec: &apachemesos.PodSpec{
					ExecutorSpec: &apachemesos.PodSpec_ExecutorSpec{
						Type: apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_CUSTOM,
					},
				},
			},
			wantErr: "path is empty for host_path volume",
		},
		{
			name: "expect error, volume mount name not defined",
			spec: &pod.PodSpec{
				Containers: []*pod.ContainerSpec{
					{
						Name:  "container-0",
						Image: "docker-image",
						VolumeMounts: []*pod.VolumeMount{
							{
								Name: "",
							},
						},
					},
				},
				Volumes: []*volume.VolumeSpec{
					{
						Name: "test-volume",
						Type: volume.VolumeSpec_VOLUME_TYPE_HOST_PATH,
						HostPath: &volume.VolumeSpec_HostPathVolumeSource{
							Path: "/tmp",
						},
					},
				},
				MesosSpec: &apachemesos.PodSpec{
					ExecutorSpec: &apachemesos.PodSpec_ExecutorSpec{
						Type: apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_CUSTOM,
					},
				},
			},
			wantErr: "volume mount does not specify volume name",
		},
		{
			name: "expect error, volume mount mount path not defined",
			spec: &pod.PodSpec{
				Containers: []*pod.ContainerSpec{
					{
						Name:  "container-0",
						Image: "docker-image",
						VolumeMounts: []*pod.VolumeMount{
							{
								Name: "test-volume",
							},
						},
					},
				},
				Volumes: []*volume.VolumeSpec{
					{
						Name: "test-volume",
						Type: volume.VolumeSpec_VOLUME_TYPE_HOST_PATH,
						HostPath: &volume.VolumeSpec_HostPathVolumeSource{
							Path: "/tmp",
						},
					},
				},
				MesosSpec: &apachemesos.PodSpec{
					ExecutorSpec: &apachemesos.PodSpec_ExecutorSpec{
						Type: apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_CUSTOM,
					},
				},
			},
			wantErr: "volume mount does not specify mount path",
		},
		{
			name: "expect error, undefined volume",
			spec: &pod.PodSpec{
				Containers: []*pod.ContainerSpec{
					{
						Name:  "container-0",
						Image: "docker-image",
						VolumeMounts: []*pod.VolumeMount{
							{
								Name:      "test-volume-2",
								MountPath: "/tmp",
							},
						},
					},
				},
				Volumes: []*volume.VolumeSpec{
					{
						Name: "test-volume",
						Type: volume.VolumeSpec_VOLUME_TYPE_HOST_PATH,
						HostPath: &volume.VolumeSpec_HostPathVolumeSource{
							Path: "/tmp",
						},
					},
				},
				MesosSpec: &apachemesos.PodSpec{
					ExecutorSpec: &apachemesos.PodSpec_ExecutorSpec{
						Type: apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_CUSTOM,
					},
				},
			},
			wantErr: "volume not defined: test-volume-2",
		},
		{
			name: "expect error, invalid environment",
			spec: &pod.PodSpec{
				Containers: []*pod.ContainerSpec{
					{
						Name:        "container-0",
						Image:       "docker-image",
						Environment: []*pod.Environment{{}},
					},
				},
				MesosSpec: &apachemesos.PodSpec{
					ExecutorSpec: &apachemesos.PodSpec_ExecutorSpec{
						Type: apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_CUSTOM,
					},
				},
			},
			wantErr: "environment variable name not defined",
		},
		{
			name: "expect error, duplicate environment name",
			spec: &pod.PodSpec{
				InitContainers: []*pod.ContainerSpec{
					{
						Name: "initcontainer-0",
						Environment: []*pod.Environment{
							{
								Name:  "env3",
								Value: "value3",
							},
							{
								Name:  "env1",
								Value: "value1",
							},
						},
					},
				},
				Containers: []*pod.ContainerSpec{
					{
						Name:  "container-0",
						Image: "docker-image",
						Environment: []*pod.Environment{
							{
								Name:  "env1",
								Value: "value1",
							},
							{
								Name:  "env2",
								Value: "value2",
							},
						},
					},
				},
				MesosSpec: &apachemesos.PodSpec{
					ExecutorSpec: &apachemesos.PodSpec_ExecutorSpec{
						Type: apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_CUSTOM,
					},
				},
			},
			wantErr: "duplicate environment variable not allowed",
		},
		{
			name: "expect error, duplicate volume mount",
			spec: &pod.PodSpec{
				InitContainers: []*pod.ContainerSpec{
					{
						Name: "initcontainer-0",
						VolumeMounts: []*pod.VolumeMount{
							{
								Name:      "mount1",
								MountPath: "/tmp/mount1",
							},
						},
					},
				},
				Containers: []*pod.ContainerSpec{
					{
						Name:  "container-0",
						Image: "docker-image",
						VolumeMounts: []*pod.VolumeMount{
							{
								Name:      "mount1",
								MountPath: "/tmp/mount1",
							},
							{
								Name:      "mount2",
								MountPath: "/tmp/mount2",
							},
						},
					},
				},
				Volumes: []*volume.VolumeSpec{
					{
						Name: "mount1",
						Type: volume.VolumeSpec_VOLUME_TYPE_HOST_PATH,
						HostPath: &volume.VolumeSpec_HostPathVolumeSource{
							Path: "/tmp",
						},
					},
					{
						Name: "mount2",
						Type: volume.VolumeSpec_VOLUME_TYPE_HOST_PATH,
						HostPath: &volume.VolumeSpec_HostPathVolumeSource{
							Path: "/tmp",
						},
					},
				},
				MesosSpec: &apachemesos.PodSpec{
					ExecutorSpec: &apachemesos.PodSpec_ExecutorSpec{
						Type: apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_CUSTOM,
					},
				},
			},
			wantErr: "duplicate volume mount not allowed",
		},
		{
			name: "expect convert, custom executor empty data",
			spec: &pod.PodSpec{
				Containers: []*pod.ContainerSpec{
					{
						Name:  "container-0",
						Image: "docker-image",
						VolumeMounts: []*pod.VolumeMount{
							{
								Name:      "volume-2",
								MountPath: "/tmp",
							},
						},
						Environment: []*pod.Environment{
							{
								Name:  "test-env",
								Value: "test-value",
							},
							{
								Name:  "empty-env",
								Value: "",
							},
						},
					},
				},
				Volumes: []*volume.VolumeSpec{
					{
						Name: "volume-1",
						Type: volume.VolumeSpec_VOLUME_TYPE_HOST_PATH,
						HostPath: &volume.VolumeSpec_HostPathVolumeSource{
							Path: "/tmp/hostpath1",
						},
					},
					{
						Name: "volume-2",
						Type: volume.VolumeSpec_VOLUME_TYPE_HOST_PATH,
						HostPath: &volume.VolumeSpec_HostPathVolumeSource{
							Path: "/tmp/hostpath2",
						},
					},
				},
				MesosSpec: &apachemesos.PodSpec{
					ExecutorSpec: &apachemesos.PodSpec_ExecutorSpec{
						Type: apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_CUSTOM,
					},
				},
			},
			wantConvert: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			convert, err := requiresThermosConvert(tc.spec)
			require.Equal(t, tc.wantConvert, convert)
			if len(tc.wantErr) > 0 {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCollectResources(t *testing.T) {
	testCases := []struct {
		name          string
		podSpec       *pod.PodSpec
		wantResources *pod.ResourceSpec
		wantPorts     []*pod.PortSpec
	}{
		{
			name:          "empty container",
			podSpec:       &pod.PodSpec{},
			wantResources: &pod.ResourceSpec{},
			wantPorts:     []*pod.PortSpec{},
		},
		{
			name: "empty init container, single container",
			podSpec: &pod.PodSpec{
				Containers: []*pod.ContainerSpec{
					{
						Resource: &pod.ResourceSpec{
							CpuLimit:    1.1,
							MemLimitMb:  1.2,
							DiskLimitMb: 1.3,
							FdLimit:     2,
							GpuLimit:    1.4,
						},
						Ports: []*pod.PortSpec{
							{
								Name: "port1",
							},
							{
								Name: "port2",
							},
						},
					},
				},
			},
			wantResources: &pod.ResourceSpec{
				CpuLimit:    1.1,
				MemLimitMb:  1.2,
				DiskLimitMb: 1.3,
				FdLimit:     2,
				GpuLimit:    1.4,
			},
			wantPorts: []*pod.PortSpec{
				{
					Name: "port1",
				},
				{
					Name: "port2",
				},
			},
		},
		{
			name: "empty init container, multiple containers",
			podSpec: &pod.PodSpec{
				Containers: []*pod.ContainerSpec{
					{
						Resource: &pod.ResourceSpec{
							CpuLimit:    1.1,
							MemLimitMb:  1.2,
							DiskLimitMb: 1.3,
							FdLimit:     2,
							GpuLimit:    1.4,
						},
						Ports: []*pod.PortSpec{
							{
								Name: "port1",
							},
							{
								Name: "port2",
							},
						},
					},
					{
						Resource: &pod.ResourceSpec{
							CpuLimit:    1.9,
							MemLimitMb:  1.8,
							DiskLimitMb: 1.7,
							FdLimit:     3,
							GpuLimit:    1.6,
						},
						Ports: []*pod.PortSpec{
							{
								Name: "port3",
							},
							{
								Name: "port1",
							},
						},
					},
				},
			},
			wantResources: &pod.ResourceSpec{
				CpuLimit:    3,
				MemLimitMb:  3,
				DiskLimitMb: 3,
				FdLimit:     5,
				GpuLimit:    3,
			},
			wantPorts: []*pod.PortSpec{
				{
					Name: "port1",
				},
				{
					Name: "port2",
				},
				{
					Name: "port3",
				},
			},
		},
		{
			name: "max init container resources less than container resources",
			podSpec: &pod.PodSpec{
				InitContainers: []*pod.ContainerSpec{
					{
						Resource: &pod.ResourceSpec{
							CpuLimit:    2,
							MemLimitMb:  2,
							DiskLimitMb: 2,
							FdLimit:     3,
							GpuLimit:    2,
						},
						Ports: []*pod.PortSpec{
							{
								Name: "port1",
							},
							{
								Name: "port2",
							},
						},
					},
					{
						Resource: &pod.ResourceSpec{
							CpuLimit:    2,
							MemLimitMb:  2,
							DiskLimitMb: 2,
							FdLimit:     3,
							GpuLimit:    2,
						},
						Ports: []*pod.PortSpec{
							{
								Name: "port1",
							},
							{
								Name: "port2",
							},
						},
					},
				},
				Containers: []*pod.ContainerSpec{
					{
						Resource: &pod.ResourceSpec{
							CpuLimit:    1.1,
							MemLimitMb:  1.2,
							DiskLimitMb: 1.3,
							FdLimit:     2,
							GpuLimit:    1.4,
						},
						Ports: []*pod.PortSpec{
							{
								Name: "port1",
							},
							{
								Name: "port2",
							},
						},
					},
					{
						Resource: &pod.ResourceSpec{
							CpuLimit:    1.9,
							MemLimitMb:  1.8,
							DiskLimitMb: 1.7,
							FdLimit:     3,
							GpuLimit:    1.6,
						},
						Ports: []*pod.PortSpec{
							{
								Name: "port3",
							},
							{
								Name: "port1",
							},
						},
					},
				},
			},
			wantResources: &pod.ResourceSpec{
				CpuLimit:    3,
				MemLimitMb:  3,
				DiskLimitMb: 3,
				FdLimit:     5,
				GpuLimit:    3,
			},
			wantPorts: []*pod.PortSpec{
				{
					Name: "port1",
				},
				{
					Name: "port2",
				},
				{
					Name: "port3",
				},
			},
		},
		{
			name: "max init container resources greater than container resources",
			podSpec: &pod.PodSpec{
				InitContainers: []*pod.ContainerSpec{
					{
						Resource: &pod.ResourceSpec{
							CpuLimit:    4,
							MemLimitMb:  4,
							DiskLimitMb: 4,
							FdLimit:     6,
							GpuLimit:    4,
						},
						Ports: []*pod.PortSpec{
							{
								Name: "port1",
							},
							{
								Name: "port2",
							},
						},
					},
					{
						Resource: &pod.ResourceSpec{
							CpuLimit:    2,
							MemLimitMb:  2,
							DiskLimitMb: 2,
							FdLimit:     3,
							GpuLimit:    2,
						},
						Ports: []*pod.PortSpec{
							{
								Name: "port1",
							},
							{
								Name: "port2",
							},
						},
					},
				},
				Containers: []*pod.ContainerSpec{
					{
						Resource: &pod.ResourceSpec{
							CpuLimit:    1.1,
							MemLimitMb:  1.2,
							DiskLimitMb: 1.3,
							FdLimit:     2,
							GpuLimit:    1.4,
						},
						Ports: []*pod.PortSpec{
							{
								Name: "port1",
							},
							{
								Name: "port2",
							},
						},
					},
					{
						Resource: &pod.ResourceSpec{
							CpuLimit:    1.9,
							MemLimitMb:  1.8,
							DiskLimitMb: 1.7,
							FdLimit:     3,
							GpuLimit:    1.6,
						},
						Ports: []*pod.PortSpec{
							{
								Name: "port3",
							},
							{
								Name: "port1",
							},
						},
					},
				},
			},
			wantResources: &pod.ResourceSpec{
				CpuLimit:    4,
				MemLimitMb:  4,
				DiskLimitMb: 4,
				FdLimit:     6,
				GpuLimit:    4,
			},
			wantPorts: []*pod.PortSpec{
				{
					Name: "port1",
				},
				{
					Name: "port2",
				},
				{
					Name: "port3",
				},
			},
		},
		{
			name: "max init container resources greater than some of container resources",
			podSpec: &pod.PodSpec{
				InitContainers: []*pod.ContainerSpec{
					{
						Resource: &pod.ResourceSpec{
							CpuLimit:    4,
							MemLimitMb:  4,
							DiskLimitMb: 2,
							FdLimit:     3,
							GpuLimit:    4,
						},
						Ports: []*pod.PortSpec{
							{
								Name: "port1",
							},
							{
								Name: "port2",
							},
						},
					},
					{
						Resource: &pod.ResourceSpec{
							CpuLimit:    2,
							MemLimitMb:  2,
							DiskLimitMb: 2,
							FdLimit:     3,
							GpuLimit:    2,
						},
						Ports: []*pod.PortSpec{
							{
								Name: "port1",
							},
							{
								Name: "port2",
							},
							{
								Name: "port4",
							},
						},
					},
				},
				Containers: []*pod.ContainerSpec{
					{
						Resource: &pod.ResourceSpec{
							CpuLimit:    1.1,
							MemLimitMb:  1.2,
							DiskLimitMb: 1.3,
							FdLimit:     2,
							GpuLimit:    1.4,
						},
						Ports: []*pod.PortSpec{
							{
								Name: "port1",
							},
							{
								Name: "port2",
							},
						},
					},
					{
						Resource: &pod.ResourceSpec{
							CpuLimit:    1.9,
							MemLimitMb:  1.8,
							DiskLimitMb: 1.7,
							FdLimit:     3,
							GpuLimit:    1.6,
						},
						Ports: []*pod.PortSpec{
							{
								Name: "port3",
							},
							{
								Name: "port1",
							},
						},
					},
				},
			},
			wantResources: &pod.ResourceSpec{
				CpuLimit:    4,
				MemLimitMb:  4,
				DiskLimitMb: 3,
				FdLimit:     5,
				GpuLimit:    4,
			},
			wantPorts: []*pod.PortSpec{
				{
					Name: "port1",
				},
				{
					Name: "port2",
				},
				{
					Name: "port3",
				},
				{
					Name: "port4",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res, ports := collectResources(tc.podSpec)
			assert.Equal(t, tc.wantResources, res)

			// Compare we have the correct port names after sorting
			portNames := make([]string, 0, len(ports))
			for _, p := range ports {
				portNames = append(portNames, p.GetName())
			}

			wantPortNames := make([]string, 0, len(tc.wantPorts))
			for _, p := range tc.wantPorts {
				wantPortNames = append(wantPortNames, p.GetName())
			}

			sort.Strings(portNames)
			sort.Strings(wantPortNames)

			assert.Equal(t, wantPortNames, portNames)
		})
	}
}

func TestCreateDockerInfo(t *testing.T) {
	testCases := []struct {
		name              string
		podSpec           *pod.PodSpec
		wantContainerInfo *mesos.ContainerInfo_DockerInfo
	}{
		{
			name: "image name only",
			podSpec: &pod.PodSpec{
				Containers: []*pod.ContainerSpec{
					{
						Image: "docker-image",
					},
				},
			},
			wantContainerInfo: &mesos.ContainerInfo_DockerInfo{
				Image: ptr.String("docker-image"),
			},
		},
		{
			name: "image name and environment variables",
			podSpec: &pod.PodSpec{
				Containers: []*pod.ContainerSpec{
					{
						Image: "docker-image",
						Environment: []*pod.Environment{
							{
								Name:  "env-1",
								Value: "value-1",
							},
							{
								Name:  "env-2",
								Value: "value-2",
							},
						},
					},
				},
			},
			wantContainerInfo: &mesos.ContainerInfo_DockerInfo{
				Image: ptr.String("docker-image"),
				Parameters: []*mesos.Parameter{
					{
						Key:   ptr.String("env"),
						Value: ptr.String("env-1=value-1"),
					},
					{
						Key:   ptr.String("env"),
						Value: ptr.String("env-2=value-2"),
					},
				},
			},
		},
		{
			name: "image name, environment variables and volume mounts",
			podSpec: &pod.PodSpec{
				Containers: []*pod.ContainerSpec{
					{
						Image: "docker-image",
						Environment: []*pod.Environment{
							{
								Name:  "env-1",
								Value: "value-1",
							},
							{
								Name:  "env-2",
								Value: "value-2",
							},
						},
						VolumeMounts: []*pod.VolumeMount{
							{
								Name:      "etc",
								ReadOnly:  true,
								MountPath: "/mnt/etc",
							},
						},
					},
					{
						Environment: []*pod.Environment{
							{
								Name:  "env-3",
								Value: "value-3",
							},
						},
						VolumeMounts: []*pod.VolumeMount{
							{
								Name:      "tmp",
								MountPath: "/mnt/tmp",
							},
						},
					},
				},
				Volumes: []*volume.VolumeSpec{
					{
						Name: "tmp",
						Type: volume.VolumeSpec_VOLUME_TYPE_HOST_PATH,
						HostPath: &volume.VolumeSpec_HostPathVolumeSource{
							Path: "/tmp",
						},
					},
					{
						Name: "etc",
						Type: volume.VolumeSpec_VOLUME_TYPE_HOST_PATH,
						HostPath: &volume.VolumeSpec_HostPathVolumeSource{
							Path: "/etc",
						},
					},
				},
			},
			wantContainerInfo: &mesos.ContainerInfo_DockerInfo{
				Image: ptr.String("docker-image"),
				Parameters: []*mesos.Parameter{
					{
						Key:   ptr.String("env"),
						Value: ptr.String("env-1=value-1"),
					},
					{
						Key:   ptr.String("env"),
						Value: ptr.String("env-2=value-2"),
					},
					{
						Key:   ptr.String("volume"),
						Value: ptr.String("/etc:/mnt/etc:ro"),
					},
					{
						Key:   ptr.String("env"),
						Value: ptr.String("env-3=value-3"),
					},
					{
						Key:   ptr.String("volume"),
						Value: ptr.String("/tmp:/mnt/tmp:rw"),
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ci := createDockerInfo(tc.podSpec)
			assert.Equal(t, tc.wantContainerInfo, ci)
		})
	}
}

func TestConvertHealthCheckConfig(t *testing.T) {
	testCases := []struct {
		name          string
		envs          []*pod.Environment
		livenessCheck *pod.HealthCheckSpec
		wantHC        *HealthCheckConfig
		wantErr       string
	}{
		{
			name:          "nil liveness check",
			livenessCheck: nil,
			wantHC:        nil,
			wantErr:       "",
		},
		{
			name:          "disabled liveness check",
			livenessCheck: &pod.HealthCheckSpec{Enabled: false},
			wantHC:        nil,
			wantErr:       "",
		},
		{
			name: "unknown check, expect error",
			livenessCheck: &pod.HealthCheckSpec{
				Enabled: true,
			},
			wantHC:  nil,
			wantErr: "unsupported liveness check type",
		},
		{
			name: "simple command check, default check config",
			livenessCheck: &pod.HealthCheckSpec{
				Enabled: true,
				Type:    pod.HealthCheckSpec_HEALTH_CHECK_TYPE_COMMAND,
				Command: &pod.CommandSpec{
					Value:     "cmd",
					Arguments: []string{"arg0", "arg1"},
				},
			},
			wantHC: &HealthCheckConfig{
				HealthChecker: &HealthCheckerConfig{
					Shell: &ShellHealthChecker{
						ShellCommand: ptr.String("'cmd' 'arg0' 'arg1'"),
					},
				},
				InitialIntervalSecs:     ptr.Float64(15.0),
				IntervalSecs:            ptr.Float64(10.0),
				MaxConsecutiveFailures:  ptr.Int32(3),
				MinConsecutiveSuccesses: ptr.Int32(1),
				TimeoutSecs:             ptr.Float64(20.0),
			},
			wantErr: "",
		},
		{
			name: "simple command check, env replacement",
			envs: []*pod.Environment{
				{
					Name:  "MSG",
					Value: "VALUE",
				},
			},
			livenessCheck: &pod.HealthCheckSpec{
				Enabled: true,
				Type:    pod.HealthCheckSpec_HEALTH_CHECK_TYPE_COMMAND,
				Command: &pod.CommandSpec{
					Value:     "cmd",
					Arguments: []string{"arg0", "arg1", "$(MSG)"},
				},
			},
			wantHC: &HealthCheckConfig{
				HealthChecker: &HealthCheckerConfig{
					Shell: &ShellHealthChecker{
						ShellCommand: ptr.String("'cmd' 'arg0' 'arg1' 'VALUE'"),
					},
				},
				InitialIntervalSecs:     ptr.Float64(15.0),
				IntervalSecs:            ptr.Float64(10.0),
				MaxConsecutiveFailures:  ptr.Int32(3),
				MinConsecutiveSuccesses: ptr.Int32(1),
				TimeoutSecs:             ptr.Float64(20.0),
			},
			wantErr: "",
		},
		{
			name: "simple command check, custom check config",
			livenessCheck: &pod.HealthCheckSpec{
				Enabled: true,
				Type:    pod.HealthCheckSpec_HEALTH_CHECK_TYPE_COMMAND,
				Command: &pod.CommandSpec{
					Value:     "cmd",
					Arguments: []string{"arg0", "arg1"},
				},
				InitialIntervalSecs:    5,
				IntervalSecs:           1,
				MaxConsecutiveFailures: 5,
				SuccessThreshold:       2,
				TimeoutSecs:            10,
			},
			wantHC: &HealthCheckConfig{
				HealthChecker: &HealthCheckerConfig{
					Shell: &ShellHealthChecker{
						ShellCommand: ptr.String("'cmd' 'arg0' 'arg1'"),
					},
				},
				InitialIntervalSecs:     ptr.Float64(5),
				IntervalSecs:            ptr.Float64(1),
				MaxConsecutiveFailures:  ptr.Int32(5),
				MinConsecutiveSuccesses: ptr.Int32(2),
				TimeoutSecs:             ptr.Float64(10.0),
			},
			wantErr: "",
		},
		{
			name: "simple http check, default check config",
			livenessCheck: &pod.HealthCheckSpec{
				Enabled: true,
				Type:    pod.HealthCheckSpec_HEALTH_CHECK_TYPE_HTTP,
				HttpGet: &pod.HTTPGetSpec{
					Scheme: "https",
					Port:   8080,
					Path:   "/health_check",
				},
			},
			wantHC: &HealthCheckConfig{
				HealthChecker: &HealthCheckerConfig{
					Http: &HttpHealthChecker{
						Endpoint:             ptr.String("https://127.0.0.1:8080/health_check"),
						ExpectedResponseCode: ptr.Int32(200),
					},
				},
				InitialIntervalSecs:     ptr.Float64(15.0),
				IntervalSecs:            ptr.Float64(10.0),
				MaxConsecutiveFailures:  ptr.Int32(3),
				MinConsecutiveSuccesses: ptr.Int32(1),
				TimeoutSecs:             ptr.Float64(20.0),
			},
			wantErr: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			hc, err := convertHealthCheckConfig(tc.envs, tc.livenessCheck)
			assert.Equal(t, tc.wantHC, hc)
			if len(tc.wantErr) > 0 {
				assert.Contains(t, err.Error(), tc.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConvertExecutorData(t *testing.T) {
	testCases := []struct {
		name             string
		jobSpec          *stateless.JobSpec
		podSpec          *pod.PodSpec
		wantExecutorData *ExecutorData
		wantErr          string
	}{
		{
			name: "simple convert",
			jobSpec: &stateless.JobSpec{
				Name: "job-0",
				Sla: &stateless.SlaSpec{
					Priority:    13,
					Revocable:   true,
					Preemptible: true,
				},
			},
			podSpec: &pod.PodSpec{
				InitContainers: []*pod.ContainerSpec{
					{
						Name: "init-0",
						Entrypoint: &pod.CommandSpec{
							Value:     "cmd-0",
							Arguments: []string{"arg-0", "arg-1"},
						},
					},
					{
						Name: "init-1",
						Entrypoint: &pod.CommandSpec{
							Value:     "cmd-0",
							Arguments: []string{"arg-0", "arg-1"},
						},
					},
				},
				Containers: []*pod.ContainerSpec{
					{
						Name: "container-0",
						Entrypoint: &pod.CommandSpec{
							Value:     "cmd-0",
							Arguments: []string{"arg-0", "arg-1"},
						},
						Resource: &pod.ResourceSpec{
							CpuLimit:    2.0,
							MemLimitMb:  512.0,
							DiskLimitMb: 1024.0,
							GpuLimit:    1.0,
						},
						LivenessCheck: &pod.HealthCheckSpec{
							Enabled: true,
							Type:    pod.HealthCheckSpec_HEALTH_CHECK_TYPE_COMMAND,
							Command: &pod.CommandSpec{
								Value:     "/bin/health_check.sh",
								Arguments: []string{"12345", "54321"},
							},
						},
					},
					{
						Name: "container-1",
						Entrypoint: &pod.CommandSpec{
							Value:     "cmd-1",
							Arguments: []string{"arg-1", "arg-2"},
						},
						Resource: &pod.ResourceSpec{
							CpuLimit:    3.0,
							MemLimitMb:  128.0,
							DiskLimitMb: 2048.0,
						},
						LivenessCheck: &pod.HealthCheckSpec{
							Enabled: true,
							Type:    pod.HealthCheckSpec_HEALTH_CHECK_TYPE_COMMAND,
							Command: &pod.CommandSpec{
								Value:     "/bin/health_check.sh",
								Arguments: []string{"22222", "33333"},
							},
						},
					},
				},
			},
			wantExecutorData: &ExecutorData{
				Role:            ptr.String("job-0"),
				Environment:     ptr.String("us1.production"),
				Name:            ptr.String("job-0.job-0"),
				Priority:        ptr.Int32(13),
				MaxTaskFailures: ptr.Int32(1),
				Production:      ptr.Bool(false),
				Tier:            ptr.String("revocable"),
				HealthCheckConfig: &HealthCheckConfig{
					HealthChecker: &HealthCheckerConfig{
						Shell: &ShellHealthChecker{
							ShellCommand: ptr.String("'/bin/health_check.sh' '12345' '54321'"),
						},
					},
					InitialIntervalSecs:     ptr.Float64(15),
					IntervalSecs:            ptr.Float64(10),
					MaxConsecutiveFailures:  ptr.Int32(3),
					MinConsecutiveSuccesses: ptr.Int32(1),
					TimeoutSecs:             ptr.Float64(20),
				},
				CronCollisionPolicy: ptr.String("KILL_EXISTING"),
				EnableHooks:         ptr.Bool(false),
				Task: &Task{
					Name:             ptr.String("job-0"),
					FinalizationWait: ptr.Int32(30),
					MaxFailures:      ptr.Int32(1),
					MaxConcurrency:   ptr.Int32(0),
					Resources: &Resources{
						Cpu:       ptr.Float64(5),
						RamBytes:  ptr.Int64(640 * MbInBytes),
						DiskBytes: ptr.Int64(3072 * MbInBytes),
						Gpu:       ptr.Int32(1),
					},
					Processes: []*Process{
						{
							Cmdline:     ptr.String("'cmd-0' 'arg-0' 'arg-1'"),
							Name:        ptr.String("init-0"),
							MaxFailures: ptr.Int32(1),
							Daemon:      ptr.Bool(false),
							Ephemeral:   ptr.Bool(false),
							MinDuration: ptr.Int32(5),
							Final:       ptr.Bool(false),
						},
						{
							Cmdline:     ptr.String("'cmd-0' 'arg-0' 'arg-1'"),
							Name:        ptr.String("init-1"),
							MaxFailures: ptr.Int32(1),
							Daemon:      ptr.Bool(false),
							Ephemeral:   ptr.Bool(false),
							MinDuration: ptr.Int32(5),
							Final:       ptr.Bool(false),
						},
						{
							Cmdline:     ptr.String("'cmd-0' 'arg-0' 'arg-1'"),
							Name:        ptr.String("container-0"),
							MaxFailures: ptr.Int32(1),
							Daemon:      ptr.Bool(false),
							Ephemeral:   ptr.Bool(false),
							MinDuration: ptr.Int32(5),
							Final:       ptr.Bool(false),
						},
						{
							Cmdline:     ptr.String("'cmd-1' 'arg-1' 'arg-2'"),
							Name:        ptr.String("container-1"),
							MaxFailures: ptr.Int32(1),
							Daemon:      ptr.Bool(false),
							Ephemeral:   ptr.Bool(false),
							MinDuration: ptr.Int32(5),
							Final:       ptr.Bool(false),
						},
					},
					Constraints: []*Constraint{
						{
							Order: []*string{
								ptr.String("init-0"),
								ptr.String("init-1"),
								ptr.String("container-0"),
							},
						},
						{
							Order: []*string{
								ptr.String("init-0"),
								ptr.String("init-1"),
								ptr.String("container-1"),
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			executorData, err := convertExecutorData(tc.jobSpec, tc.podSpec)
			assert.Equal(t, tc.wantExecutorData, executorData)
			if len(tc.wantErr) > 0 {
				assert.Contains(t, err.Error(), tc.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConvert(t *testing.T) {
	testCases := []struct {
		name       string
		jobSpec    *stateless.JobSpec
		podSpec    *pod.PodSpec
		wantConfig *aurora.TaskConfig
		wantErr    string
	}{
		{
			name: "test convert",
			jobSpec: &stateless.JobSpec{
				Name:  "testjob",
				Owner: "testowner",
				Sla: &stateless.SlaSpec{
					Preemptible: true,
					Revocable:   true,
					Priority:    666,
				},
			},
			podSpec: &pod.PodSpec{
				InitContainers: []*pod.ContainerSpec{
					{
						Name: "init-0",
						Entrypoint: &pod.CommandSpec{
							Value: "cmd-0",
						},
						Environment: []*pod.Environment{
							{
								Name:  "key-1",
								Value: "value-1",
							},
						},
					},
				},
				Containers: []*pod.ContainerSpec{
					{
						Name:  "container-0",
						Image: "dockerimage",
						Entrypoint: &pod.CommandSpec{
							Value: "cmd-0",
						},
						Resource: &pod.ResourceSpec{
							CpuLimit:    2.0,
							MemLimitMb:  512.0,
							DiskLimitMb: 1024.0,
							GpuLimit:    1.0,
						},
						LivenessCheck: &pod.HealthCheckSpec{
							Enabled: true,
							Type:    pod.HealthCheckSpec_HEALTH_CHECK_TYPE_COMMAND,
							Command: &pod.CommandSpec{
								Value:     "/bin/health_check.sh",
								Arguments: []string{"12345", "54321"},
							},
						},
						VolumeMounts: []*pod.VolumeMount{
							{
								Name:      "volume",
								ReadOnly:  true,
								MountPath: "/mnt/tmp",
							},
						},
						Environment: []*pod.Environment{
							{
								Name:  "key-2",
								Value: "value-2",
							},
						},
					},
				},
				Labels: []*peloton.Label{
					{
						Key:   "label",
						Value: "value",
					},
					{
						Key:   "org.apache.aurora.metadata.aurora-label",
						Value: "aurora-value",
					},
				},
				Volumes: []*volume.VolumeSpec{
					{
						Name: "volume",
						Type: volume.VolumeSpec_VOLUME_TYPE_HOST_PATH,
						HostPath: &volume.VolumeSpec_HostPathVolumeSource{
							Path: "/tmp",
						},
					},
				},
			},
			wantConfig: &aurora.TaskConfig{
				Job: &aurora.JobKey{
					Role:        ptr.String("testjob"),
					Environment: ptr.String("us1.production"),
					Name:        ptr.String("testjob.testjob"),
				},
				Owner: &aurora.Identity{
					User: ptr.String("testowner"),
				},
				IsService:  ptr.Bool(true),
				Priority:   ptr.Int32(666),
				Production: ptr.Bool(false),
				Tier:       ptr.String("revocable"),
				Resources: []*aurora.Resource{
					{
						NumCpus: ptr.Float64(2),
					},
					{
						RamMb: ptr.Int64(512),
					},
					{
						DiskMb: ptr.Int64(1024),
					},
					{
						NumGpus: ptr.Int64(1),
					},
				},
				ExecutorConfig: &aurora.ExecutorConfig{
					Name: ptr.String("AuroraExecutor"),
					Data: ptr.String(`{"role":"testjob","environment":"us1.production","name":"testjob.testjob","priority":666,"max_task_failures":1,"production":false,"tier":"revocable","health_check_config":{"health_checker":{"shell":{"shell_command":"'/bin/health_check.sh' '12345' '54321'"}},"initial_interval_secs":15,"interval_secs":10,"max_consecutive_failures":3,"min_consecutive_successes":1,"timeout_secs":20},"cron_collision_policy":"KILL_EXISTING","enable_hooks":false,"task":{"name":"testjob","finalization_wait":30,"max_failures":1,"max_concurrency":0,"resources":{"cpu":2,"ram":536870912,"disk":1073741824,"gpu":1},"processes":[{"cmdline":"'cmd-0'","name":"init-0","max_failures":1,"daemon":false,"ephemeral":false,"min_duration":5,"final":false},{"cmdline":"'cmd-0'","name":"container-0","max_failures":1,"daemon":false,"ephemeral":false,"min_duration":5,"final":false}],"constraints":[{"order":["init-0","container-0"]}]}}`),
				},
				Metadata: []*aurora.Metadata{
					{
						Key:   ptr.String("label"),
						Value: ptr.String("value"),
					},
					{
						Key:   ptr.String("aurora-label"),
						Value: ptr.String("aurora-value"),
					},
				},
				Container: &aurora.Container{
					Docker: &aurora.DockerContainer{
						Image: ptr.String("dockerimage"),
						Parameters: []*aurora.DockerParameter{
							{
								Name:  ptr.String("env"),
								Value: ptr.String("key-1=value-1"),
							},
							{
								Name:  ptr.String("env"),
								Value: ptr.String("key-2=value-2"),
							},
							{
								Name:  ptr.String("volume"),
								Value: ptr.String("/tmp:/mnt/tmp:ro"),
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config, err := convert(tc.jobSpec, tc.podSpec)
			if len(tc.wantErr) > 0 {
				require.Equal(t, tc.wantErr, err.Error())
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantConfig, config)
		})
	}
}

func TestConvertCmdline(t *testing.T) {
	testCases := []struct {
		name        string
		envs        []*pod.Environment
		command     *pod.CommandSpec
		wantCmdline string
	}{
		{
			name: "simple string, no replacement",
			command: &pod.CommandSpec{
				Value:     "/bin/echo",
				Arguments: []string{"hello", "world"},
			},
			wantCmdline: "'/bin/echo' 'hello' 'world'",
		},
		{
			name: "simple string, has replacement no env",
			command: &pod.CommandSpec{
				Value:     "/bin/echo",
				Arguments: []string{"$(MSG)"},
			},
			wantCmdline: "'/bin/echo' '$(MSG)'",
		},
		{
			name: "simple string, has replacement",
			envs: []*pod.Environment{
				{
					Name:  "MSG",
					Value: "hello world",
				},
			},
			command: &pod.CommandSpec{
				Value:     "/bin/echo",
				Arguments: []string{"$(MSG)"},
			},
			wantCmdline: "'/bin/echo' 'hello world'",
		},
		{
			name: "simple string, replacement escape",
			envs: []*pod.Environment{
				{
					Name:  "MSG",
					Value: "hello world",
				},
			},
			command: &pod.CommandSpec{
				Value:     "/bin/echo",
				Arguments: []string{"$(MSG)", "$$(MSG)"},
			},
			wantCmdline: "'/bin/echo' 'hello world' '$(MSG)'",
		},
		{
			name: "bash script string, no replacement",
			command: &pod.CommandSpec{
				Value:     "/bin/bash",
				Arguments: []string{"-c", `while true; do echo 'hello\nworld'; sleep 5; done`},
			},
			wantCmdline: `'/bin/bash' '-c' 'while true; do echo '"'"'hello\nworld'"'"'; sleep 5; done'`,
		},
		{
			name: "bash script string, with native and bash replacement",
			envs: []*pod.Environment{
				{
					Name:  "MSG",
					Value: "VALUE",
				},
			},
			command: &pod.CommandSpec{
				Value:     "/bin/bash",
				Arguments: []string{"-c", `while true; do echo $(MSG); echo ${HOME}; sleep 5; done`},
			},
			wantCmdline: `'/bin/bash' '-c' 'while true; do echo VALUE; echo ${HOME}; sleep 5; done'`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cmdline := convertCmdline(tc.envs, tc.command)
			assert.Equal(t, tc.wantCmdline, cmdline)
		})
	}
}

func TestConvertTier(t *testing.T) {
	testCases := []struct {
		name     string
		spec     *stateless.JobSpec
		wantTier string
	}{
		{
			name: "convert tier, expect revocable",
			spec: &stateless.JobSpec{
				Sla: &stateless.SlaSpec{
					Preemptible: true,
					Revocable:   true,
				},
			},
			wantTier: "revocable",
		},
		{
			name: "convert tier, expect preemptible",
			spec: &stateless.JobSpec{
				Sla: &stateless.SlaSpec{
					Preemptible: true,
					Revocable:   false,
				},
			},
			wantTier: "preemptible",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tier := convertTier(tc.spec)
			assert.Equal(t, tc.wantTier, tier)
		})
	}
}

func TestConvertTaskConfigToBinary(t *testing.T) {
	testCases := []struct {
		name     string
		config   *aurora.TaskConfig
		wantData []byte
		wantErr  string
	}{
		{
			name: "simple task config convert",
			config: &aurora.TaskConfig{
				Job: &aurora.JobKey{
					Role:        ptr.String("role"),
					Environment: ptr.String("environment"),
					Name:        ptr.String("name"),
				},
			},
			wantData: []byte{
				0x0c, 0x00, 0x1c, 0x0b, 0x00, 0x01, 0x00, 0x00,
				0x00, 0x04, 0x72, 0x6f, 0x6c, 0x65, 0x0b, 0x00,
				0x02, 0x00, 0x00, 0x00, 0x0b, 0x65, 0x6e, 0x76,
				0x69, 0x72, 0x6f, 0x6e, 0x6d, 0x65, 0x6e, 0x74,
				0x0b, 0x00, 0x03, 0x00, 0x00, 0x00, 0x04, 0x6e,
				0x61, 0x6d, 0x65, 0x00, 0x0c, 0x00, 0x1d, 0x0c,
				0x00, 0x01, 0x00, 0x00, 0x00},
		},
		{
			name: "task config failure",
			config: &aurora.TaskConfig{
				Container: &aurora.Container{},
			},
			wantErr: "Container should have exactly one field: got 0 fields",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := thermos.EncodeTaskConfig(tc.config)
			if len(tc.wantErr) > 0 {
				assert.Contains(t, err.Error(), tc.wantErr)
			} else {
				assert.NoError(t, err)
			}
			require.Equal(t, tc.wantData, data)
		})
	}
}

func TestMutatePodSpec(t *testing.T) {
	testCases := []struct {
		name          string
		podSpec       *pod.PodSpec
		thermosConfig config.ThermosExecutorConfig
		wantPodSpec   *pod.PodSpec
	}{
		{
			name: "simple mutate pod spec test",
			podSpec: &pod.PodSpec{
				InitContainers: []*pod.ContainerSpec{
					{
						Name: "init-0",
						Entrypoint: &pod.CommandSpec{
							Value:     "cmd-0",
							Arguments: []string{"arg-0", "arg-1"},
						},
						Environment: []*pod.Environment{
							{
								Name:  "name-1",
								Value: "value-1",
							},
						},
					},
					{
						Name: "init-1",
						Entrypoint: &pod.CommandSpec{
							Value:     "cmd-0",
							Arguments: []string{"arg-0", "arg-1"},
						},
						VolumeMounts: []*pod.VolumeMount{
							{
								Name:      "v1",
								ReadOnly:  false,
								MountPath: "/mountpath1",
							},
						},
					},
				},
				Containers: []*pod.ContainerSpec{
					{
						Name: "container-0",
						Entrypoint: &pod.CommandSpec{
							Value:     "cmd-0",
							Arguments: []string{"arg-0", "arg-1"},
						},
						Resource: &pod.ResourceSpec{
							CpuLimit:    2.0,
							MemLimitMb:  512.0,
							DiskLimitMb: 1024.0,
							GpuLimit:    1.0,
						},
						Environment: []*pod.Environment{
							{
								Name:  "name-2",
								Value: "value-2",
							},
						},
					},
					{
						Name: "container-1",
						Entrypoint: &pod.CommandSpec{
							Value:     "cmd-1",
							Arguments: []string{"arg-1", "arg-2"},
						},
						Resource: &pod.ResourceSpec{
							CpuLimit:    3.0,
							MemLimitMb:  128.0,
							DiskLimitMb: 2048.0,
						},
						VolumeMounts: []*pod.VolumeMount{
							{
								Name:      "v2",
								ReadOnly:  true,
								MountPath: "/mountpath2",
							},
						},
					},
				},
				Volumes: []*volume.VolumeSpec{
					{
						Name: "v1",
						Type: volume.VolumeSpec_VOLUME_TYPE_HOST_PATH,
						HostPath: &volume.VolumeSpec_HostPathVolumeSource{
							Path: "/hostpath1",
						},
					},
					{
						Name: "v2",
						Type: volume.VolumeSpec_VOLUME_TYPE_HOST_PATH,
						HostPath: &volume.VolumeSpec_HostPathVolumeSource{
							Path: "/hostpath2",
						},
					},
				},
				MesosSpec: &apachemesos.PodSpec{
					ExecutorSpec: &apachemesos.PodSpec_ExecutorSpec{
						Type: apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_CUSTOM,
					},
				},
			},
			thermosConfig: config.ThermosExecutorConfig{
				Path:      "/path/to/thermos/executor",
				Resources: "res1,res2,res3",
				Flags:     "-flag1 -flag2 -flag3",
				CPU:       0.25,
				RAM:       128,
			},
			wantPodSpec: &pod.PodSpec{
				Containers: []*pod.ContainerSpec{
					{
						Name: "thermos-container",
						Resource: &pod.ResourceSpec{
							CpuLimit:    5.0,
							MemLimitMb:  640.0,
							DiskLimitMb: 3072.0,
							GpuLimit:    1.0,
						},
						Entrypoint: &pod.CommandSpec{
							Value: "${MESOS_SANDBOX=.}/executor -flag1 -flag2 -flag3",
						},
					},
				},
				MesosSpec: &apachemesos.PodSpec{
					Type: apachemesos.PodSpec_CONTAINER_TYPE_DOCKER,
					DockerParameters: []*apachemesos.PodSpec_DockerParameter{
						{
							Key:   "env",
							Value: "name-1=value-1",
						},
						{
							Key:   "volume",
							Value: "/hostpath1:/mountpath1:rw",
						},
						{
							Key:   "env",
							Value: "name-2=value-2",
						},
						{
							Key:   "volume",
							Value: "/hostpath2:/mountpath2:ro",
						},
					},
					Uris: []*apachemesos.PodSpec_URI{
						{
							Value:      "/path/to/thermos/executor",
							Executable: true,
						},
						{
							Value:      "res1",
							Executable: true,
						},
						{
							Value:      "res2",
							Executable: true,
						},
						{
							Value:      "res3",
							Executable: true,
						},
					},
					Shell: true,
					ExecutorSpec: &apachemesos.PodSpec_ExecutorSpec{
						Type:       apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_CUSTOM,
						ExecutorId: "PLACEHOLDER",
						Resources: &apachemesos.PodSpec_ExecutorSpec_Resources{
							Cpu:   0.25,
							MemMb: 128,
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			spec := proto.Clone(tc.podSpec).(*pod.PodSpec)
			mutatePodSpec(spec, nil, tc.thermosConfig)
			assert.True(t, proto.Equal(tc.wantPodSpec, spec))
		})
	}
}
