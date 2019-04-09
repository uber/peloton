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

package taskconfig

import (
	"testing"

	mesos_v1 "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/pkg/common/util"

	"github.com/stretchr/testify/assert"
)

func TestMergeNoInstanceConfig(t *testing.T) {
	defaultConfig := &task.TaskConfig{
		Resource: &task.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
			FdLimit:     1000,
		},
	}

	assert.Equal(t, defaultConfig, Merge(defaultConfig, nil))
}

func TestMergeNoDefaultConfig(t *testing.T) {
	instanceConfig := &task.TaskConfig{
		Resource: &task.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
			FdLimit:     1000,
		},
	}

	assert.Equal(t, instanceConfig, Merge(nil, instanceConfig))
	assert.Equal(t, (*task.TaskConfig)(nil), Merge(nil, nil))
}

func TestMergeInstanceOverride(t *testing.T) {
	defaultConfig := &task.TaskConfig{
		Name: "Instance_X",
		Resource: &task.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
			FdLimit:     1000,
		},
		Controller: false,
	}
	instanceConfig := &task.TaskConfig{
		Resource: &task.ResourceConfig{
			CpuLimit:    1,
			MemLimitMb:  100,
			DiskLimitMb: 2000,
			FdLimit:     3000,
		},
		Controller: true,
	}

	assert.Equal(t, instanceConfig.Resource, Merge(defaultConfig, instanceConfig).Resource)
	assert.Equal(t, defaultConfig.Name, Merge(defaultConfig, instanceConfig).Name)
	assert.Equal(t, instanceConfig.Controller, Merge(defaultConfig,
		instanceConfig).Controller)
}

// TestMergeInstanceOverrideGracePeriod tests if the merged
// task config reflects the expected killgraceperiodseconds
func TestMergeInstanceOverrideGracePeriod(t *testing.T) {
	// check override works when override config has non-zero
	// grace period
	cfg := Merge(
		&task.TaskConfig{KillGracePeriodSeconds: uint32(20)},
		&task.TaskConfig{KillGracePeriodSeconds: uint32(30)})
	assert.Equal(t, uint32(30), cfg.GetKillGracePeriodSeconds())

	// check if we retain base config when override config
	// grace period is not set
	cfg = Merge(
		&task.TaskConfig{KillGracePeriodSeconds: uint32(20)},
		&task.TaskConfig{KillGracePeriodSeconds: uint32(0)})
	assert.Equal(t, uint32(20), cfg.GetKillGracePeriodSeconds())
}

func TestMergeWithExistingEnviron(t *testing.T) {
	defaultConfig := &task.TaskConfig{
		Command: &mesos_v1.CommandInfo{
			Value: util.PtrPrintf("echo Hello"),
			Environment: &mesos_v1.Environment{
				Variables: []*mesos_v1.Environment_Variable{
					{
						Name:  util.PtrPrintf("PATH"),
						Value: util.PtrPrintf("/usr/bin;/usr/sbin"),
					},
				},
			},
		},
	}

	assert.Equal(t, &mesos_v1.Environment{
		Variables: []*mesos_v1.Environment_Variable{
			{
				Name:  util.PtrPrintf("PATH"),
				Value: util.PtrPrintf("/usr/bin;/usr/sbin"),
			},
		},
	}, Merge(defaultConfig, nil).Command.Environment)
}

// TestRetainBaseSecretsInInstanceConfig tests if instance config that contains
// overriding container info still retains the secret volumes from the default
// config's container info.
func TestRetainBaseSecretsInInstanceConfig(t *testing.T) {
	mesosContainerizer := mesos_v1.ContainerInfo_MESOS
	defaultConfig := &task.TaskConfig{
		Container: &mesos_v1.ContainerInfo{
			Type: &mesosContainerizer,
			Volumes: []*mesos_v1.Volume{
				util.CreateSecretVolume("/tmp/secret", "data"),
			}}}
	imageType := mesos_v1.Image_DOCKER
	imageName := "sparkdocker"
	instanceConfig := &task.TaskConfig{
		Container: &mesos_v1.ContainerInfo{
			Type: &mesosContainerizer,
			Mesos: &mesos_v1.ContainerInfo_MesosInfo{
				Image: &mesos_v1.Image{
					Type:   &imageType,
					Docker: &mesos_v1.Image_Docker{Name: &imageName},
				}}}}

	// default config is empty. there should be no change in returned cfg
	// and instance config
	cfg := retainBaseSecretsInInstanceConfig(&task.TaskConfig{}, instanceConfig)
	assert.Equal(t, cfg, instanceConfig)

	// default config contains secret volume. returned instance config should
	// contain same secret volumes as part of its new container info.
	cfg = retainBaseSecretsInInstanceConfig(defaultConfig, instanceConfig)
	assert.Equal(t, cfg.GetContainer().GetMesos(),
		instanceConfig.GetContainer().GetMesos())
	assert.NotEqual(t, defaultConfig.GetContainer().GetMesos(),
		instanceConfig.GetContainer().GetMesos())
	assert.Equal(t, defaultConfig.GetContainer().GetVolumes(),
		instanceConfig.GetContainer().GetVolumes())

	// instance config contains non secret volumes, returned instance config
	// should contain existing plus secret volumes
	volumeMode := mesos_v1.Volume_RO
	testPath := "/test"
	instanceConfig.GetContainer().Volumes = []*mesos_v1.Volume{{
		Mode: &volumeMode, ContainerPath: &testPath,
	}}
	cfg = retainBaseSecretsInInstanceConfig(defaultConfig, instanceConfig)
	assert.Equal(t, len(cfg.GetContainer().GetVolumes()), 2)
}

// TestMergeNoInstanceSpec checks MergePodSpec simply returns default spec
// when instance spec is not provided.
func TestMergeNoInstanceSpec(t *testing.T) {
	defaultSpec := &pod.PodSpec{
		Containers: []*pod.ContainerSpec{
			{
				Resource: &pod.ResourceSpec{
					CpuLimit:    0.8,
					MemLimitMb:  800,
					DiskLimitMb: 1500,
					FdLimit:     1000,
				},
			},
		},
	}

	assert.Equal(t, defaultSpec, MergePodSpec(defaultSpec, nil))
}

// TestMergeNoDefaultSpec checks MergePodSpec simply returns instance spec
// when default spec is not provided.
func TestMergeNoDefaultSpec(t *testing.T) {
	instanceSpec := &pod.PodSpec{
		Containers: []*pod.ContainerSpec{
			{
				Resource: &pod.ResourceSpec{
					CpuLimit:    0.8,
					MemLimitMb:  800,
					DiskLimitMb: 1500,
					FdLimit:     1000,
				},
			},
		},
	}

	assert.Equal(t, instanceSpec, MergePodSpec(nil, instanceSpec))
	assert.Equal(t, (*pod.PodSpec)(nil), MergePodSpec(nil, nil))
}

// TestMergeSpecOverride checks MergePodSpec successfully returning
// merged pod spec when both default and instance spec are provided.
func TestMergeSpecOverride(t *testing.T) {
	defaultSpec := &pod.PodSpec{
		Containers: []*pod.ContainerSpec{
			{
				Resource: &pod.ResourceSpec{
					CpuLimit:    0.8,
					MemLimitMb:  800,
					DiskLimitMb: 1500,
					FdLimit:     1000,
				},
			},
		},
		Controller: false,
	}
	instanceSpec := &pod.PodSpec{
		Containers: []*pod.ContainerSpec{
			{
				Resource: &pod.ResourceSpec{
					CpuLimit:    1,
					MemLimitMb:  100,
					DiskLimitMb: 2000,
					FdLimit:     3000,
				},
			},
		},
		Controller: true,
	}

	assert.Equal(t,
		instanceSpec.Containers[0].Resource,
		MergePodSpec(defaultSpec, instanceSpec).Containers[0].Resource,
	)
	assert.Equal(t,
		instanceSpec.Controller,
		MergePodSpec(defaultSpec, instanceSpec).Controller,
	)
}

// TestMergeSpecOverride checks MergePodSpec successfully returning
// merged pod spec when kill_grace_period_seconds (int field) exists
// in both default and instance specs.
func TestMergeSpecOverrideGracePeriod(t *testing.T) {
	cfg := MergePodSpec(
		&pod.PodSpec{KillGracePeriodSeconds: uint32(20)},
		&pod.PodSpec{KillGracePeriodSeconds: uint32(30)})
	assert.Equal(t, uint32(30), cfg.GetKillGracePeriodSeconds())

	cfg = MergePodSpec(
		&pod.PodSpec{KillGracePeriodSeconds: uint32(20)},
		&pod.PodSpec{KillGracePeriodSeconds: uint32(0)})
	assert.Equal(t, uint32(20), cfg.GetKillGracePeriodSeconds())
}
