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
	"bytes"
	"encoding/json"
	"fmt"
	"sort"

	mesos_v1 "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/thrift/aurora/api"
	"github.com/uber/peloton/pkg/aurorabridge/common"
	"github.com/uber/peloton/pkg/aurorabridge/label"

	"go.uber.org/thriftrw/protocol"
	"go.uber.org/thriftrw/ptr"
)

// NewPodSpec creates a new PodSpec.
func NewPodSpec(
	t *api.TaskConfig,
	c ThermosExecutorConfig,
) (*pod.PodSpec, error) {
	// Taking aurora TaskConfig struct from JobUpdateRequest, and
	// serialize it using Thrift binary protocol. The resulting
	// byte array will be attached to ExecutorInfo.Data.
	//
	// After task placement, jobmgr will deserialize the byte array,
	// combine it with placement info, and generates aurora AssignedTask
	// struct, which will be eventually be consumed by thermos executor.
	//
	// Leaving encodeTaskConfig at the top since it has the side-effect
	// of sorting all the "list" fields.
	executorData, err := encodeTaskConfig(t)
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

	return &pod.PodSpec{
		PodName:        nil, // Unused.
		Labels:         labels,
		InitContainers: nil, // Unused.
		Containers: []*pod.ContainerSpec{{
			Name:           "", // Unused.
			Resource:       newResourceSpec(t.GetResources(), gpuLimit),
			Container:      newMesosContainerInfo(t.GetContainer()),
			Command:        NewThermosCommandInfo(c),
			Executor:       NewThermosExecutorInfo(c, executorData),
			LivenessCheck:  nil, // TODO(codyg): Figure this default.
			ReadinessCheck: nil, // Unused.
			Ports:          newPortSpecs(t.GetResources()),
		}},
		Constraint:             constraint,
		RestartPolicy:          nil,   // Unused.
		Volume:                 nil,   // Unused.
		PreemptionPolicy:       nil,   // Unused.
		Controller:             false, // Unused.
		KillGracePeriodSeconds: 0,     // Unused.
		Revocable:              t.GetTier() == common.Revocable,
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

func newMesosContainerInfo(c *api.Container) *mesos_v1.ContainerInfo {
	if c == nil {
		return nil
	}

	result := &mesos_v1.ContainerInfo{}
	if c.IsSetMesos() {
		result.Type = mesos_v1.ContainerInfo_MESOS.Enum()
		result.Mesos = &mesos_v1.ContainerInfo_MesosInfo{
			Image: newMesosImage(c.GetMesos().GetImage()),
		}
		result.Volumes = newMesosVolumes(c.GetMesos().GetVolumes())
	}
	if c.IsSetDocker() {
		result.Type = mesos_v1.ContainerInfo_DOCKER.Enum()
		result.Docker = &mesos_v1.ContainerInfo_DockerInfo{
			Image:      ptr.String(c.GetDocker().GetImage()),
			Parameters: newMesosDockerParameters(c.GetDocker().GetParameters()),
		}
	}
	return result
}

func newMesosImage(i *api.Image) *mesos_v1.Image {
	if i == nil {
		return nil
	}

	result := &mesos_v1.Image{}
	if i.IsSetDocker() {
		result.Type = mesos_v1.Image_DOCKER.Enum()
		result.Docker = &mesos_v1.Image_Docker{
			Name: ptr.String(
				fmt.Sprintf("%s:%s", i.GetDocker().GetName(), i.GetDocker().GetTag())),
		}
	}
	if i.IsSetAppc() {
		result.Type = mesos_v1.Image_APPC.Enum()
		result.Appc = &mesos_v1.Image_Appc{
			Name: ptr.String(i.GetAppc().GetName()),
			Id:   ptr.String(i.GetAppc().GetImageId()),
		}
	}
	return result
}

func newMesosVolumes(vs []*api.Volume) []*mesos_v1.Volume {
	var result []*mesos_v1.Volume
	for _, v := range vs {
		result = append(result, &mesos_v1.Volume{
			ContainerPath: ptr.String(v.GetContainerPath()),
			HostPath:      ptr.String(v.GetHostPath()),
			Mode:          newMesosVolumeMode(v.GetMode()).Enum(),
		})
	}
	return result
}

func newMesosVolumeMode(mode api.Mode) mesos_v1.Volume_Mode {
	if mode == api.ModeRw {
		return mesos_v1.Volume_RW
	}
	return mesos_v1.Volume_RO
}

func newMesosDockerParameters(ps []*api.DockerParameter) []*mesos_v1.Parameter {
	var result []*mesos_v1.Parameter
	for _, p := range ps {
		result = append(result, &mesos_v1.Parameter{
			Key:   ptr.String(p.GetName()),
			Value: ptr.String(p.GetValue()),
		})
	}
	return result
}

func encodeTaskConfig(t *api.TaskConfig) ([]byte, error) {
	// Sort golang list types in order to make results consistent
	sort.Stable(common.MetadataByKey(t.Metadata))
	sort.Stable(common.ResourceByType(t.Resources))
	sort.Stable(common.ConstraintByName(t.Constraints))
	sort.Stable(common.MesosFetcherURIByValue(t.MesosFetcherUris))

	if len(t.GetExecutorConfig().GetData()) != 0 {
		var dat map[string]interface{}
		json.Unmarshal([]byte(t.GetExecutorConfig().GetData()), &dat)
		data, _ := json.MarshalIndent(dat, "", "")
		t.ExecutorConfig.Data = ptr.String(string(data))
	}

	w, err := t.ToWire()
	if err != nil {
		return nil, fmt.Errorf("convert task config to wire value: %s", err)
	}

	var b bytes.Buffer
	err = protocol.Binary.Encode(w, &b)
	if err != nil {
		return nil, fmt.Errorf("serialize task config to binary: %s", err)
	}

	return b.Bytes(), nil
}
