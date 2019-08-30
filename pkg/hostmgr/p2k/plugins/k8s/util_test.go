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
	"testing"

	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
)

func TestToK8SPodSpec(t *testing.T) {
	require := require.New(t)
	containerPath := "/container/path"
	hostPath := "/host/path"

	// Launch pod and verify.
	testPodSpec := &pbpod.PodSpec{
		Containers: []*pbpod.ContainerSpec{
			{
				Name: "",
				Resource: &pbpod.ResourceSpec{
					CpuLimit:   1.0,
					MemLimitMb: 10.0,
				},
				Ports: []*pbpod.PortSpec{
					{
						Name:  "http",
						Value: 8080,
					},
				},
				Image: "",
				VolumeMounts: []*pbpod.VolumeMount{
					{
						Name:      hostPath,
						MountPath: containerPath,
						ReadOnly:  true,
					},
				},
			},
		},
	}

	returnedPod := toK8SPodSpec(testPodSpec)
	require.NotNil(returnedPod)
	cname := uuid.Parse(returnedPod.Spec.Containers[0].Name)
	require.NotNil(cname)
	require.Equal(returnedPod.Spec.Containers[0].Image, _defaultImageName)
	require.Equal(
		returnedPod.Spec.Containers[0].VolumeMounts[0].MountPath,
		testPodSpec.Containers[0].VolumeMounts[0].MountPath,
	)
	require.Equal(
		returnedPod.Spec.Containers[0].VolumeMounts[0].Name,
		testPodSpec.Containers[0].VolumeMounts[0].Name,
	)
}
