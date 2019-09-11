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

package hostsummary

import (
	"fmt"
	"github.com/uber/peloton/pkg/hostmgr/models"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/pkg/hostmgr/scalar"

	"github.com/pborman/uuid"
)

var (
	// capacity of host for testing
	_capacity = scalar.Resources{
		CPU: 10.0,
		Mem: 100.0,
	}
	// host name string
	_hostname = "host"
	// host resource version
	_version = "1234"
	// lease ID
	_leaseID = uuid.New()
	// pod ID
	_podID = uuid.New()
)

type FakeHostSummary struct {
	*baseHostSummary
}

func (f *FakeHostSummary) SetAllocated(allocated scalar.Resources) {
	f.allocated = models.HostResources{
		NonSlack: allocated,
	}
}

func (f *FakeHostSummary) GetPodInfo(
	podID *peloton.PodID,
) (pbpod.PodState, *pbpod.PodSpec, bool) {
	podInfo, ok := f.pods.GetPodInfo(podID.GetValue())
	if !ok {
		return pbpod.PodState_POD_STATE_INVALID, nil, false
	}

	return podInfo.state, podInfo.spec, ok
}

// NewFakeHostSummary is used for testing. It offers methods for easier
// manipulation of internal attribute.
func NewFakeHostSummary(
	hostname string, version string, capacity scalar.Resources) *FakeHostSummary {

	f := &FakeHostSummary{
		baseHostSummary: newBaseHostSummary(hostname, version),
	}
	f.capacity = models.HostResources{
		NonSlack: capacity,
	}
	f.available = f.capacity

	return f
}

// GenerateFakeHostSummaries returns a list of FakeHostSummary.
func GenerateFakeHostSummaries(numHosts int) []*FakeHostSummary {
	var hosts []*FakeHostSummary
	for i := 0; i < numHosts; i++ {
		hosts = append(
			hosts,
			NewFakeHostSummary(
				fmt.Sprintf("%v%v", _hostname, i),
				_version,
				_capacity),
		)
	}
	return hosts
}

func CreateResource(cpu, mem float64) (r scalar.Resources) {
	r.CPU = cpu
	r.Mem = mem
	return r
}

// GeneratePodToResMap generates a map of podIDs to resources where each pod
// gets the specified `cpu` and `mem`
func GeneratePodToResMap(
	numPods int,
	cpu, mem float64,
) map[string]scalar.Resources {
	podMap := make(map[string]scalar.Resources)
	for i := 0; i < numPods; i++ {
		podMap[uuid.New()] = CreateResource(cpu, mem)
	}
	return podMap
}

func GeneratePodSpecWithRes(
	numPods int,
	cpu, mem float64,
) map[string]*pbpod.PodSpec {
	podMap := make(map[string]*pbpod.PodSpec)
	for i := 0; i < numPods; i++ {
		podMap[uuid.New()] = &pbpod.PodSpec{
			Containers: []*pbpod.ContainerSpec{
				{Resource: &pbpod.ResourceSpec{
					CpuLimit:   cpu,
					MemLimitMb: mem,
				}},
			},
		}
	}
	return podMap
}
