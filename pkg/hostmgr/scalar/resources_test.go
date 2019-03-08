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

package scalar

import (
	"strconv"
	"testing"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/pkg/common"

	"github.com/stretchr/testify/assert"
	"github.com/uber/peloton/pkg/common/util"
)

var (
	_testAgent       = "agent"
	_defaultResValue = 1.0

	_cpuRes = util.NewMesosResourceBuilder().
		WithName(common.MesosCPU).
		WithValue(1.0).
		Build()
	_cpuRevocableRes = util.NewMesosResourceBuilder().
				WithName(common.MesosCPU).
				WithValue(1.0).
				WithRevocable(&mesos.Resource_RevocableInfo{}).
				Build()
	_memRes = util.NewMesosResourceBuilder().
		WithName(common.MesosMem).
		WithValue(1.0).
		Build()
	_memRevocableRes = util.NewMesosResourceBuilder().
				WithName(common.MesosMem).
				WithValue(1.0).
				WithRevocable(&mesos.Resource_RevocableInfo{}).
				Build()
	_diskRes = util.NewMesosResourceBuilder().
			WithName(common.MesosDisk).
			WithValue(1.0).
			Build()
	_gpuRes = util.NewMesosResourceBuilder().
		WithName(common.MesosGPU).
		WithValue(1.0).
		Build()
	_isSlackResourceType = func(resourceType string) bool {
		if resourceType == common.MesosCPU {
			return true
		}
		return false
	}
)

const _zeroDelta = 0.000001

func createResource(cpus, gpus, mem, disk float64) Resources {
	return Resources{
		CPU:  cpus,
		Mem:  mem,
		Disk: disk,
		GPU:  gpus,
	}
}

func createUnreservedMesosOffer(
	offerID string) *mesos.Offer {
	rs := []*mesos.Resource{
		_cpuRes,
		_memRes,
		_diskRes,
		_gpuRes,
		_cpuRevocableRes,
		_memRevocableRes,
	}

	return &mesos.Offer{
		Id: &mesos.OfferID{
			Value: &offerID,
		},
		AgentId: &mesos.AgentID{
			Value: &_testAgent,
		},
		Hostname:  &_testAgent,
		Resources: rs,
	}
}

func createUnreservedMesosOffers(count int) []*mesos.Offer {
	var offers []*mesos.Offer
	for i := 0; i < count; i++ {
		offers = append(offers, createUnreservedMesosOffer("offer-id-"+strconv.Itoa(i)))
	}
	return offers
}

func TestContains(t *testing.T) {
	// An empty Resources should container another empty one.
	empty1 := Resources{}
	empty2 := Resources{}
	assert.True(t, empty1.Contains(empty1))
	assert.True(t, empty1.Contains(empty2))

	r1 := Resources{
		CPU: 1.0,
	}
	assert.True(t, r1.Contains(r1))
	assert.False(t, empty1.Contains(r1))
	assert.True(t, r1.Contains(empty1))

	r2 := Resources{
		Mem: 1.0,
	}
	assert.False(t, r1.Contains(r2))
	assert.False(t, r2.Contains(r1))

	r3 := Resources{
		CPU:  1.0,
		Mem:  1.0,
		Disk: 1.0,
		GPU:  1.0,
	}
	assert.False(t, r1.Contains(r3))
	assert.False(t, r2.Contains(r3))
	assert.True(t, r3.Contains(r1))
	assert.True(t, r3.Contains(r2))
	assert.True(t, r3.Contains(r3))
}

func TestCompareGe(t *testing.T) {
	r1 := Resources{
		CPU:  3.0,
		GPU:  2.0,
		Mem:  1024,
		Disk: 1024,
	}
	l1 := Resources{
		CPU:  2.0,
		GPU:  1.0,
		Mem:  1024,
		Disk: 1024,
	}
	assert.True(t, r1.Compare(l1, false))

	l2 := Resources{
		CPU:  3.0,
		GPU:  2.0,
		Mem:  1024,
		Disk: 1024,
	}
	assert.True(t, r1.Compare(l2, false))

	l3 := Resources{
		CPU: 3.0,
		GPU: 0.0,
	}
	assert.True(t, r1.Compare(l3, false))

	l4 := Resources{}
	assert.True(t, r1.Compare(l4, false))

	l5 := Resources{
		CPU:  4.0,
		GPU:  3.0,
		Mem:  2048,
		Disk: 2048,
	}
	assert.False(t, r1.Compare(l5, false))

	l6 := Resources{
		CPU: 4.0,
		GPU: 1.0,
	}
	assert.False(t, r1.Compare(l6, false))
}

func TestCompareLess(t *testing.T) {
	r1 := Resources{
		CPU:  3.0,
		GPU:  2.0,
		Mem:  1024,
		Disk: 1024,
	}
	l1 := Resources{
		CPU:  4.0,
		GPU:  3.0,
		Mem:  2048,
		Disk: 2048,
	}
	assert.True(t, r1.Compare(l1, true))

	l2 := Resources{
		CPU:  4.0,
		GPU:  3.0,
		Mem:  1024,
		Disk: 1024,
	}
	assert.False(t, r1.Compare(l2, true))

	l3 := Resources{
		CPU:  4.0,
		GPU:  3.0,
		Mem:  1024,
		Disk: 2048,
	}
	assert.False(t, r1.Compare(l3, true))

	l4 := Resources{
		CPU:  4.0,
		GPU:  3.0,
		Mem:  2048,
		Disk: 1024,
	}
	assert.False(t, r1.Compare(l4, true))

	l5 := Resources{
		CPU: 3.0,
		GPU: 2.0,
	}
	assert.False(t, r1.Compare(l5, true))

	l6 := Resources{
		CPU: 3.0,
		GPU: 0.0,
	}
	assert.False(t, r1.Compare(l6, true))

	l9 := Resources{
		CPU: 0.0,
		GPU: 2.0,
	}
	assert.False(t, r1.Compare(l9, true))

	l7 := Resources{}
	assert.True(t, r1.Compare(l7, true))

	l8 := Resources{
		CPU: 4.0,
		GPU: 0.0,
	}
	assert.True(t, r1.Compare(l8, true))
}

func TestHasGPU(t *testing.T) {
	empty := Resources{}
	assert.False(t, empty.HasGPU())

	r1 := Resources{
		CPU: 1.0,
	}
	assert.False(t, r1.HasGPU())

	r2 := Resources{
		CPU: 1.0,
		GPU: 1.0,
	}
	assert.True(t, r2.HasGPU())
}

func TestAdd(t *testing.T) {
	empty := Resources{}
	r1 := Resources{
		CPU: 1.0,
	}

	result := empty.Add(empty)
	assert.InDelta(t, 0.0, result.CPU, _zeroDelta)
	assert.InDelta(t, 0.0, result.Mem, _zeroDelta)
	assert.InDelta(t, 0.0, result.Disk, _zeroDelta)
	assert.InDelta(t, 0.0, result.GPU, _zeroDelta)

	result = r1.Add(Resources{})
	assert.InDelta(t, 1.0, result.CPU, _zeroDelta)
	assert.InDelta(t, 0.0, result.Mem, _zeroDelta)
	assert.InDelta(t, 0.0, result.Disk, _zeroDelta)
	assert.InDelta(t, 0.0, result.GPU, _zeroDelta)

	r2 := Resources{
		CPU:  4.0,
		Mem:  3.0,
		Disk: 2.0,
		GPU:  1.0,
	}
	result = r1.Add(r2)
	assert.InDelta(t, 5.0, result.CPU, _zeroDelta)
	assert.InDelta(t, 3.0, result.Mem, _zeroDelta)
	assert.InDelta(t, 2.0, result.Disk, _zeroDelta)
	assert.InDelta(t, 1.0, result.GPU, _zeroDelta)
}

func TestTrySubtract(t *testing.T) {
	empty := Resources{}
	r1 := Resources{
		CPU:  1.0,
		Mem:  2.0,
		Disk: 3.0,
		GPU:  4.0,
	}

	res, ok := empty.TrySubtract(empty)
	assert.True(t, ok)
	assert.InDelta(t, 0.0, res.CPU, _zeroDelta)
	assert.InDelta(t, 0.0, res.Mem, _zeroDelta)
	assert.InDelta(t, 0.0, res.Disk, _zeroDelta)
	assert.InDelta(t, 0.0, res.GPU, _zeroDelta)

	_, ok = empty.TrySubtract(r1)
	assert.False(t, ok)

	r2 := r1
	res, ok = r2.TrySubtract(r1)
	assert.True(t, ok)
	assert.InDelta(t, 0.0, res.CPU, _zeroDelta)
	assert.InDelta(t, 0.0, res.Mem, _zeroDelta)
	assert.InDelta(t, 0.0, res.Disk, _zeroDelta)
	assert.InDelta(t, 0.0, res.GPU, _zeroDelta)

	res, ok = r1.TrySubtract(empty)
	assert.True(t, ok)
	assert.InDelta(t, 1.0, res.CPU, _zeroDelta)
	assert.InDelta(t, 2.0, res.Mem, _zeroDelta)
	assert.InDelta(t, 3.0, res.Disk, _zeroDelta)
	assert.InDelta(t, 4.0, res.GPU, _zeroDelta)

	r3 := Resources{
		CPU:  5.0,
		Mem:  6.0,
		Disk: 7.0,
		GPU:  8.0,
	}
	res, ok = r3.TrySubtract(r1)
	assert.NotNil(t, res)
	assert.InDelta(t, 4.0, res.CPU, _zeroDelta)
	assert.InDelta(t, 4.0, res.Mem, _zeroDelta)
	assert.InDelta(t, 4.0, res.Disk, _zeroDelta)
	assert.InDelta(t, 4.0, res.GPU, _zeroDelta)

	// r3 is more than r1
	_, ok = r1.TrySubtract(r3)
	assert.False(t, ok)
}

func TestFromOfferMap(t *testing.T) {
	rs := []*mesos.Resource{
		util.NewMesosResourceBuilder().WithName("cpus").WithValue(1.0).Build(),
		util.NewMesosResourceBuilder().WithName("mem").WithValue(2.0).Build(),
		util.NewMesosResourceBuilder().WithName("disk").WithValue(3.0).Build(),
		util.NewMesosResourceBuilder().WithName("gpus").WithValue(4.0).Build(),
		util.NewMesosResourceBuilder().WithName("custom").WithValue(5.0).Build(),
	}

	offer := mesos.Offer{
		Resources: rs,
	}

	result := FromOfferMap(map[string]*mesos.Offer{"o1": &offer})
	assert.InDelta(t, 1.0, result.CPU, _zeroDelta)
	assert.InDelta(t, 2.0, result.Mem, _zeroDelta)
	assert.InDelta(t, 3.0, result.Disk, _zeroDelta)
	assert.InDelta(t, 4.0, result.GPU, _zeroDelta)

	result = FromOfferMap(map[string]*mesos.Offer{
		"o1": &offer,
		"o2": &offer,
	})
	assert.InDelta(t, 2.0, result.CPU, _zeroDelta)
	assert.InDelta(t, 4.0, result.Mem, _zeroDelta)
	assert.InDelta(t, 6.0, result.Disk, _zeroDelta)
	assert.InDelta(t, 8.0, result.GPU, _zeroDelta)
}

func TestFromOffers(t *testing.T) {
	rs := []*mesos.Resource{
		util.NewMesosResourceBuilder().WithName("cpus").WithValue(1.0).Build(),
		util.NewMesosResourceBuilder().WithName("mem").WithValue(2.0).Build(),
		util.NewMesosResourceBuilder().WithName("disk").WithValue(3.0).Build(),
		util.NewMesosResourceBuilder().WithName("gpus").WithValue(4.0).Build(),
		util.NewMesosResourceBuilder().WithName("custom").WithValue(5.0).Build(),
	}

	offer := mesos.Offer{
		Resources: rs,
	}

	result := FromOffers([]*mesos.Offer{&offer})
	assert.InDelta(t, 1.0, result.CPU, _zeroDelta)
	assert.InDelta(t, 2.0, result.Mem, _zeroDelta)
	assert.InDelta(t, 3.0, result.Disk, _zeroDelta)
	assert.InDelta(t, 4.0, result.GPU, _zeroDelta)

	result = FromOffers([]*mesos.Offer{
		&offer,
		&offer,
	})
	assert.InDelta(t, 2.0, result.CPU, _zeroDelta)
	assert.InDelta(t, 4.0, result.Mem, _zeroDelta)
	assert.InDelta(t, 6.0, result.Disk, _zeroDelta)
	assert.InDelta(t, 8.0, result.GPU, _zeroDelta)
}

func TestFromResourceConfig(t *testing.T) {
	result := FromResourceConfig(&task.ResourceConfig{
		CpuLimit:    1.0,
		MemLimitMb:  2.0,
		DiskLimitMb: 3.0,
		GpuLimit:    4.0,
	})
	assert.InDelta(t, 1.0, result.CPU, _zeroDelta)
	assert.InDelta(t, 2.0, result.Mem, _zeroDelta)
	assert.InDelta(t, 3.0, result.Disk, _zeroDelta)
	assert.InDelta(t, 4.0, result.GPU, _zeroDelta)
}

func TestMinimum(t *testing.T) {
	r1 := Minimum(
		Resources{
			CPU: 1.0,
			Mem: 2.0,
		},
		Resources{
			CPU:  2.0,
			Mem:  2.0,
			Disk: 2.0,
		},
	)

	assert.InDelta(t, 1.0, r1.CPU, _zeroDelta)
	assert.InDelta(t, 2.0, r1.Mem, _zeroDelta)
	assert.InDelta(t, 0.0, r1.Disk, _zeroDelta)
	assert.InDelta(t, 0.0, r1.GPU, _zeroDelta)
}

func TestNonEmptyFields(t *testing.T) {
	r1 := Resources{}
	assert.True(t, r1.Empty())
	assert.Empty(t, r1.NonEmptyFields())

	r2 := Resources{
		CPU:  1.0,
		Disk: 2.0,
	}
	assert.False(t, r2.Empty())
	assert.Equal(t, []string{"cpus", "disk"}, r2.NonEmptyFields())
}

func TestScarceResourceType(t *testing.T) {
	testTable := []struct {
		scarceResourceType []string
		reqResource        Resources
		agentResources     Resources
		expected           bool
		msg                string
	}{
		{
			msg:                "GPU task can schedule on GPU machine",
			scarceResourceType: []string{"GPU"},
			reqResource:        createResource(1.0, 1.0, 100.0, 100.0),
			agentResources:     createResource(24.0, 4.0, 10000.0, 100000.0),
			expected:           true,
		},
		{
			msg:                "non-GPU task can not schedule on GPU machine",
			scarceResourceType: []string{"GPU"},
			reqResource:        createResource(1.0, 0, 100.0, 100.0),
			agentResources:     createResource(24.0, 4.0, 10000.0, 100000.0),
			expected:           false,
		},
	}

	for _, tt := range testTable {
		assert.Equal(t, tt.reqResource.GetCPU(), 1.0)
		assert.Equal(t, tt.reqResource.GetMem(), 100.0)
		assert.Equal(t, tt.reqResource.GetDisk(), 100.0)
		assert.Equal(t, tt.agentResources.GetGPU(), 4.0)
		for _, resourceType := range tt.scarceResourceType {
			assert.NotEqual(t, HasResourceType(tt.agentResources, tt.reqResource, resourceType), tt.expected)
		}
	}
}

func TestRevocableResources(t *testing.T) {
	offer := createUnreservedMesosOffer("offer-1")
	offerMap := map[string]*mesos.Offer{}
	offerMap["offer-1"] = offer
	offerMap["offer-2"] = nil
	revocableOffers, nonRevocableOffers := FilterRevocableMesosResources(
		FromOffersMapToMesosResources(offerMap))
	for _, r := range revocableOffers {
		assert.True(t, r.GetRevocable() != nil)
	}
	for _, r := range nonRevocableOffers {
		assert.True(t, r.GetRevocable() == nil)
	}
}
