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
	"fmt"
	"math"
	"sync"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"

	"github.com/uber/peloton/pkg/common/util"
)

// Resources is a non-thread safe helper struct holding recognized resources.
type Resources struct {
	CPU  float64
	Mem  float64
	Disk float64
	GPU  float64
}

// a safe less than or equal to comparator which takes epsilon into consideration.
// TODO(zhitao): Explore fixed point number in T777007.
func lessThanOrEqual(f1, f2 float64) bool {
	v := f1 - f2
	if math.Abs(v) < util.ResourceEpsilon {
		return true
	}
	return v < 0
}

// a safe less than to comparator which takes epsilon into consideration.
func lessThan(f1, f2 float64) bool {
	v := f1 - f2
	if math.Abs(v) < util.ResourceEpsilon {
		return false
	}
	return v < 0
}

// GetCPU returns the CPU resource
func (r Resources) GetCPU() float64 {
	return r.CPU
}

// GetDisk returns the Disk resource
func (r Resources) GetDisk() float64 {
	return r.Disk
}

// GetMem returns the Memory resource
func (r Resources) GetMem() float64 {
	return r.Mem
}

// GetGPU returns the GPU resource
func (r Resources) GetGPU() float64 {
	return r.GPU
}

// HasGPU is a special condition to ensure exclusive protection for GPU.
func (r Resources) HasGPU() bool {
	return math.Abs(r.GPU) > util.ResourceEpsilon
}

// Contains determines whether current Resources is large enough to contain
// the other one.
func (r Resources) Contains(other Resources) bool {
	return lessThanOrEqual(other.CPU, r.CPU) &&
		lessThanOrEqual(other.Mem, r.Mem) &&
		lessThanOrEqual(other.Disk, r.Disk) &&
		lessThanOrEqual(other.GPU, r.GPU)
}

// Compare method compares current Resources with the other one, return
// true if current Resources is strictly larger than or equal to / less than
// the other one (based on boolean cmpLess), false if not. Fields in the
// other one are ignore if they are 0.
func (r Resources) Compare(other Resources, cmpLess bool) bool {
	// lessThan(r.XXX, other.XXX) != cmpLess XOR's those two values.
	// cmpLess flag toggles either it's comparing the current one is
	// greater than or equal to the other (when set to false), or
	// the current one is less than the other one (when set to true).
	// The following is the possible combinations of inputs, and their
	// result:
	//
	//   r.XXX < other.XXX  cmpLess  XOR    return
	//   false              false    false  true (current >= other)
	//   true               false    true   false (current < other)
	//   false              true     true   false (current >= other)
	//   true               true     false  true (current < other)
	if other.CPU > 0 && lessThan(r.CPU, other.CPU) != cmpLess {
		return false
	}
	if other.Mem > 0 && lessThan(r.Mem, other.Mem) != cmpLess {
		return false
	}
	if other.GPU > 0 && lessThan(r.GPU, other.GPU) != cmpLess {
		return false
	}
	if other.Disk > 0 && lessThan(r.Disk, other.Disk) != cmpLess {
		return false
	}
	return true
}

// Add atomically add another scalar resources onto current one.
func (r Resources) Add(other Resources) Resources {
	return Resources{
		CPU:  r.CPU + other.CPU,
		Mem:  r.Mem + other.Mem,
		Disk: r.Disk + other.Disk,
		GPU:  r.GPU + other.GPU,
	}
}

// TrySubtract attempts to subtract another scalar resources from current one
// , but returns false if other has more resources.
func (r Resources) TrySubtract(other Resources) (Resources, bool) {
	if !r.Contains(other) {
		return Resources{}, false
	}
	return r.Subtract(other), true
}

// Subtract another scalar resources from current one and return a new copy of result.
func (r Resources) Subtract(other Resources) Resources {
	return Resources{
		CPU:  r.CPU - other.CPU,
		Mem:  r.Mem - other.Mem,
		Disk: r.Disk - other.Disk,
		GPU:  r.GPU - other.GPU,
	}
}

// NonEmptyFields returns corresponding Mesos resource names for fields which are not empty.
func (r Resources) NonEmptyFields() []string {
	var nonEmptyFields []string
	if math.Abs(r.CPU) > util.ResourceEpsilon {
		nonEmptyFields = append(nonEmptyFields, "cpus")
	}
	if math.Abs(r.Mem) > util.ResourceEpsilon {
		nonEmptyFields = append(nonEmptyFields, "mem")
	}
	if math.Abs(r.Disk) > util.ResourceEpsilon {
		nonEmptyFields = append(nonEmptyFields, "disk")
	}
	if math.Abs(r.GPU) > util.ResourceEpsilon {
		nonEmptyFields = append(nonEmptyFields, "gpus")
	}

	return nonEmptyFields
}

// Empty returns whether all fields are empty now.
func (r Resources) Empty() bool {
	return len(r.NonEmptyFields()) == 0
}

// String returns a formatted string for scalar resources
func (r Resources) String() string {
	return fmt.Sprintf("CPU:%.2f MEM:%.2f DISK:%.2f GPU:%.2f",
		r.GetCPU(), r.GetMem(), r.GetDisk(), r.GetGPU())
}

// HasResourceType validates requested resource type is present agent resource type.
func HasResourceType(agentRes, reqRes Resources, resourceType string) bool {
	switch resourceType {
	case "GPU":
		return !reqRes.HasGPU() && agentRes.HasGPU()
	}
	return false
}

// FilterRevocableMesosResources separates revocable resources
// and non-revocable resources
func FilterRevocableMesosResources(rs []*mesos.Resource) (
	revocable []*mesos.Resource, nonRevocable []*mesos.Resource) {
	return FilterMesosResources(
		rs,
		func(r *mesos.Resource) bool {
			return r.GetRevocable() != nil
		})
}

// FilterMesosResources separates Mesos resources slice into matched and
// unmatched ones.
func FilterMesosResources(rs []*mesos.Resource, filter func(*mesos.Resource) bool) (
	matched []*mesos.Resource, unmatched []*mesos.Resource) {
	for _, r := range rs {
		if filter(r) {
			matched = append(matched, r)
		} else {
			unmatched = append(unmatched, r)
		}
	}
	return matched, unmatched
}

// FromResourceConfig creates a new instance of `Resources` from a `ResourceConfig`.
func FromResourceConfig(rc *task.ResourceConfig) (r Resources) {
	r.CPU = rc.GetCpuLimit()
	r.Mem = rc.GetMemLimitMb()
	r.Disk = rc.GetDiskLimitMb()
	r.GPU = rc.GetGpuLimit()
	return r
}

// FromResourceSpec creates a new instance of `Resources` from a `ResourceSpec`
func FromResourceSpec(rc *pbpod.ResourceSpec) (r Resources) {
	r.CPU = rc.GetCpuLimit()
	r.Mem = rc.GetMemLimitMb()
	r.GPU = rc.GetGpuLimit()
	return r
}

// FromPodSpec creates a new instance of `Resources` from a `PodSpec`
func FromPodSpec(podspec *pbpod.PodSpec) (r Resources) {
	var res Resources
	for _, c := range podspec.GetContainers() {
		res = FromResourceSpec(
			c.GetResource(),
		)
		r = r.Add(res)
	}
	for _, c := range podspec.GetInitContainers() {
		res = FromResourceSpec(
			c.GetResource(),
		)
		r = r.Add(res)
	}
	return r
}

// FromResourceSpec creates a new instance of `Resources` from
// `peloton.Resources`
func FromPelotonResources(rp *peloton.Resources) (r Resources) {
	r.CPU = rp.GetCpu()
	r.Mem = rp.GetMemMb()
	r.Disk = rp.GetDiskMb()
	r.GPU = rp.GetGpu()
	return r
}

// ToPelotonResources creates a new instance of `peloton.Resources` from
// `Resources`
func ToPelotonResources(rs Resources) *peloton.Resources {
	return &peloton.Resources{
		Cpu:    rs.CPU,
		MemMb:  rs.Mem,
		DiskMb: rs.Disk,
		Gpu:    rs.GPU,
	}
}

// FromMesosResource returns the scalar Resources from a single Mesos resource object.
func FromMesosResource(resource *mesos.Resource) (r Resources) {
	value := resource.GetScalar().GetValue()
	switch name := resource.GetName(); name {
	case "cpus":
		r.CPU += value
	case "mem":
		r.Mem += value
	case "disk":
		r.Disk += value
	case "gpus":
		r.GPU += value
	}
	return r
}

// FromMesosResources returns the scalar Resources from a list of Mesos resource objects.
func FromMesosResources(resources []*mesos.Resource) (r Resources) {
	for _, resource := range resources {
		tmp := FromMesosResource(resource)
		r = r.Add(tmp)
	}

	return r
}

// FromOfferToMesosResources returns list of resources from a single offer
func FromOfferToMesosResources(offer *mesos.Offer) []*mesos.Resource {
	var resources []*mesos.Resource
	if offer == nil {
		resources = append(resources, &mesos.Resource{})
	} else {
		for _, r := range offer.GetResources() {
			resources = append(resources, r)
		}
	}
	return resources
}

// FromOffersMapToMesosResources returns list of resources from a offerMap
func FromOffersMapToMesosResources(offerMap map[string]*mesos.Offer) []*mesos.Resource {
	var resources []*mesos.Resource
	for _, offer := range offerMap {
		resources = append(resources, FromOfferToMesosResources(offer)...)
	}
	return resources
}

// FromOffer returns the scalar Resources from an offer.
func FromOffer(offer *mesos.Offer) Resources {
	if offer == nil {
		return Resources{}
	}
	return FromMesosResources(offer.GetResources())
}

// FromOffers returns the scalar Resources from given offers
func FromOffers(offers []*mesos.Offer) (r Resources) {
	for _, offer := range offers {
		tmp := FromOffer(offer)
		r = r.Add(tmp)
	}

	return r
}

// FromOfferMap returns the scalar Resources from given id to offer map.
func FromOfferMap(offerMap map[string]*mesos.Offer) (r Resources) {
	for _, offer := range offerMap {
		tmp := FromOffer(offer)
		r = r.Add(tmp)
	}

	return r
}

// Minimum returns minimum amount of resources in each type.
func Minimum(r1, r2 Resources) (m Resources) {
	m.CPU = math.Min(r1.CPU, r2.CPU)
	m.Mem = math.Min(r1.Mem, r2.Mem)
	m.Disk = math.Min(r1.Disk, r2.Disk)
	m.GPU = math.Min(r1.GPU, r2.GPU)
	return m
}

// AtomicResources is a wrapper around `Resources` provide thread safety.
type AtomicResources struct {
	sync.RWMutex

	resources Resources
}

// Get returns a copy of current value.
func (a *AtomicResources) Get() Resources {
	a.RLock()
	defer a.RUnlock()
	return a.resources
}

// Set sets value to r.
func (a *AtomicResources) Set(r Resources) {
	a.Lock()
	defer a.Unlock()
	a.resources = r
}
