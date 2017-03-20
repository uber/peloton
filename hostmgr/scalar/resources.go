package scalar

import (
	"math"
	mesos "mesos/v1"
	"peloton/api/task/config"
	"sync"

	"code.uber.internal/infra/peloton/util"
)

// Resources is a non-thread safe helper struct holding recognized resources.
type Resources struct {
	// TODO(zhitao): Figure out a way to upgrade to newer version of
	// https://github.com/uber-go/atomic and use `Float64`.
	CPU  float64
	Mem  float64
	Disk float64
	GPU  float64
}

// a safe less than or equal to comparator which takes epsilon into consideration.
// TODO(zhitao): Explore fixed point number in T777007.
func lessThanOrEqual(f1, f2 float64) bool {
	v := f1 - f2
	if math.Abs(v) < util.ResourceEspilon {
		return true
	}
	return v < 0
}

// HasGPU is a special condition to ensure exclusive protection for GPU.
func (r *Resources) HasGPU() bool {
	return math.Abs(r.GPU) > util.ResourceEspilon
}

// Contains determines whether current Resources is large enough to contain
// the other one.
func (r *Resources) Contains(other *Resources) bool {
	return lessThanOrEqual(other.CPU, r.CPU) &&
		lessThanOrEqual(other.Mem, r.Mem) &&
		lessThanOrEqual(other.Disk, r.Disk) &&
		lessThanOrEqual(other.GPU, r.GPU)
}

// Add atomically add another scalar resources onto current one.
func (r *Resources) Add(other *Resources) *Resources {
	return &Resources{
		CPU:  r.CPU + other.CPU,
		Mem:  r.Mem + other.Mem,
		Disk: r.Disk + other.Disk,
		GPU:  r.GPU + other.GPU,
	}
}

// TrySubtract attempts to subtract another scalar resources from current one
// , but returns nil if other has more resources.
func (r *Resources) TrySubtract(other *Resources) *Resources {
	if !r.Contains(other) {
		return nil
	}
	return r.Subtract(other)
}

// Subtract another scalar resources from current one and return a new copy of result.
func (r *Resources) Subtract(other *Resources) *Resources {
	return &Resources{
		CPU:  r.CPU - other.CPU,
		Mem:  r.Mem - other.Mem,
		Disk: r.Disk - other.Disk,
		GPU:  r.GPU - other.GPU,
	}
}

// NonEmptyFields returns corresponding Mesos resource names for fields which are not empty.
func (r *Resources) NonEmptyFields() []string {
	var nonEmptyFields []string
	if math.Abs(r.CPU) > util.ResourceEspilon {
		nonEmptyFields = append(nonEmptyFields, "cpus")
	}
	if math.Abs(r.Mem) > util.ResourceEspilon {
		nonEmptyFields = append(nonEmptyFields, "mem")
	}
	if math.Abs(r.Disk) > util.ResourceEspilon {
		nonEmptyFields = append(nonEmptyFields, "disk")
	}
	if math.Abs(r.GPU) > util.ResourceEspilon {
		nonEmptyFields = append(nonEmptyFields, "gpus")
	}

	return nonEmptyFields
}

// Empty returns whether all fields are empty now.
func (r *Resources) Empty() bool {
	return len(r.NonEmptyFields()) == 0
}

// FromResourceConfig creats a new instance of `Resources` frmo a `ResourceConfig`.
func FromResourceConfig(rc *config.ResourceConfig) (r Resources) {
	r.CPU = rc.GetCpuLimit()
	r.Mem = rc.GetMemLimitMb()
	r.Disk = rc.GetDiskLimitMb()
	r.GPU = rc.GetGpuLimit()
	return r
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
		r = *(r.Add(&tmp))
	}

	return r
}

// FromOffer returns the scalar Resources from an offer.
func FromOffer(offer *mesos.Offer) (r Resources) {
	return FromMesosResources(offer.GetResources())
}

// FromOfferMap returns the scalar Resources from given id to offer map.
func FromOfferMap(offerMap map[string]*mesos.Offer) (r Resources) {
	for _, offer := range offerMap {
		tmp := FromOffer(offer)
		r = *(r.Add(&tmp))
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
