package scalar

import (
	"math"
	mesos "mesos/v1"
	"peloton/api/task/config"

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

// HasGPU is a special condition to ensure exclusive protection for GPU resource.
func (r *Resources) HasGPU() bool {
	return math.Abs(r.GPU) > util.ResourceEspilon
}

// Contains determines whether current Resources is large enough to container the other one.
func (r *Resources) Contains(other *Resources) bool {
	return (r.CPU >= other.CPU &&
		r.Mem >= other.Mem &&
		r.Disk >= other.Disk &&
		r.GPU >= other.GPU)
}

// Add atomically add another scalar resources onto current one.
func (r *Resources) Add(other *Resources) {
	r.CPU += other.CPU
	r.Mem += other.Mem
	r.Disk += other.Disk
	r.GPU += other.GPU
}

// TrySubtract attempts to subtract another scalar resources from current one, but returns false
// if other has more resources while not changing current instance.
func (r *Resources) TrySubtract(other *Resources) bool {
	if !r.Contains(other) {
		return false
	}
	r.CPU -= other.CPU
	r.Mem -= other.Mem
	r.Disk -= other.Disk
	r.GPU -= other.GPU
	return true
}

// FromResourceConfig creats a new instance of `Resources` frmo a `ResourceConfig`.
func FromResourceConfig(rc *config.ResourceConfig) (r Resources) {
	r.CPU = rc.GetCpuLimit()
	r.Mem = rc.GetMemLimitMb()
	r.Disk = rc.GetDiskLimitMb()
	r.GPU = rc.GetGpuLimit()
	return r
}

// FromMesosResources returns the scalar Resources from a list of Mesos resource objects.
func FromMesosResources(resources []*mesos.Resource) (r Resources) {
	for _, resource := range resources {
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
		default:
			continue
		}
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
		r.Add(&tmp)
	}

	return r
}
