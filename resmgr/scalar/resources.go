package scalar

import (
	"math"

	"code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/util"

	log "github.com/sirupsen/logrus"
)

// ZeroResource represents the minimum value of a resource
var ZeroResource = &Resources{
	CPU:    float64(0),
	GPU:    float64(0),
	DISK:   float64(0),
	MEMORY: float64(0),
}

// Resources is a non-thread safe helper struct holding recognized resources.
type Resources struct {
	CPU    float64
	MEMORY float64
	DISK   float64
	GPU    float64
}

// GetCPU returns the CPU resource
func (r *Resources) GetCPU() float64 {
	return r.CPU
}

// GetDisk returns the disk resource
func (r *Resources) GetDisk() float64 {
	return r.DISK
}

// GetMem returns the memory resource
func (r *Resources) GetMem() float64 {
	return r.MEMORY
}

// GetGPU returns the GPU resource
func (r *Resources) GetGPU() float64 {
	return r.GPU
}

// Get returns the kind of resource
func (r *Resources) Get(kind string) float64 {
	switch kind {
	case common.CPU:
		return r.GetCPU()
	case common.GPU:
		return r.GetGPU()
	case common.MEMORY:
		return r.GetMem()
	case common.DISK:
		return r.GetDisk()
	}
	return float64(0)
}

// Set sets the kind of resource with the value
func (r *Resources) Set(kind string, value float64) {
	switch kind {
	case common.CPU:
		r.CPU = value
	case common.GPU:
		r.GPU = value
	case common.MEMORY:
		r.MEMORY = value
	case common.DISK:
		r.DISK = value
	}
}

// Add atomically add another scalar resources onto current one.
func (r *Resources) Add(other *Resources) *Resources {
	return &Resources{
		CPU:    r.CPU + other.CPU,
		MEMORY: r.MEMORY + other.MEMORY,
		DISK:   r.DISK + other.DISK,
		GPU:    r.GPU + other.GPU,
	}
}

func lessThanOrEqual(f1, f2 float64) bool {
	v := f1 - f2
	if math.Abs(v) < util.ResourceEpsilon {
		return true
	}
	return v < 0
}

// LessThanOrEqual determines current Resources is less than or equal
// the other one.
func (r *Resources) LessThanOrEqual(other *Resources) bool {
	return lessThanOrEqual(r.CPU, other.CPU) &&
		lessThanOrEqual(r.MEMORY, other.MEMORY) &&
		lessThanOrEqual(r.DISK, other.DISK) &&
		lessThanOrEqual(r.GPU, other.GPU)
}

func equal(f1, f2 float64) bool {
	return f1 == f2
}

// Equal determines current Resources is equal to
// the other one.
func (r *Resources) Equal(other *Resources) bool {
	return equal(r.CPU, other.CPU) &&
		equal(r.MEMORY, other.MEMORY) &&
		equal(r.DISK, other.DISK) &&
		equal(r.GPU, other.GPU)
}

// ConvertToResmgrResource converts task resource config to scalar.Resources
func ConvertToResmgrResource(resource *task.ResourceConfig) *Resources {
	return &Resources{
		CPU:    resource.GetCpuLimit(),
		DISK:   resource.GetDiskLimitMb(),
		GPU:    resource.GetGpuLimit(),
		MEMORY: resource.GetMemLimitMb(),
	}
}

// Subtract another scalar resources from current one and return a new copy of result.
func (r *Resources) Subtract(other *Resources) *Resources {
	var result Resources
	if r.CPU < other.CPU {
		log.WithFields(log.Fields{
			"Subtracted From CPU ": r.CPU,
			"Subtracted Value ":    other.CPU,
		}).Debug("Subtracted value is Greater")
		result.CPU = float64(0)
	} else {
		result.CPU = r.CPU - other.CPU
		if result.CPU < util.ResourceEpsilon {
			result.CPU = float64(0)
		}
	}

	if r.GPU < other.GPU {
		log.WithFields(log.Fields{
			"Subtracted From GPU ": r.GPU,
			"Subtracted Value ":    other.GPU,
		}).Debug("Subtracted value is Greater")
		result.GPU = float64(0)
	} else {
		result.GPU = r.GPU - other.GPU
		if result.GPU < util.ResourceEpsilon {
			result.GPU = float64(0)
		}
	}

	if r.MEMORY < other.MEMORY {
		log.WithFields(log.Fields{
			"Subtracted From Memory ": r.MEMORY,
			"Subtracted Value ":       other.MEMORY,
		}).Debug("Subtracted value is Greater")
		result.MEMORY = float64(0)
	} else {
		result.MEMORY = r.MEMORY - other.MEMORY
		if result.MEMORY < util.ResourceEpsilon {
			result.MEMORY = float64(0)
		}
	}

	if r.DISK < other.DISK {
		log.WithFields(log.Fields{
			"Subtracted From DISK ": r.DISK,
			"Subtracted Value ":     other.DISK,
		}).Debug("Subtracted value is Greater")
		result.DISK = float64(0)
	} else {
		result.DISK = r.DISK - other.DISK
		if result.DISK < util.ResourceEpsilon {
			result.DISK = float64(0)
		}
	}
	return &result
}
