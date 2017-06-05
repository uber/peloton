package scalar

import (
	"math"

	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/util"
	log "github.com/Sirupsen/logrus"
)

// Resources is a non-thread safe helper struct holding recognized resources.
type Resources struct {
	CPU    float64
	MEMORY float64
	DISK   float64
	GPU    float64
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
	if math.Abs(v) < util.ResourceEspilon {
		return true
	}
	return v < 0
}

// LessThanOrEqual determines current Resources is LessThanOrEqual
// the other one.
func (r *Resources) LessThanOrEqual(other *Resources) bool {
	return lessThanOrEqual(r.CPU, other.CPU) &&
		lessThanOrEqual(r.MEMORY, other.MEMORY) &&
		lessThanOrEqual(r.DISK, other.DISK) &&
		lessThanOrEqual(r.GPU, other.GPU)
}

// ConvertToResmgrResource converts task resource config to scalar.Resources
func ConvertToResmgrResource(resource *task.ResourceConfig) *Resources {
	return &Resources{
		CPU:    resource.GetCpuLimit(),
		DISK:   resource.GetDiskLimitMb(),
		GPU:    resource.GpuLimit,
		MEMORY: resource.GetMemLimitMb(),
	}
}

// Subtract another scalar resources from current one and return a new copy of result.
func (r *Resources) Subtract(other *Resources) *Resources {
	var result Resources
	if r.CPU < other.CPU {
		log.WithFields(log.Fields{
			"Subtracted From CPU ": r.CPU,
			"Subracted Value ":     other.CPU,
		}).Warn("Subracted value is Greater")
		result.CPU = float64(0)
	} else {
		result.CPU = r.CPU - other.CPU
	}

	if r.GPU < other.GPU {
		log.WithFields(log.Fields{
			"Subtracted From GPU ": r.GPU,
			"Subracted Value ":     other.GPU,
		}).Warn("Subracted value is Greater")
		result.GPU = float64(0)
	} else {
		result.GPU = r.GPU - other.GPU
	}

	if r.MEMORY < other.MEMORY {
		log.WithFields(log.Fields{
			"Subtracted From Memory ": r.MEMORY,
			"Subracted Value ":        other.MEMORY,
		}).Warn("Subracted value is Greater")
		result.MEMORY = float64(0)
	} else {
		result.MEMORY = r.MEMORY - other.MEMORY
	}

	if r.DISK < other.DISK {
		log.WithFields(log.Fields{
			"Subtracted From DISK ": r.DISK,
			"Subracted Value ":      other.DISK,
		}).Warn("Subracted value is Greater")
		result.DISK = float64(0)
	} else {
		result.DISK = r.DISK - other.DISK
	}
	return &result
}
