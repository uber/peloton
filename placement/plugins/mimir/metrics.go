package mimir

import "code.uber.internal/infra/peloton/mimir-lib/model/metrics"

var (
	// CPUAvailable represents the available cpu in a host offer, each cpu adds 100 %, so a 24 core machine will
	// have 2400 % cpu.
	CPUAvailable = metrics.MetricType{
		Name: "cpu_available",
		Unit: "%",
	}
	// CPUReserved represents the reserved cpu on a host offer or of a task.
	CPUReserved = metrics.MetricType{
		Name: "cpu_reserved",
		Unit: "%",
	}
	// CPUFree represents the free cpu for a host offer.
	CPUFree = metrics.MetricType{
		Name: "cpu_free",
		Unit: "%",
	}

	// GPUAvailable represents the available gpu in a host offer, each gpu adds 100 %, so a 384 core gpu will
	// have 38400 % cpu.
	GPUAvailable = metrics.MetricType{
		Name: "gpu_available",
		Unit: "%",
	}
	// GPUReserved represents the reserved gpu on a host offer or of a task.
	GPUReserved = metrics.MetricType{
		Name: "gpu_reserved",
		Unit: "%",
	}
	// GPUFree represents the free gpu for a host offer.
	GPUFree = metrics.MetricType{
		Name: "gpu_free",
		Unit: "%",
	}

	// MemoryAvailable represents the available memory on a host offer.
	MemoryAvailable = metrics.MetricType{
		Name: "memory_available",
		Unit: "bytes",
	}
	// MemoryReserved represents the reserved memory on a host offer or of a task.
	MemoryReserved = metrics.MetricType{
		Name: "memory_reserved",
		Unit: "bytes",
	}
	// MemoryFree represents the free memory for a host offer.
	MemoryFree = metrics.MetricType{
		Name: "memory_free",
		Unit: "bytes",
	}

	// DiskAvailable represents the available disk on a host offer.
	DiskAvailable = metrics.MetricType{
		Name: "disk_available",
		Unit: "bytes",
	}
	// DiskReserved represents the reserved disk on a host offer or of a task.
	DiskReserved = metrics.MetricType{
		Name: "disk_reserved",
		Unit: "bytes",
	}
	// DiskFree represents the free disk for a host offer.
	DiskFree = metrics.MetricType{
		Name: "disk_free",
		Unit: "bytes",
	}

	// PortsAvailable represents the available ports on a host offer.
	PortsAvailable = metrics.MetricType{
		Name: "ports_available",
		Unit: "#",
	}
	// PortsReserved represents the reserved ports on a host offer or of a task.
	PortsReserved = metrics.MetricType{
		Name: "ports_reserved",
		Unit: "#",
	}
	// PortsFree represents the free ports for a host offer.
	PortsFree = metrics.MetricType{
		Name: "ports_free",
		Unit: "#",
	}
)

var _ = initializeDerivations()

func initializeDerivations() bool {
	CPUFree.SetDerivation(free(CPUAvailable, CPUReserved))
	GPUFree.SetDerivation(free(GPUAvailable, GPUReserved))
	MemoryFree.SetDerivation(free(MemoryAvailable, MemoryReserved))
	DiskFree.SetDerivation(free(DiskAvailable, DiskReserved))
	PortsFree.SetDerivation(free(PortsAvailable, PortsReserved))
	return true
}

type derivation struct {
	dependencies []metrics.MetricType
	calculation  func(metricType metrics.MetricType, metricSet *metrics.MetricSet)
}

func (derivation *derivation) Dependencies() []metrics.MetricType {
	return derivation.dependencies
}

func (derivation *derivation) Calculate(metricType metrics.MetricType, metricSet *metrics.MetricSet) {
	derivation.calculation(metricType, metricSet)
}

func free(available, reserved metrics.MetricType) metrics.Derivation {
	return &derivation{
		dependencies: []metrics.MetricType{available, reserved},
		calculation: func(free metrics.MetricType, metricSet *metrics.MetricSet) {
			metricSet.Set(free, metricSet.Get(available)-metricSet.Get(reserved))
		},
	}
}
