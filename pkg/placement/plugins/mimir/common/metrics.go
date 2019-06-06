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

package mimir

import "github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/metrics"

var (
	// CPUAvailable represents the available cpu in a host offer, each cpu adds 100 %, so a 24 core machine will
	// have 2400 % cpu.
	CPUAvailable = metrics.Type{
		Name:      "cpu_available",
		Unit:      "%",
		Inherited: false,
	}
	// CPUReserved represents the reserved cpu on a host offer or of a task.
	CPUReserved = metrics.Type{
		Name:      "cpu_reserved",
		Unit:      "%",
		Inherited: true,
	}
	// CPUFree represents the free cpu for a host offer.
	CPUFree = metrics.Type{
		Name:      "cpu_free",
		Unit:      "%",
		Inherited: false,
	}

	// GPUAvailable represents the available gpu in a host offer, each gpu adds 100 %, so a 384 core gpu will
	// have 38400 % cpu.
	GPUAvailable = metrics.Type{
		Name:      "gpu_available",
		Unit:      "%",
		Inherited: false,
	}
	// GPUReserved represents the reserved gpu on a host offer or of a task.
	GPUReserved = metrics.Type{
		Name:      "gpu_reserved",
		Unit:      "%",
		Inherited: true,
	}
	// GPUFree represents the free gpu for a host offer.
	GPUFree = metrics.Type{
		Name:      "gpu_free",
		Unit:      "%",
		Inherited: false,
	}

	// MemoryAvailable represents the available memory on a host offer.
	MemoryAvailable = metrics.Type{
		Name:      "memory_available",
		Unit:      "bytes",
		Inherited: false,
	}
	// MemoryReserved represents the reserved memory on a host offer or of a task.
	MemoryReserved = metrics.Type{
		Name:      "memory_reserved",
		Unit:      "bytes",
		Inherited: true,
	}
	// MemoryFree represents the free memory for a host offer.
	MemoryFree = metrics.Type{
		Name:      "memory_free",
		Unit:      "bytes",
		Inherited: false,
	}

	// DiskAvailable represents the available disk on a host offer.
	DiskAvailable = metrics.Type{
		Name:      "disk_available",
		Unit:      "bytes",
		Inherited: false,
	}
	// DiskReserved represents the reserved disk on a host offer or of a task.
	DiskReserved = metrics.Type{
		Name:      "disk_reserved",
		Unit:      "bytes",
		Inherited: true,
	}
	// DiskFree represents the free disk for a host offer.
	DiskFree = metrics.Type{
		Name:      "disk_free",
		Unit:      "bytes",
		Inherited: false,
	}

	// PortsAvailable represents the available ports on a host offer.
	PortsAvailable = metrics.Type{
		Name:      "ports_available",
		Unit:      "#",
		Inherited: false,
	}
	// PortsReserved represents the reserved ports on a host offer or of a task.
	PortsReserved = metrics.Type{
		Name:      "ports_reserved",
		Unit:      "#",
		Inherited: true,
	}
	// PortsFree represents the free ports for a host offer.
	PortsFree = metrics.Type{
		Name:      "ports_free",
		Unit:      "#",
		Inherited: false,
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
	dependencies []metrics.Type
	calculation  func(metricType metrics.Type, metricSet *metrics.Set)
}

func (derivation *derivation) Dependencies() []metrics.Type {
	return derivation.dependencies
}

func (derivation *derivation) Calculate(metricType metrics.Type, metricSet *metrics.Set) {
	derivation.calculation(metricType, metricSet)
}

func free(available, reserved metrics.Type) metrics.Derivation {
	return &derivation{
		dependencies: []metrics.Type{available, reserved},
		calculation: func(free metrics.Type, set *metrics.Set) {
			set.Set(free, set.Get(available)-set.Get(reserved))
		},
	}
}
