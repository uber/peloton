// @generated AUTO GENERATED - DO NOT EDIT! 117d51fa2854b0184adc875246a35929bbbf0a91

// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package metrics

// Type represents a type of information, it can be cpu usage, memory usage, disk usage, etc.
type Type struct {
	Name       string
	Unit       string
	derivation Derivation
	Inherited  bool // Inherited represents if the metric is inherited from entities or not.
}

// Derivation returns the derivation of this type.
func (metricType Type) Derivation() Derivation {
	return metricType.derivation
}

// SetDerivation sets the derivation of this type and gives an error if the derivation introduces a cycle in the
// derivation rules.
func (metricType *Type) SetDerivation(value Derivation) error {
	metricType.derivation = value
	_, err := TopSort(*metricType)
	if err != nil {
		metricType.derivation = nil
		return err
	}
	return nil
}

var (
	// CPUTotal represents the total cpu for a group, each cpu adds 100 %, so a 24 core machine will have 2400 % cpu.
	CPUTotal = Type{
		Name:      "cpu_total",
		Unit:      "%",
		Inherited: false,
	}
	// CPUUsed represents the used cpu usage for a group or an entity.
	CPUUsed = Type{
		Name:      "cpu_used",
		Unit:      "%",
		Inherited: true,
	}
	// CPUFree represents the free cpu usage for a group.
	CPUFree = Type{
		Name:       "cpu_free",
		Unit:       "%",
		Inherited:  false,
		derivation: computeFree(CPUTotal, CPUUsed),
	}

	// MemoryTotal represents the total memory.
	MemoryTotal = Type{
		Name:      "memory_total",
		Unit:      "bytes",
		Inherited: false,
	}
	// MemoryUsed represents the used memory for a group or an entity.
	MemoryUsed = Type{
		Name:      "memory_used",
		Unit:      "bytes",
		Inherited: true,
	}
	// MemoryFree represents the free memory for a group.
	MemoryFree = Type{
		Name:       "memory_free",
		Unit:       "bytes",
		Inherited:  false,
		derivation: computeFree(MemoryTotal, MemoryUsed),
	}

	// DiskTotal represents the total disk for a group
	DiskTotal = Type{
		Name:      "disk_total",
		Unit:      "bytes",
		Inherited: false,
	}
	// DiskUsed represents the used disk for a group or an entity.
	DiskUsed = Type{
		Name:      "disk_used",
		Unit:      "bytes",
		Inherited: true,
	}
	// DiskFree represents the free disk for a group.
	DiskFree = Type{
		Name:       "disk_free",
		Unit:       "bytes",
		Inherited:  false,
		derivation: computeFree(DiskTotal, DiskUsed),
	}

	// NetworkTotal represents the total network for a group
	NetworkTotal = Type{
		Name:      "network_total",
		Unit:      "bits",
		Inherited: false,
	}
	// NetworkUsed represents the used network for a group or an entity.
	NetworkUsed = Type{
		Name:      "network_used",
		Unit:      "bits",
		Inherited: true,
	}
	// NetworkFree represents the free network for a group.
	NetworkFree = Type{
		Name:       "network_free",
		Unit:       "bits",
		Inherited:  false,
		derivation: computeFree(NetworkTotal, NetworkUsed),
	}

	// GPUTotal represents the total gpu for a group, each gpu adds 100 %, so a 24 core gpu will have 2400 % gpu.
	GPUTotal = Type{
		Name:      "gpu_total",
		Unit:      "%",
		Inherited: false,
	}
	// GPUUsed represents the used gpu usage for a group or an entity.
	GPUUsed = Type{
		Name:      "gpu_used",
		Unit:      "%",
		Inherited: true,
	}
	// GPUFree represents the free gpu usage for a group.
	GPUFree = Type{
		Name:       "gpu_free",
		Unit:       "%",
		Inherited:  false,
		derivation: computeFree(GPUTotal, GPUUsed),
	}

	// FileDescriptorsTotal represents the total number of file descriptors available for a group.
	FileDescriptorsTotal = Type{
		Name:      "file_descriptors_total",
		Unit:      "#",
		Inherited: false,
	}
	// FileDescriptorsUsed represents the number of used file descriptors available for a group or entity.
	FileDescriptorsUsed = Type{
		Name:      "file_descriptors_used",
		Unit:      "#",
		Inherited: true,
	}
	// FileDescriptorsFree represents the number of free file descriptors available for a group.
	FileDescriptorsFree = Type{
		Name:       "file_descriptors_free",
		Unit:       "#",
		Inherited:  false,
		derivation: computeFree(FileDescriptorsTotal, FileDescriptorsUsed),
	}

	// PortsTotal represents the total number of ports available for a group.
	PortsTotal = Type{
		Name:      "ports_total",
		Unit:      "#",
		Inherited: false,
	}
	// PortsUsed represents the used number of ports for a group.
	PortsUsed = Type{
		Name:      "ports_used",
		Unit:      "#",
		Inherited: true,
	}
	// PortsFree represents the free number of ports for a group.
	PortsFree = Type{
		Name:       "ports_free",
		Unit:       "#",
		Inherited:  false,
		derivation: computeFree(PortsTotal, PortsUsed),
	}
)

const (
	// Byte is the unit of bytes
	Byte = 1.0
	// KiB is the unit Kibibytes
	KiB = 1024.0 * Byte
	// MiB is the unit Mebibytes
	MiB = 1024.0 * KiB
	// GiB is the unit Gibibytes
	GiB = 1024.0 * MiB
	// TiB is the unit Tebibytes
	TiB = 1024.0 * GiB
	// Bit is the unit bits
	Bit = 1.0
	// KiBit is the unit Kibibits
	KiBit = 1024.0 * Bit
	// MiBit is the unit Mebibits
	MiBit = 1024.0 * KiBit
	// GiBit is the unit Gibibits
	GiBit = 1024.0 * MiBit
)

type derivation struct {
	dependencies []Type
	calculation  func(metricType Type, metricSet *Set)
}

func (derivation *derivation) Dependencies() []Type {
	return derivation.dependencies
}

func (derivation *derivation) Calculate(metricType Type, metricSet *Set) {
	derivation.calculation(metricType, metricSet)
}

func computeFree(total, used Type) Derivation {
	return &derivation{
		dependencies: []Type{total, used},
		calculation: func(free Type, metricSet *Set) {
			metricSet.Set(free, metricSet.Get(total)-metricSet.Get(used))
		},
	}
}
