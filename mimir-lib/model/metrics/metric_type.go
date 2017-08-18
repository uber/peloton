// @generated AUTO GENERATED - DO NOT EDIT!
// Copyright (c) 2017 Uber Technologies, Inc.
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

// MetricType represents a type of information, it can be cpu usage, memory usage, disk usage, etc.
type MetricType struct {
	Name string
	Unit string
}

var (
	// CPUTotal represents the total cpu for a group, each cpu adds 100 %, so a 24 core machine will have 2400 % cpu.
	CPUTotal = MetricType{
		Name: "cpu_total",
		Unit: "%",
	}
	// CPUUsed represents the used cpu usage for a group or an entity.
	CPUUsed = MetricType{
		Name: "cpu_used",
		Unit: "%",
	}
	// CPUFree represents the free cpu usage for a group.
	CPUFree = MetricType{
		Name: "cpu_free",
		Unit: "%",
	}

	// MemoryTotal represents the total memory.
	MemoryTotal = MetricType{
		Name: "memory_total",
		Unit: "bytes",
	}
	// MemoryUsed represents the used memory for a group or an entity.
	MemoryUsed = MetricType{
		Name: "memory_used",
		Unit: "bytes",
	}
	// MemoryFree represents the free memory for a group.
	MemoryFree = MetricType{
		Name: "memory_free",
		Unit: "bytes",
	}

	// DiskTotal represents the total disk for a group
	DiskTotal = MetricType{
		Name: "disk_total",
		Unit: "bytes",
	}
	// DiskUsed represents the used disk for a group or an entity.
	DiskUsed = MetricType{
		Name: "disk_used",
		Unit: "bytes",
	}
	// DiskFree represents the free disk for a group.
	DiskFree = MetricType{
		Name: "disk_free",
		Unit: "bytes",
	}

	// NetworkTotal represents the total network for a group
	NetworkTotal = MetricType{
		Name: "network_total",
		Unit: "bits",
	}
	// NetworkUsed represents the used network for a group or an entity.
	NetworkUsed = MetricType{
		Name: "network_used",
		Unit: "bits",
	}
	// NetworkFree represents the free network for a group.
	NetworkFree = MetricType{
		Name: "network_free",
		Unit: "bits",
	}

	// GPUTotal represents the total gpu for a group, each gpu adds 100 %, so a 24 core gpu will have 2400 % gpu.
	GPUTotal = MetricType{
		Name: "gpu_total",
		Unit: "%",
	}
	// GPUUsed represents the used gpu usage for a group or an entity.
	GPUUsed = MetricType{
		Name: "gpu_used",
		Unit: "%",
	}
	// GPUFree represents the free gpu usage for a group.
	GPUFree = MetricType{
		Name: "gpu_free",
		Unit: "%",
	}

	// FileDescriptorsTotal represents the total number of file descriptors available for a group.
	FileDescriptorsTotal = MetricType{
		Name: "file_descriptors_total",
		Unit: "#",
	}
	// FileDescriptorsUsed represents the number of used file descriptors available for a group or entity.
	FileDescriptorsUsed = MetricType{
		Name: "file_descriptors_used",
		Unit: "#",
	}
	// FileDescriptorsFree represents the number of free file descriptors available for a group.
	FileDescriptorsFree = MetricType{
		Name: "file_descriptors_free",
		Unit: "#",
	}

	// PortsTotal represents the total number of ports available for a group.
	PortsTotal = MetricType{
		Name: "ports_total",
		Unit: "#",
	}
	// PortsUsed represents the used number of ports for a group.
	PortsUsed = MetricType{
		Name: "ports_used",
		Unit: "#",
	}
	// PortsFree represents the free number of ports for a group.
	PortsFree = MetricType{
		Name: "ports_free",
		Unit: "#",
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
