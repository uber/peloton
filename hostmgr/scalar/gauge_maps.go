package scalar

import "github.com/uber-go/tally"

type resourceKey int

const (
	cpu resourceKey = iota
	mem
	disk
	gpu
)

// GaugeMaps wraps around a group of metrics which can be used for reporting
// scalar resources as a group of gauges.
type GaugeMaps map[resourceKey]tally.Gauge

// NewGaugeMaps returns the GaugeMaps initialized at given tally scope.
func NewGaugeMaps(scope tally.Scope) GaugeMaps {
	return GaugeMaps{
		cpu:  scope.Gauge("cpu"),
		mem:  scope.Gauge("mem"),
		disk: scope.Gauge("disk"),
		gpu:  scope.Gauge("gpu"),
	}
}

// Update updates all gauges from given resources.
func (g GaugeMaps) Update(resources Resources) {
	g[cpu].Update(resources.CPU)
	g[mem].Update(resources.Mem)
	g[disk].Update(resources.Disk)
	g[gpu].Update(resources.GPU)
}
