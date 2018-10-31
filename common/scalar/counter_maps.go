package scalar

import "github.com/uber-go/tally"

// CounterMaps wraps around a group of metrics which can be used for reporting
// scalar resources as a group of counter.
type CounterMaps map[resourceKey]tally.Counter

// NewCounterMaps returns the CounterMaps initialized at given tally scope.
func NewCounterMaps(scope tally.Scope) CounterMaps {
	return CounterMaps{
		cpu:  scope.Counter("cpu"),
		mem:  scope.Counter("mem"),
		disk: scope.Counter("disk"),
		gpu:  scope.Counter("gpu"),
	}
}

// Inc increments all counters for given resources.
func (g CounterMaps) Inc(resources Resources) {
	g[cpu].Inc(int64(resources.GetCPU()))
	g[mem].Inc(int64(resources.GetMem()))
	g[disk].Inc(int64(resources.GetDisk()))
	g[gpu].Inc(int64(resources.GetGPU()))
}
