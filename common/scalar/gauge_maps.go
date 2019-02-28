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
	g[cpu].Update(resources.GetCPU())
	g[mem].Update(resources.GetMem())
	g[disk].Update(resources.GetDisk())
	g[gpu].Update(resources.GetGPU())
}
