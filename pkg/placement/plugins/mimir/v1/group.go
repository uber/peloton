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

package mimir_v1

import (
	"github.com/uber/peloton/pkg/hostmgr/scalar"
	common "github.com/uber/peloton/pkg/placement/plugins/mimir/common"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/labels"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/metrics"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
)

// CreateGroup creates a mimir placement group from a hostname, available
// resources, and labels.
func CreateGroup(
	hostname string,
	res scalar.Resources,
	ports uint64,
	labels map[string]string,
) *placement.Group {
	group := placement.NewGroup(hostname)
	group.Metrics = makeMetrics(res, ports)
	group.Labels = makeLabels(hostname, labels)
	return group
}

func makeMetrics(res scalar.Resources, ports uint64) *metrics.Set {
	result := metrics.NewSet()
	result.Set(common.CPUAvailable, res.CPU*100.0)
	result.Set(common.GPUAvailable, res.GPU*100.0)
	result.Set(common.MemoryAvailable, res.Mem*metrics.MiB)
	result.Set(common.DiskAvailable, res.Disk*metrics.MiB)
	result.Set(common.PortsAvailable, float64(ports))

	result.Set(common.CPUFree, 0.0)
	result.Set(common.GPUFree, 0.0)
	result.Set(common.MemoryFree, 0.0)
	result.Set(common.DiskFree, 0.0)
	result.Set(common.PortsFree, 0)
	result.Update()
	return result
}

func makeLabels(hostname string, genericLabels map[string]string) *labels.Bag {
	result := labels.NewBag()
	for k, v := range genericLabels {
		result.Add(labels.NewLabel(k, v))
	}
	result.Add(labels.NewLabel(common.HostNameLabel, hostname))
	return result
}
