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

package mimir_v0

import (
	"fmt"
	"strings"

	"github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"

	common "github.com/uber/peloton/pkg/placement/plugins/mimir/common"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/labels"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/metrics"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
)

// OfferToGroup will convert an offer to a group.
func OfferToGroup(hostOffer *hostsvc.HostOffer) *placement.Group {
	group := placement.NewGroup(hostOffer.Hostname)
	group.Metrics = makeMetrics(hostOffer.GetResources())
	group.Labels = makeLabels(hostOffer)
	return group
}

func makeMetrics(resources []*mesos_v1.Resource) *metrics.Set {
	result := metrics.NewSet()
	for _, resource := range resources {
		value := resource.GetScalar().GetValue()
		switch name := resource.GetName(); name {
		case "cpus":
			result.Add(common.CPUAvailable, value*100.0)
			result.Set(common.CPUFree, 0.0)
		case "gpus":
			result.Add(common.GPUAvailable, value*100.0)
			result.Set(common.GPUFree, 0.0)
		case "mem":
			result.Add(common.MemoryAvailable, value*metrics.MiB)
			result.Set(common.MemoryFree, 0.0)
		case "disk":
			result.Add(common.DiskAvailable, value*metrics.MiB)
			result.Set(common.DiskFree, 0.0)
		case "ports":
			ports := uint64(0)
			for _, r := range resource.GetRanges().GetRange() {
				ports += r.GetEnd() - r.GetBegin() + 1
			}
			result.Add(common.PortsAvailable, float64(ports))
			result.Set(common.PortsFree, 0.0)
		}
	}
	// Compute the derived metrics, e.g. the free metrics from the available and reserved metrics.
	result.Update()
	return result
}

// makeLabels will convert Mesos attributes into Mimir labels.
// A scalar attribute with name n and value v will be turned into the label ["n", "v"].
// A text attribute with name n and value t will be turned into the label ["n", "t"].
// A ranges attribute with name n and ranges [r_1a:r_1b], ..., [r_na:r_nb] will be turned into
// the label ["n", "[r_1a-r1b];...[r_na-r_nb]"].
func makeLabels(hostOffer *hostsvc.HostOffer) *labels.Bag {
	attributes := hostOffer.GetAttributes()
	result := labels.NewBag()
	for _, attribute := range attributes {
		var value string
		switch attribute.GetType() {
		case mesos_v1.Value_SCALAR:
			value = fmt.Sprintf("%v", attribute.GetScalar().GetValue())
		case mesos_v1.Value_TEXT:
			value = attribute.GetText().GetValue()
		case mesos_v1.Value_RANGES:
			ranges := attribute.GetRanges().GetRange()
			length := len(ranges)
			for i, valueRange := range ranges {
				value += fmt.Sprintf("[%v-%v]", valueRange.GetBegin(), valueRange.GetEnd())
				if i < length-1 {
					value += ";"
				}
			}
		}
		names := strings.Split(attribute.GetName(), ".")
		names = append(names, value)
		result.Add(labels.NewLabel(names...))
	}
	result.Add(labels.NewLabel(common.HostNameLabel, hostOffer.GetHostname()))
	return result
}
