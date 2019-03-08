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

package requirements

import (
	"fmt"

	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/metrics"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
)

// MetricRequirement represents a hard requirement for placing a Database in a given PlacementGroup which should have
// certain requirements for a specific metric.
//
// An example initialization could be:
//	requirement := NewMetricRequirement(
//		metrics.DiskFree,
//		GreaterThanEqual,
//		256*metrics.GiB,
//	)
// which requires that the group should have 256 GiB or more of the metric disk free.
type MetricRequirement struct {
	MetricType metrics.Type
	Comparison Comparison
	Value      float64
}

// NewMetricRequirement creates a new metric requirement.
func NewMetricRequirement(metricType metrics.Type, comparison Comparison, value float64) *MetricRequirement {
	return &MetricRequirement{
		MetricType: metricType,
		Comparison: comparison,
		Value:      value,
	}
}

// Passed checks if the requirement is fulfilled by the given group within the scope groups.
func (requirement *MetricRequirement) Passed(group *placement.Group, scopeSet *placement.ScopeSet,
	entity *placement.Entity, transcript *placement.Transcript) bool {
	value := group.Metrics.Get(requirement.MetricType)
	fulfilled, err := requirement.Comparison.Compare(value, requirement.Value)
	if err != nil || !fulfilled {
		transcript.IncFailed()
		return false
	}
	transcript.IncPassed()
	return true
}

func (requirement *MetricRequirement) String() string {
	return fmt.Sprintf("requires that %v should be %v %v %v", requirement.MetricType.Name,
		requirement.Comparison, requirement.Value, requirement.MetricType.Unit)
}

// Composite returns false as the requirement is not composite and the name of the requirement type.
func (requirement *MetricRequirement) Composite() (bool, string) {
	return false, "metric"
}
