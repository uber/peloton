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

package orderings

import (
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/metrics"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
)

// Metric will create an ordering which will order groups based on their value of the given metric type.
func Metric(source Source, metricType metrics.Type) placement.Ordering {
	return &MetricCustom{
		Source:     source,
		MetricType: metricType,
	}
}

// MetricCustom can create a tuple of one float which is the value of the metric found in the given source.
type MetricCustom struct {
	Source     Source
	MetricType metrics.Type
}

// Tuple returns a tuple of floats created from the group, scope groups and the entity.
func (custom *MetricCustom) Tuple(group *placement.Group, scopeSet *placement.ScopeSet, entity *placement.Entity) []float64 {
	switch custom.Source {
	case EntitySource:
		return []float64{entity.Metrics.Get(custom.MetricType)}
	case GroupSource:
		return []float64{group.Metrics.Get(custom.MetricType)}
	}
	return []float64{0.0}
}
