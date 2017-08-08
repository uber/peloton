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

// Deriver derives a metric type, i.e. a metric type that is derived from other metric types. An example is
// free CPUs which is computed from the total CPUs minus the used CPUs.
type Deriver interface {
	// Derive calculates any derived metrics and updates them in the metric set.
	Derive(metricSet *MetricSet)
}

// FreeMetricTuple represents a free, used and total metric type to use to compute the value of the free metric type from.
type FreeMetricTuple [3]MetricType

// NewFreeMetricTuple creates a new free metric tuple from a free, used and total metric type.
func NewFreeMetricTuple(freeType, usedType, totalType MetricType) FreeMetricTuple {
	return FreeMetricTuple{
		freeType, usedType, totalType,
	}
}

// FreeType gives the metric type for the free metric.
func (tuple FreeMetricTuple) FreeType() MetricType {
	return tuple[0]
}

// UsedType gives the metric type for the used metric.
func (tuple FreeMetricTuple) UsedType() MetricType {
	return tuple[1]
}

// TotalType gives the metric type for the total metric.
func (tuple FreeMetricTuple) TotalType() MetricType {
	return tuple[2]
}

// NewDeriver creates a new deriver that will derive free cpu, memory, network and disk.
func NewDeriver(mapping []FreeMetricTuple) Deriver {
	return &deriver{
		mapping: mapping,
	}
}

func computeFree(metricSet *MetricSet, usedType, totalType MetricType) float64 {
	total := metricSet.Get(totalType)
	used := metricSet.Get(usedType)
	return total - used
}

type deriver struct {
	mapping []FreeMetricTuple
}

func (_deriver *deriver) Derive(metricSet *MetricSet) {
	newMetrics := NewMetricSet()
	for _, tuple := range _deriver.mapping {
		newMetrics.Set(tuple.FreeType(), computeFree(metricSet, tuple.UsedType(), tuple.TotalType()))
	}
	metricSet.SetAll(newMetrics)
}
