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

// TypeMapper maps a metric type, say a metric type of a requirement, to another metric type. An example is
// used CPUs of an entity which is computed from the free CPUs required by the entity.
type TypeMapper interface {
	// Map will change the types of the metrics in the metric set to other types.
	Map(metricSet *MetricSet)
}

// NewTypeMapper will create a type mapper that maps the types of a metric set to the types given in the mapping.
func NewTypeMapper(mapping map[MetricType]MetricType) TypeMapper {
	return &changer{
		mapping: mapping,
	}
}

type changer struct {
	mapping map[MetricType]MetricType
}

func (_changer *changer) Map(metricSet *MetricSet) {
	for from, to := range _changer.mapping {
		metricSet.Set(to, metricSet.Get(from))
		metricSet.Clear(from)
	}
}
