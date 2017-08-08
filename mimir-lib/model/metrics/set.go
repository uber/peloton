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

import "sync"

// MetricSet represents some metric on a group which represents a host or zone or entity which represents a process,
// container or task that is running on a group. This can be, but is not limited to, cpu load, memory size, disk size,
// network bandwidth, estimated disk usage per day, etc.
type MetricSet struct {
	set  map[MetricType]float64
	lock sync.RWMutex
}

// NewMetricSet will create a new metric set.
func NewMetricSet() *MetricSet {
	return &MetricSet{
		set: map[MetricType]float64{},
	}
}

// Size will return the number of different metrics in the metric set.
func (set *MetricSet) Size() int {
	set.lock.RLock()
	defer set.lock.RUnlock()

	return len(set.set)
}

// Add will add the value of a metric type to the value which is already there.
func (set *MetricSet) Add(metricType MetricType, value float64) {
	set.lock.Lock()
	defer set.lock.Unlock()

	set.set[metricType] += value
}

// Set will set the value of a metric type.
func (set *MetricSet) Set(metricType MetricType, value float64) {
	set.lock.Lock()
	defer set.lock.Unlock()

	set.set[metricType] = value
}

// Get returns the value of a metric type.
func (set *MetricSet) Get(metricType MetricType) float64 {
	set.lock.RLock()
	defer set.lock.RUnlock()

	return set.set[metricType]
}

// Clear will clear the value of a metric type.
func (set *MetricSet) Clear(metricType MetricType) {
	set.lock.Lock()
	defer set.lock.Unlock()

	delete(set.set, metricType)
}

func copyContent(src *MetricSet) map[MetricType]float64 {
	src.lock.RLock()
	defer src.lock.RUnlock()

	dst := make(map[MetricType]float64, len(src.set))
	for metricType, value := range src.set {
		dst[metricType] = value
	}
	return dst
}

// AddAll combines the values from another metric set into this set, common metric types are combined.
func (set *MetricSet) AddAll(other *MetricSet) {
	otherCopy := copyContent(other)

	set.lock.Lock()
	defer set.lock.Unlock()

	for metricType, value := range otherCopy {
		set.set[metricType] += value
	}
}

// SetAll sets the values from another metric set in this set.
func (set *MetricSet) SetAll(other *MetricSet) {
	otherCopy := copyContent(other)

	set.lock.Lock()
	defer set.lock.Unlock()

	for metricType, value := range otherCopy {
		set.set[metricType] = value
	}
}
