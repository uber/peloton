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

package metrics

import (
	"sort"
	"sync"
)

// Set represents some metric on a group which represents a host or zone or entity which represents a process,
// container or task that is running on a group. This can be, but is not limited to, cpu load, memory size, disk size,
// network bandwidth, estimated disk usage per day, etc.
type Set struct {
	set  map[Type]float64
	lock sync.RWMutex
}

// NewSet will create a new metric set.
func NewSet() *Set {
	return &Set{
		set: map[Type]float64{},
	}
}

// Size will return the number of different metrics in the metric set.
func (set *Set) Size() int {
	set.lock.RLock()
	defer set.lock.RUnlock()

	return len(set.set)
}

// Add will add the value of a metric type to the value which is already there.
func (set *Set) Add(metricType Type, value float64) {
	set.lock.Lock()
	defer set.lock.Unlock()

	old := set.set[metricType]
	set.set[metricType] = old + value
}

// Set will set the value of a metric type.
func (set *Set) Set(metricType Type, value float64) {
	set.lock.Lock()
	defer set.lock.Unlock()

	set.set[metricType] = value
}

// Get returns the value of a metric type.
func (set *Set) Get(metricType Type) float64 {
	set.lock.RLock()
	defer set.lock.RUnlock()

	return set.set[metricType]
}

type sortedMetricTypes []Type

func (sorted sortedMetricTypes) Len() int {
	return len(sorted)
}

func (sorted sortedMetricTypes) Less(i, j int) bool {
	return sorted[i].Name < sorted[j].Name
}

func (sorted sortedMetricTypes) Swap(i, j int) {
	sorted[i], sorted[j] = sorted[j], sorted[i]
}

// Types returns a new slice of all metric types in the set.The list of types is sorted by the metric type names in
// ascending order.
func (set *Set) Types() []Type {
	set.lock.RLock()
	defer set.lock.RUnlock()

	var result sortedMetricTypes
	for metricType := range set.set {
		result = append(result, metricType)
	}
	sort.Sort(result)
	return result
}

// Clear will clear the value of a metric type.
func (set *Set) Clear(metricType Type) {
	set.lock.Lock()
	defer set.lock.Unlock()

	delete(set.set, metricType)
}

// ClearAll will clear all metric types and their value.
func (set *Set) ClearAll(clearInherited, clearNonInherited bool) {
	set.lock.Lock()
	defer set.lock.Unlock()

	var toRemove []Type
	for metricType := range set.set {
		if metricType.Inherited == clearInherited || !metricType.Inherited == clearNonInherited {
			toRemove = append(toRemove, metricType)
		}
	}
	for _, metricType := range toRemove {
		delete(set.set, metricType)
	}
}

func copyContent(src *Set, lock bool) map[Type]float64 {
	if lock {
		src.lock.RLock()
		defer src.lock.RUnlock()
	}

	dst := make(map[Type]float64, len(src.set))
	for metricType, pair := range src.set {
		dst[metricType] = pair
	}
	return dst
}

// AddAll combines the values from another metric set into this set, common metric types are combined.
func (set *Set) AddAll(other *Set) {
	otherCopy := copyContent(other, true)

	set.lock.Lock()
	defer set.lock.Unlock()

	for metricType, value := range otherCopy {
		old := set.set[metricType]
		set.set[metricType] = old + value
	}
}

// SetAll sets the values from another metric set in this set.
func (set *Set) SetAll(other *Set) {
	otherCopy := copyContent(other, true)

	set.lock.Lock()
	defer set.lock.Unlock()

	for metricType, value := range otherCopy {
		set.set[metricType] = value
	}
}

// Update calculates the values of derived metric types from the non-derived metric types.
func (set *Set) Update() {
	set.lock.Lock()
	defer set.lock.Unlock()

	unmarked := make([]Type, 0, len(set.set))
	for metricType := range set.set {
		unmarked = append(unmarked, metricType)
	}
	// We can ignore the error since the metric type have already checked if it is part of a dependency cycle
	order, _ := TopSort(unmarked...)

	// Make a copy of the metrics set
	result := &Set{
		set: copyContent(set, false),
	}
	for _, metricType := range order {
		if metricType.Derivation() != nil {
			metricType.Derivation().Calculate(metricType, result)
		}
	}
	set.set = result.set
}
