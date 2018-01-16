// @generated AUTO GENERATED - DO NOT EDIT! 9f8b9e47d86b5e1a3668856830c149e768e78415
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
	"sync"
)

// Option represents a flag that can be passed to some methods of the metric set.
type Option interface {
	// Flag returns the value of the flag for the given option.
	Flag() int
}

const (
	ephemeral    = 1
	nonEphemeral = ephemeral << 1
	all          = ephemeral | nonEphemeral
)

type option int

func (_option option) Flag() int {
	return int(_option)
}

// Ephemeral returns an option that selects only ephemeral metrics.
func Ephemeral() Option {
	return option(ephemeral)
}

// NonEphemeral returns an option that selects only non-ephemeral metrics.
func NonEphemeral() Option {
	return option(nonEphemeral)
}

// All returns an option that selects both ephemeral and non-ephemeral metrics.
func All() Option {
	return option(all)
}

// readCombine combines all the options by OR-ing their flag values together. If no option flags are provided it
// defaults to the all flag.
func readCombine(options ...Option) int {
	var flags int
	for i := range options {
		flags = flags | options[i].Flag()
	}
	if flags <= 0 {
		flags = all
	}
	return flags
}

// writeCombine combines all the options by taking the highest value flag, i.e. the top operation on the lattice that
// the three flags form. If no option flags are provided or if the flag is higher or equal to the non-ephemeral flag
// then it defaults to the non-ephemeral flag.
func writeCombine(options ...Option) int {
	var flags int
	for i := range options {
		if flag := options[i].Flag(); flag > flags {
			flags = flag
		}
	}
	if flags <= 0 || nonEphemeral <= flags {
		flags = nonEphemeral
	}
	return flags
}

type valuePair struct {
	flag  int
	value float64
}

// MetricSet represents some metric on a group which represents a host or zone or entity which represents a process,
// container or task that is running on a group. This can be, but is not limited to, cpu load, memory size, disk size,
// network bandwidth, estimated disk usage per day, etc.
type MetricSet struct {
	set  map[MetricType]valuePair
	lock sync.RWMutex
}

// NewMetricSet will create a new metric set.
func NewMetricSet() *MetricSet {
	return &MetricSet{
		set: map[MetricType]valuePair{},
	}
}

// Size will return the number of different metrics in the metric set.
func (set *MetricSet) Size() int {
	set.lock.RLock()
	defer set.lock.RUnlock()

	return len(set.set)
}

// Add will add the value of a metric type to the value which is already there. Using the options the metric can be
// added as an ephemeral or non-ephemeral.
func (set *MetricSet) Add(metricType MetricType, value float64, options ...Option) {
	set.lock.Lock()
	defer set.lock.Unlock()

	old := set.set[metricType].value
	set.set[metricType] = valuePair{
		value: old + value,
		flag:  writeCombine(options...),
	}
}

// Set will set the value of a metric type. Using the options the metric can be set to be ephemeral or non-ephemeral.
func (set *MetricSet) Set(metricType MetricType, value float64, options ...Option) {
	set.lock.Lock()
	defer set.lock.Unlock()

	set.set[metricType] = valuePair{
		value: value,
		flag:  writeCombine(options...),
	}
}

// Get returns the value of a metric type.
func (set *MetricSet) Get(metricType MetricType) float64 {
	set.lock.RLock()
	defer set.lock.RUnlock()

	return set.set[metricType].value
}

// Types returns a new slice of all metric types in the set. Using the options ephemeral and/or non-ephemeral metrics
// can be returned.
func (set *MetricSet) Types(options ...Option) []MetricType {
	set.lock.RLock()
	defer set.lock.RUnlock()

	flags := readCombine(options...)
	var result []MetricType
	for metricType, pair := range set.set {
		if (flags & pair.flag) > 0 {
			result = append(result, metricType)
		}
	}
	return result
}

// Clear will clear the value of a metric type. Using the options an ephemeral and/or non-ephemeral metric can
// be cleared.
func (set *MetricSet) Clear(metricType MetricType, options ...Option) { // ephemeral OR non-ephemeral OR all
	set.lock.Lock()
	defer set.lock.Unlock()

	flags := readCombine(options...)
	flag := set.set[metricType].flag
	if (flags & flag) > 0 {
		delete(set.set, metricType)
	}
}

// ClearAll will clear all metric types and their value. Using the options ephemeral and/or non-ephemeral metrics can
// be cleared.
func (set *MetricSet) ClearAll(options ...Option) {
	flags := readCombine(options...)
	var toRemove []MetricType
	for metricType, pair := range set.set {
		if (flags & pair.flag) > 0 {
			toRemove = append(toRemove, metricType)
		}
	}
	for _, metricType := range toRemove {
		delete(set.set, metricType)
	}
}

func copyContent(src *MetricSet, lock bool) map[MetricType]valuePair {
	if lock {
		src.lock.RLock()
		defer src.lock.RUnlock()
	}

	dst := make(map[MetricType]valuePair, len(src.set))
	for metricType, pair := range src.set {
		dst[metricType] = pair
	}
	return dst
}

// AddAll combines the values from another metric set into this set, common metric types are combined. Using the options
// the metrics can be added as ephemeral or non-ephemeral.
func (set *MetricSet) AddAll(other *MetricSet, options ...Option) { // ephemeral XOR non-ephemeral
	otherCopy := copyContent(other, true)

	set.lock.Lock()
	defer set.lock.Unlock()

	flag := writeCombine(options...)
	for metricType, pair := range otherCopy {
		old := set.set[metricType].value
		set.set[metricType] = valuePair{
			value: old + pair.value,
			flag:  flag,
		}
	}
}

// SetAll sets the values from another metric set in this set. Using the options the metrics can be set as ephemeral
// or non-ephemeral.
func (set *MetricSet) SetAll(other *MetricSet, options ...Option) {
	otherCopy := copyContent(other, true)

	set.lock.Lock()
	defer set.lock.Unlock()

	flag := writeCombine(options...)
	for metricType, pair := range otherCopy {
		set.set[metricType] = valuePair{
			value: pair.value,
			flag:  flag,
		}
	}
}

// Update calculates the values of derived metric types from the non-derived metric types.
func (set *MetricSet) Update() {
	set.lock.Lock()
	defer set.lock.Unlock()

	unmarked := make([]MetricType, 0, len(set.set))
	for metricType := range set.set {
		unmarked = append(unmarked, metricType)
	}
	// We can ignore the error since the metric type have already checked if it is part of a dependency cycle
	order, _ := TopSort(unmarked...)

	// Make a copy of the metrics set
	result := &MetricSet{
		set: copyContent(set, false),
	}
	for _, metricType := range order {
		if metricType.Derivation() != nil {
			metricType.Derivation().Calculate(metricType, result)
		}
	}
	set.set = result.set
}
