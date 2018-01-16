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

package orderings

import (
	"math"
	"testing"

	"code.uber.internal/infra/peloton/mimir-lib/model/labels"
	"code.uber.internal/infra/peloton/mimir-lib/model/metrics"
	"code.uber.internal/infra/peloton/mimir-lib/model/placement"
	"github.com/stretchr/testify/assert"
)

func setupTwoGroupsAndEntity() (*placement.Group, *placement.Group, []*placement.Group, *placement.Entity) {
	metrics1 := metrics.NewMetricSet()
	metrics1.Set(metrics.MemoryTotal, 128*metrics.GiB)
	metrics1.Set(metrics.MemoryUsed, 32*metrics.GiB)
	metrics1.Set(metrics.MemoryFree, 96*metrics.GiB)
	metrics1.Set(metrics.DiskTotal, 2*metrics.TiB)
	metrics1.Set(metrics.DiskUsed, 1*metrics.TiB)
	metrics1.Set(metrics.DiskFree, 1*metrics.TiB)
	labels1 := labels.NewLabelBag()
	labels1.Add(labels.NewLabel("datacenter", "sjc1"))
	labels1.Add(labels.NewLabel("rack", "sjc1-a0042"))
	relations1 := labels.NewLabelBag()
	relations1.Add(labels.NewLabel("schemaless", "instance", "mezzanine"))
	group1 := &placement.Group{
		Name:      "group1",
		Metrics:   metrics1,
		Labels:    labels1,
		Relations: relations1,
	}

	metrics2 := metrics.NewMetricSet()
	metrics2.Set(metrics.MemoryTotal, 128*metrics.GiB)
	metrics2.Set(metrics.MemoryUsed, 64*metrics.GiB)
	metrics2.Set(metrics.MemoryFree, 64*metrics.GiB)
	metrics2.Set(metrics.DiskTotal, 2*metrics.TiB)
	metrics2.Set(metrics.DiskUsed, 0.5*metrics.TiB)
	metrics2.Set(metrics.DiskFree, 1.5*metrics.TiB)
	labels2 := labels.NewLabelBag()
	labels2.Add(labels.NewLabel("datacenter", "sjc1"))
	labels2.Add(labels.NewLabel("rack", "sjc1-a0084"))
	relations2 := labels.NewLabelBag()
	relations2.Add(labels.NewLabel("schemaless", "instance", "trifle"))
	group2 := &placement.Group{
		Name:      "group2",
		Metrics:   metrics2,
		Labels:    labels2,
		Relations: relations2,
	}

	usage := metrics.NewMetricSet()
	usage.Set(metrics.DiskUsed, 0.5*metrics.TiB)
	entity := &placement.Entity{
		Name:    "entity",
		Metrics: usage,
	}

	return group1, group2, []*placement.Group{group1, group2}, entity
}

func TestCustomByMultiplyNoSubExpressions(t *testing.T) {
	custom := Multiply()

	group1, group2, groups, entity := setupTwoGroupsAndEntity()

	assert.Equal(t, 0, len(custom.Tuple(group1, groups, entity)))
	assert.Equal(t, 0, len(custom.Tuple(group2, groups, entity)))
}

func TestCustomByMetric(t *testing.T) {
	custom := Multiply(
		Metric(GroupSource, metrics.DiskFree),
		Inverse(Metric(EntitySource, metrics.DiskUsed)),
	)
	group1, group2, groups, entity := setupTwoGroupsAndEntity()

	assert.Equal(t, 2.0, custom.Tuple(group1, groups, entity)[0])
	assert.Equal(t, 3.0, custom.Tuple(group2, groups, entity)[0])
}

func TestCustomByMetricForInvalidSource(t *testing.T) {
	custom := Metric(Source("invalid"), metrics.DiskFree)
	group1, group2, groups, entity := setupTwoGroupsAndEntity()

	assert.Equal(t, 0.0, custom.Tuple(group1, groups, entity)[0])
	assert.Equal(t, 0.0, custom.Tuple(group2, groups, entity)[0])
}

func TestCustomByInverse(t *testing.T) {
	custom := Inverse(
		Metric(GroupSource, metrics.DiskFree))
	group1, group2, groups, entity := setupTwoGroupsAndEntity()

	assert.Equal(t, 1.0, custom.Tuple(group1, groups, entity)[0]*metrics.TiB)
	assert.Equal(t, 1.0/1.5, custom.Tuple(group2, groups, entity)[0]*metrics.TiB)
}

func TestCustomByInverseDivisionByZero(t *testing.T) {
	custom := Inverse(
		// The groups have no network metrics so we divide by zero
		Metric(GroupSource, metrics.NetworkFree))
	group1, group2, groups, entity := setupTwoGroupsAndEntity()

	assert.Equal(t, math.Inf(1), custom.Tuple(group1, groups, entity)[0])
	assert.Equal(t, math.Inf(1), custom.Tuple(group2, groups, entity)[0])
}

func TestCustomByConstant(t *testing.T) {
	custom := Inverse(
		Constant(2.0))
	group1, group2, groups, entity := setupTwoGroupsAndEntity()

	assert.Equal(t, 0.5, custom.Tuple(group1, groups, entity)[0])
	assert.Equal(t, 0.5, custom.Tuple(group2, groups, entity)[0])
}

func TestCustomByMapping(t *testing.T) {
	mapping, _ := NewMapping(
		NewBucket(
			NewEndpoint(math.Inf(-1), false),
			NewEndpoint(1.1*metrics.TiB, true),
			0,
		),
		NewBucket(
			NewEndpoint(1.1*metrics.TiB, false),
			NewEndpoint(math.Inf(1), false),
			1,
		),
	)
	custom := Map(
		mapping,
		Metric(GroupSource, metrics.DiskFree))
	group1, group2, groups, entity := setupTwoGroupsAndEntity()

	assert.Equal(t, 0.0, custom.Tuple(group1, groups, entity)[0])
	assert.Equal(t, 1.0, custom.Tuple(group2, groups, entity)[0])
}

func TestOrderByMetricFreeDisk(t *testing.T) {
	custom := Metric(GroupSource, metrics.DiskFree)
	ordering := NewCustomOrdering(custom)
	group1, group2, groups, entity := setupTwoGroupsAndEntity()

	assert.True(t, ordering.Less(group1, group2, groups, entity))
}

func TestOrderByMetricReverseFreeDisk(t *testing.T) {
	custom := Negate(Metric(GroupSource, metrics.DiskFree))
	ordering := NewCustomOrdering(custom)
	group1, group2, groups, entity := setupTwoGroupsAndEntity()

	assert.True(t, ordering.Less(group2, group1, groups, entity))
}

func TestOrderByRelation(t *testing.T) {
	custom := Relation(
		labels.NewLabelTemplate("rack", "*"),
		labels.NewLabelTemplate("schemaless", "instance", "mezzanine"))
	ordering := NewCustomOrdering(custom)
	group1, group2, groups, entity := setupTwoGroupsAndEntity()

	assert.True(t, ordering.Less(group2, group1, groups, entity))
}

func TestOrderByLabel(t *testing.T) {
	custom := Label(
		labels.NewLabelTemplate("rack", "*"),
		labels.NewLabelTemplate("rack", "sjc1-a0084"))
	ordering := NewCustomOrdering(custom)
	group1, group2, groups, entity := setupTwoGroupsAndEntity()

	assert.True(t, ordering.Less(group1, group2, groups, entity))
}

func TestOrderByFreeDiskDividedByEntityDiskUsage(t *testing.T) {
	custom := Negate(
		Multiply(
			Metric(GroupSource, metrics.DiskFree),
			Inverse(
				Metric(EntitySource, metrics.DiskUsed))))
	ordering := NewCustomOrdering(custom)
	group1, group2, groups, entity := setupTwoGroupsAndEntity()

	assert.True(t, ordering.Less(group2, group1, groups, entity))
}

func TestOrderByRelationThenReverseFreeDisk(t *testing.T) {
	custom := Concatenate(
		Relation(nil, labels.NewLabelTemplate("schemaless", "instance", "screamstore")),
		Negate(Metric(GroupSource, metrics.DiskFree)),
	)
	ordering := NewCustomOrdering(custom)
	group1, group2, groups, entity := setupTwoGroupsAndEntity()

	assert.True(t, ordering.Less(group2, group1, groups, entity))
}

func TestOrderBySummationOfFreeMemoryAndDisk(t *testing.T) {
	custom := Negate(
		Summation(
			Metric(GroupSource, metrics.MemoryFree),
			Metric(GroupSource, metrics.DiskFree),
		),
	)
	ordering := NewCustomOrdering(custom)
	group1, group2, groups, entity := setupTwoGroupsAndEntity()

	assert.True(t, ordering.Less(group2, group1, groups, entity))
	expected1 := group1.Metrics.Get(metrics.MemoryFree) + group1.Metrics.Get(metrics.DiskFree)
	assert.Equal(t, -expected1, custom.Tuple(group1, groups, entity)[0])
	expected2 := group2.Metrics.Get(metrics.MemoryFree) + group2.Metrics.Get(metrics.DiskFree)
	assert.Equal(t, -expected2, custom.Tuple(group2, groups, entity)[0])
}
