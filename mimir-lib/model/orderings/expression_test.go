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

package orderings

import (
	"code.uber.internal/infra/peloton/mimir-lib/model/labels"
	"code.uber.internal/infra/peloton/mimir-lib/model/metrics"
	"code.uber.internal/infra/peloton/mimir-lib/model/placement"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func setupTwoGroupsAndEntity() (*placement.Group, *placement.Group, *placement.Entity) {
	metrics1 := metrics.NewMetricSet()
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

	return group1, group2, entity
}

func TestCustomByMultiplyNoSubExpressions(t *testing.T) {
	custom := Multiply()

	group1, group2, entity := setupTwoGroupsAndEntity()

	assert.Equal(t, 0, len(custom.Tuple(group1, entity)))
	assert.Equal(t, 0, len(custom.Tuple(group2, entity)))
}

func TestCustomByMetric(t *testing.T) {
	custom := Multiply(
		Metric(GroupSource, metrics.DiskFree),
		Inverse(Metric(EntitySource, metrics.DiskUsed)),
	)
	group1, group2, entity := setupTwoGroupsAndEntity()

	assert.Equal(t, 2.0, custom.Tuple(group1, entity)[0])
	assert.Equal(t, 3.0, custom.Tuple(group2, entity)[0])
}

func TestCustomByMetricForInvalidSource(t *testing.T) {
	custom := Metric(Source("invalid"), metrics.DiskFree)
	group1, group2, entity := setupTwoGroupsAndEntity()

	assert.Equal(t, 0.0, custom.Tuple(group1, entity)[0])
	assert.Equal(t, 0.0, custom.Tuple(group2, entity)[0])
}

func TestCustomByInverse(t *testing.T) {
	custom := Inverse(
		Metric(GroupSource, metrics.DiskFree))
	group1, group2, entity := setupTwoGroupsAndEntity()

	assert.Equal(t, 1.0, custom.Tuple(group1, entity)[0]*metrics.TiB)
	assert.Equal(t, 1.0/1.5, custom.Tuple(group2, entity)[0]*metrics.TiB)
}

func TestCustomByInverseDivisionByZero(t *testing.T) {
	custom := Inverse(
		// The groups have no memory metrics so we divide by zero
		Metric(GroupSource, metrics.MemoryFree))
	group1, group2, entity := setupTwoGroupsAndEntity()

	assert.Equal(t, math.Inf(1), custom.Tuple(group1, entity)[0])
	assert.Equal(t, math.Inf(1), custom.Tuple(group2, entity)[0])
}

func TestCustomByConstant(t *testing.T) {
	custom := Inverse(
		Constant(2.0))
	group1, group2, entity := setupTwoGroupsAndEntity()

	assert.Equal(t, 0.5, custom.Tuple(group1, entity)[0])
	assert.Equal(t, 0.5, custom.Tuple(group2, entity)[0])
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
	group1, group2, entity := setupTwoGroupsAndEntity()

	assert.Equal(t, 0.0, custom.Tuple(group1, entity)[0])
	assert.Equal(t, 1.0, custom.Tuple(group2, entity)[0])
}

func TestOrderByMetricFreeDisk(t *testing.T) {
	custom := Metric(GroupSource, metrics.DiskFree)
	ordering := NewCustomOrdering(custom)
	group1, group2, entity := setupTwoGroupsAndEntity()

	assert.True(t, ordering.Less(group1, group2, entity))
}

func TestOrderByMetricReverseFreeDisk(t *testing.T) {
	custom := Negate(Metric(GroupSource, metrics.DiskFree))
	ordering := NewCustomOrdering(custom)
	group1, group2, entity := setupTwoGroupsAndEntity()

	assert.True(t, ordering.Less(group2, group1, entity))
}

func TestOrderByRelation(t *testing.T) {
	custom := Relation(labels.NewLabelTemplate("schemaless", "instance", "mezzanine"))
	ordering := NewCustomOrdering(custom)
	group1, group2, entity := setupTwoGroupsAndEntity()

	assert.True(t, ordering.Less(group2, group1, entity))
}

func TestOrderByLabel(t *testing.T) {
	custom := Label(labels.NewLabelTemplate("rack", "sjc1-a0084"))
	ordering := NewCustomOrdering(custom)
	group1, group2, entity := setupTwoGroupsAndEntity()

	assert.True(t, ordering.Less(group1, group2, entity))
}

func TestOrderByFreeDiskDividedByEntityDiskUsage(t *testing.T) {
	custom := Negate(
		Multiply(
			Metric(GroupSource, metrics.DiskFree),
			Inverse(
				Metric(EntitySource, metrics.DiskUsed))))
	ordering := NewCustomOrdering(custom)
	group1, group2, entity := setupTwoGroupsAndEntity()

	assert.True(t, ordering.Less(group2, group1, entity))
}

func TestOrderByRelationThenReverseFreeDisk(t *testing.T) {
	custom := Concatenate(
		Relation(labels.NewLabelTemplate("schemaless", "instance", "screamstore")),
		Negate(Metric(GroupSource, metrics.DiskFree)),
	)
	ordering := NewCustomOrdering(custom)
	group1, group2, entity := setupTwoGroupsAndEntity()

	assert.True(t, ordering.Less(group2, group1, entity))
}
