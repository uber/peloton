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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/generation"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/internal"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/labels"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/metrics"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/orderings"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
)

func TestMetricBuilder_Generate_EntitySource(t *testing.T) {
	ordering := Metric(orderings.EntitySource, metrics.DiskUsed).
		Generate(generation.NewRandom(42), time.Duration(0))
	group1, group2, groups, entity := internal.SetupTwoGroupsAndEntity()
	scopeSet := placement.NewScopeSet(groups)
	tuple1 := ordering.Tuple(group1, scopeSet, entity)
	tuple2 := ordering.Tuple(group2, scopeSet, entity)

	assert.Equal(t, 1, len(tuple1))
	assert.Equal(t, 1, len(tuple2))
	assert.Equal(t, entity.Metrics.Get(metrics.DiskUsed), tuple1[0])
	assert.Equal(t, entity.Metrics.Get(metrics.DiskUsed), tuple2[0])
}

func TestMetricBuilder_Generate_GroupSource(t *testing.T) {
	ordering := Metric(orderings.GroupSource, metrics.DiskUsed).
		Generate(generation.NewRandom(42), time.Duration(0))
	group1, group2, groups, entity := internal.SetupTwoGroupsAndEntity()
	scopeSet := placement.NewScopeSet(groups)
	tuple1 := ordering.Tuple(group1, scopeSet, entity)
	tuple2 := ordering.Tuple(group2, scopeSet, entity)

	assert.Equal(t, 1, len(tuple1))
	assert.Equal(t, 1, len(tuple2))
	assert.Equal(t, group1.Metrics.Get(metrics.DiskUsed), tuple1[0])
	assert.Equal(t, group2.Metrics.Get(metrics.DiskUsed), tuple2[0])
}

func TestRelationBuilder_Generate_with_nil_scope(t *testing.T) {
	pattern := labels.NewTemplate("schemaless", "instance", "*")
	ordering := Relation(nil, pattern).
		Generate(generation.NewRandom(42), time.Duration(0))
	group1, group2, groups, entity := internal.SetupTwoGroupsAndEntity()
	scopeSet := placement.NewScopeSet(groups)
	tuple1 := ordering.Tuple(group1, scopeSet, entity)
	tuple2 := ordering.Tuple(group2, scopeSet, entity)

	assert.Equal(t, 1, len(tuple1))
	assert.Equal(t, 1, len(tuple2))
	assert.Equal(t, float64(group1.Relations.Count(pattern.Instantiate())), tuple1[0])
	assert.Equal(t, float64(group2.Relations.Count(pattern.Instantiate())), tuple2[0])
}

func TestRelationBuilder_Generate_with_datacenter_scope(t *testing.T) {
	pattern := labels.NewTemplate("schemaless", "instance", "*")
	ordering := Relation(labels.NewTemplate("datacenter", "*"), pattern).
		Generate(generation.NewRandom(42), time.Duration(0))
	group1, group2, groups, entity := internal.SetupTwoGroupsAndEntity()
	scopeSet := placement.NewScopeSet(groups)
	tuple1 := ordering.Tuple(group1, scopeSet, entity)
	tuple2 := ordering.Tuple(group2, scopeSet, entity)

	assert.Equal(t, 1, len(tuple1))
	assert.Equal(t, 1, len(tuple2))
	expected := group1.Relations.Count(pattern.Instantiate()) + group2.Relations.Count(pattern.Instantiate())
	assert.Equal(t, float64(expected), tuple1[0])
	assert.Equal(t, float64(expected), tuple2[0])
}

func TestLabelBuilder_Generate_with_nil_scope(t *testing.T) {
	pattern := labels.NewTemplate("rack", "*")
	ordering := Label(nil, pattern).
		Generate(generation.NewRandom(42), time.Duration(0))
	group1, group2, groups, entity := internal.SetupTwoGroupsAndEntity()
	scopeSet := placement.NewScopeSet(groups)
	tuple1 := ordering.Tuple(group1, scopeSet, entity)
	tuple2 := ordering.Tuple(group2, scopeSet, entity)

	assert.Equal(t, 1, len(tuple1))
	assert.Equal(t, 1, len(tuple2))
	assert.Equal(t, float64(group1.Labels.Count(pattern.Instantiate())), tuple1[0])
	assert.Equal(t, float64(group2.Labels.Count(pattern.Instantiate())), tuple2[0])
}

func TestLabelBuilder_Generate_with_datacenter_scope(t *testing.T) {
	pattern := labels.NewTemplate("rack", "*")
	ordering := Label(labels.NewTemplate("datacenter", "*"), pattern).
		Generate(generation.NewRandom(42), time.Duration(0))
	group1, group2, groups, entity := internal.SetupTwoGroupsAndEntity()
	scopeSet := placement.NewScopeSet(groups)
	tuple1 := ordering.Tuple(group1, scopeSet, entity)
	tuple2 := ordering.Tuple(group2, scopeSet, entity)

	assert.Equal(t, 1, len(tuple1))
	assert.Equal(t, 1, len(tuple2))
	expected := group1.Labels.Count(pattern.Instantiate()) + group2.Labels.Count(pattern.Instantiate())
	assert.Equal(t, float64(expected), tuple1[0])
	assert.Equal(t, float64(expected), tuple2[0])
}

func TestConstantBuilder_Generate(t *testing.T) {
	ordering := Constant(42.0).
		Generate(generation.NewRandom(42), time.Duration(0))
	group1, group2, groups, entity := internal.SetupTwoGroupsAndEntity()
	scopeSet := placement.NewScopeSet(groups)
	tuple1 := ordering.Tuple(group1, scopeSet, entity)
	tuple2 := ordering.Tuple(group2, scopeSet, entity)

	assert.Equal(t, 1, len(tuple1))
	assert.Equal(t, 1, len(tuple2))
	assert.Equal(t, 42.0, tuple1[0])
	assert.Equal(t, 42.0, tuple2[0])
}

func TestNegateBuilder_Generate(t *testing.T) {
	ordering := Negate(Constant(42.0)).
		Generate(generation.NewRandom(42), time.Duration(0))
	group1, group2, groups, entity := internal.SetupTwoGroupsAndEntity()
	scopeSet := placement.NewScopeSet(groups)
	tuple1 := ordering.Tuple(group1, scopeSet, entity)
	tuple2 := ordering.Tuple(group2, scopeSet, entity)

	assert.Equal(t, 1, len(tuple1))
	assert.Equal(t, 1, len(tuple2))
	assert.Equal(t, -42.0, tuple1[0])
	assert.Equal(t, -42.0, tuple2[0])
}

func TestInverseBuilder_Generate(t *testing.T) {
	ordering := Inverse(Constant(42.0)).
		Generate(generation.NewRandom(42), time.Duration(0))
	group1, group2, groups, entity := internal.SetupTwoGroupsAndEntity()
	scopeSet := placement.NewScopeSet(groups)
	tuple1 := ordering.Tuple(group1, scopeSet, entity)
	tuple2 := ordering.Tuple(group2, scopeSet, entity)

	assert.Equal(t, 1, len(tuple1))
	assert.Equal(t, 1, len(tuple2))
	assert.Equal(t, 1.0/42.0, tuple1[0])
	assert.Equal(t, 1.0/42.0, tuple2[0])
}

func TestSummationBuilder_Generate(t *testing.T) {
	ordering := Sum(Constant(1.0), Constant(1.0)).
		Generate(generation.NewRandom(42), time.Duration(0))
	group1, group2, groups, entity := internal.SetupTwoGroupsAndEntity()
	scopeSet := placement.NewScopeSet(groups)
	tuple1 := ordering.Tuple(group1, scopeSet, entity)
	tuple2 := ordering.Tuple(group2, scopeSet, entity)

	assert.Equal(t, 1, len(tuple1))
	assert.Equal(t, 1, len(tuple2))
	assert.Equal(t, 2.0, tuple1[0])
	assert.Equal(t, 2.0, tuple2[0])
}

func TestMultiplyBuilder_Generate(t *testing.T) {
	ordering := Multiply(Constant(2.0), Constant(3.0)).
		Generate(generation.NewRandom(42), time.Duration(0))
	group1, group2, groups, entity := internal.SetupTwoGroupsAndEntity()
	scopeSet := placement.NewScopeSet(groups)
	tuple1 := ordering.Tuple(group1, scopeSet, entity)
	tuple2 := ordering.Tuple(group2, scopeSet, entity)

	assert.Equal(t, 1, len(tuple1))
	assert.Equal(t, 1, len(tuple2))
	assert.Equal(t, 6.0, tuple1[0])
	assert.Equal(t, 6.0, tuple2[0])
}

func TestMapBuilder_Generate(t *testing.T) {
	mapping, _ := orderings.NewMapping(
		orderings.NewBucket(
			orderings.NewEndpoint(math.Inf(-1), false),
			orderings.NewEndpoint(0.5, true),
			23.0,
		),
		orderings.NewBucket(
			orderings.NewEndpoint(0.5, false),
			orderings.NewEndpoint(math.Inf(1), false),
			42.0,
		),
	)
	ordering := Map(mapping, Relation(nil, labels.NewTemplate("schemaless", "instance", "mezzanine"))).
		Generate(generation.NewRandom(42), time.Duration(0))
	group1, group2, groups, entity := internal.SetupTwoGroupsAndEntity()
	scopeSet := placement.NewScopeSet(groups)
	tuple1 := ordering.Tuple(group1, scopeSet, entity)
	tuple2 := ordering.Tuple(group2, scopeSet, entity)

	assert.Equal(t, 1, len(tuple1))
	assert.Equal(t, 1, len(tuple2))
	assert.Equal(t, 42.0, tuple1[0])
	assert.Equal(t, 23.0, tuple2[0])
}

func TestConcatenateBuilder_Generate(t *testing.T) {
	ordering := Concatenate(Constant(2.0), Constant(3.0)).
		Generate(generation.NewRandom(42), time.Duration(0))
	group1, group2, groups, entity := internal.SetupTwoGroupsAndEntity()
	scopeSet := placement.NewScopeSet(groups)
	tuple1 := ordering.Tuple(group1, scopeSet, entity)
	tuple2 := ordering.Tuple(group2, scopeSet, entity)

	assert.Equal(t, 2, len(tuple1))
	assert.Equal(t, 2, len(tuple2))
	assert.Equal(t, 2.0, tuple1[0])
	assert.Equal(t, 3.0, tuple1[1])
	assert.Equal(t, 2.0, tuple2[0])
	assert.Equal(t, 3.0, tuple2[1])
}
