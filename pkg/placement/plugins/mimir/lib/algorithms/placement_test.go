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

package algorithms

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/examples"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/generation"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/generation/orderings"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/metrics"
	source "github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/orderings"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
)

func setup(concurrency int) (placer Placer, relocator Relocator, groups []*placement.Group, store1dbs, store2dbs []*placement.Entity) {
	random := generation.NewRandom(42)
	entityBuilder, entityTemplates := examples.CreateSchemalessEntityBuilder()
	entityBuilder.Ordering(orderings.NewOrderingBuilder(orderings.Negate(orderings.Metric(source.GroupSource, metrics.DiskFree))))

	entityTemplates.
		Bind(examples.Instance.Name(), "store1").
		Bind(examples.Datacenter.Name(), "dc1")
	store1dbs = examples.CreateSchemalessEntities(
		random, entityBuilder, entityTemplates, 4, 4)

	entityTemplates.
		Bind(examples.Instance.Name(), "store2").
		Bind(examples.Datacenter.Name(), "dc1")
	store2dbs = examples.CreateSchemalessEntities(random, entityBuilder, entityTemplates, 4, 4)

	groupBuilder, groupTemplates := examples.CreateHostGroupsBuilder()
	groupTemplates.Bind(examples.Datacenter.Name(), "dc1")
	groups = examples.CreateHostGroups(
		random, groupBuilder, groupTemplates, 4, 16)
	placer = NewPlacer(concurrency, 1)
	relocator = NewRelocator(concurrency, 1)

	return
}

func TestPlacer_Place_takes_all_groups_into_account_when_placing_a_group_concurrently(t *testing.T) {
	placer, _, groups, store1dbs, _ := setup(2)
	groups[0].Entities.Add(store1dbs[0])
	groups[0].Update()

	groups[1].Entities.Add(store1dbs[1])
	groups[1].Update()

	assignments := []*placement.Assignment{
		placement.NewAssignment(store1dbs[2]),
	}
	scopeSet := placement.NewScopeSet(groups[0:3])
	placer.Place(assignments, groups[0:3], scopeSet)

	require.False(t, assignments[0].Failed)
	assert.Equal(t, groups[2], assignments[0].AssignedGroup)
}

func TestPlace_Place_successfully_assigns_all_entities(t *testing.T) {
	for concurrency := 1; concurrency <= 2; concurrency++ {
		placer, _, groups, store1dbs, store2dbs := setup(concurrency)
		entities := append(store1dbs, store2dbs...)

		var assignments []*placement.Assignment
		for _, entity := range entities {
			assignments = append(assignments, placement.NewAssignment(entity))
		}
		scopeSet := placement.NewScopeSet(groups)
		placer.Place(assignments, groups, scopeSet)
		for _, assignment := range assignments {
			assert.False(t, assignment.Failed)
		}
	}
}

func setupTwoGroupsOneAssignment(concurrency int) (placer Placer, relocator Relocator, assignment *placement.Assignment,
	free *placement.Group, unassigned *placement.Entity) {
	placer, relocator, groups, store1dbs, store2dbs := setup(concurrency)
	assignment = placement.NewAssignment(store1dbs[0])
	selectedGroups := []*placement.Group{groups[0]}
	scopeSet := placement.NewScopeSet(selectedGroups)
	placer.Place([]*placement.Assignment{assignment}, selectedGroups, scopeSet)
	free = groups[1]
	unassigned = store2dbs[0]

	return
}

func TestPlacer_Place_with_unassigned_entity_assigns_a_group_to_the_entity(t *testing.T) {
	for concurrency := 1; concurrency <= 2; concurrency++ {
		placer, _, assignment1, free, unassigned := setupTwoGroupsOneAssignment(concurrency)
		assignment2 := placement.NewAssignment(unassigned)

		// Assign the unassigned entity to the same group as that of assignment1
		groups1 := []*placement.Group{assignment1.AssignedGroup}
		scopeSet := placement.NewScopeSet(groups1)
		placer.Place([]*placement.Assignment{assignment2}, groups1, scopeSet)
		assert.Equal(t, assignment1.AssignedGroup, assignment2.AssignedGroup)

		// Let the placer reassign the entity of assignment2 to the free group if it is better
		groups2 := []*placement.Group{assignment1.AssignedGroup, free}
		scopeSet = placement.NewScopeSet(groups2)
		placer.Place([]*placement.Assignment{assignment2}, groups2, scopeSet)
		assert.Equal(t, free, assignment2.AssignedGroup)
	}
}

func TestPlacer_Place_updates_metrics_and_relations_of_assigned_groups(t *testing.T) {
	for concurrency := 1; concurrency <= 2; concurrency++ {
		placer, _, assignment1, _, unassigned := setupTwoGroupsOneAssignment(concurrency)
		assignment2 := placement.NewAssignment(unassigned)

		memoryUsed := unassigned.Metrics.Get(metrics.MemoryUsed)
		diskUsed := unassigned.Metrics.Get(metrics.DiskUsed)

		memoryUsedBefore := assignment1.AssignedGroup.Metrics.Get(metrics.MemoryUsed)
		diskUsedBefore := assignment1.AssignedGroup.Metrics.Get(metrics.DiskUsed)

		// Assign the unassigned entity to the same group as that of assignment1
		groups := []*placement.Group{assignment1.AssignedGroup}
		scopeSet := placement.NewScopeSet(groups)
		placer.Place([]*placement.Assignment{assignment2}, groups, scopeSet)

		memoryUsedAfter := assignment1.AssignedGroup.Metrics.Get(metrics.MemoryUsed)
		diskUsedAfter := assignment1.AssignedGroup.Metrics.Get(metrics.DiskUsed)

		assert.Equal(t, memoryUsed+memoryUsedBefore, memoryUsedAfter)
		assert.Equal(t, diskUsed+diskUsedBefore, diskUsedAfter)
	}
}
