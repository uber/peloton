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

package algorithms

import (
	"testing"

	"code.uber.internal/infra/peloton/mimir-lib/generation"
	"code.uber.internal/infra/peloton/mimir-lib/model/metrics"
	"code.uber.internal/infra/peloton/mimir-lib/model/placement"
	"github.com/stretchr/testify/assert"
)

func setup() (placer Placer, relocator Relocator, groups []*placement.Group, store1dbs, store2dbs []*placement.Entity) {
	random := generation.NewRandom(42)
	entityBuilder, entityTemplates := generation.CreateSchemalessEntityBuilder()

	entityTemplates.
		Bind(generation.Instance.Name(), "store1").
		Bind(generation.Datacenter.Name(), "dc1")
	store1dbs = generation.CreateSchemalessEntities(
		random, entityBuilder, entityTemplates, 4, 4)

	entityTemplates.
		Bind(generation.Instance.Name(), "store2").
		Bind(generation.Datacenter.Name(), "dc1")
	store2dbs = generation.CreateSchemalessEntities(random, entityBuilder, entityTemplates, 4, 4)

	groupBuilder, groupTemplates := generation.CreateHostGroupsBuilder()
	groupTemplates.Bind(generation.Datacenter.Name(), "dc1")
	groups = generation.CreateHostGroups(
		random, groupBuilder, groupTemplates, 4, 16)
	placer = NewPlacer()
	relocator = NewRelocator()

	return
}

func TestPlace_Place_successfully_assigns_all_entities(t *testing.T) {
	placer, _, groups, store1dbs, store2dbs := setup()
	entities := append(store1dbs, store2dbs...)

	assignments := []*placement.Assignment{}
	for _, entity := range entities {
		assignments = append(assignments, placement.NewAssignment(entity))
	}
	placer.Place(assignments, groups, groups)
	for _, assigment := range assignments {
		assert.False(t, assigment.Failed)
	}
}

func setupTwoGroupsOneAssignment() (placer Placer, relocator Relocator, assignment *placement.Assignment,
	free *placement.Group, unassigned *placement.Entity) {
	placer, relocator, groups, store1dbs, store2dbs := setup()
	assignment = placement.NewAssignment(store1dbs[0])
	selectedGroups := []*placement.Group{groups[0]}
	placer.Place([]*placement.Assignment{assignment}, selectedGroups, selectedGroups)
	free = groups[1]
	unassigned = store2dbs[0]

	return
}

func TestPlacer_Place_with_unassigned_entity_assigns_a_group_to_the_entity(t *testing.T) {
	placer, _, assignment1, free, unassigned := setupTwoGroupsOneAssignment()
	assignment2 := placement.NewAssignment(unassigned)

	// Assign the unassigned entity to the same group as that of assignment1
	groups1 := []*placement.Group{assignment1.AssignedGroup}
	placer.Place([]*placement.Assignment{assignment2}, groups1, groups1)
	assert.Equal(t, assignment1.AssignedGroup, assignment2.AssignedGroup)

	// Let the placer reassign the entity of assignment2 to the free group if it is better
	groups2 := []*placement.Group{assignment1.AssignedGroup, free}
	placer.Place([]*placement.Assignment{assignment2}, groups2, groups2)
	assert.Equal(t, free, assignment2.AssignedGroup)
}

func TestPlacer_Place_updates_metrics_and_relations_of_assigned_groups(t *testing.T) {
	placer, _, assignment1, _, unassigned := setupTwoGroupsOneAssignment()
	assignment2 := placement.NewAssignment(unassigned)

	memoryUsed := unassigned.Metrics.Get(metrics.MemoryUsed)
	diskUsed := unassigned.Metrics.Get(metrics.DiskUsed)

	memoryUsedBefore := assignment1.AssignedGroup.Metrics.Get(metrics.MemoryUsed)
	diskUsedBefore := assignment1.AssignedGroup.Metrics.Get(metrics.DiskUsed)

	// Assign the unassigned entity to the same group as that of assignment1
	groups := []*placement.Group{assignment1.AssignedGroup}
	placer.Place([]*placement.Assignment{assignment2}, groups, groups)

	memoryUsedAfter := assignment1.AssignedGroup.Metrics.Get(metrics.MemoryUsed)
	diskUsedAfter := assignment1.AssignedGroup.Metrics.Get(metrics.DiskUsed)

	assert.Equal(t, memoryUsed+memoryUsedBefore, memoryUsedAfter)
	assert.Equal(t, diskUsed+diskUsedBefore, diskUsedAfter)
}
