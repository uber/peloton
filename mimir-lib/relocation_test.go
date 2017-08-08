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

package mimir

import (
	"code.uber.internal/infra/peloton/mimir-lib/model/metrics"
	"code.uber.internal/infra/peloton/mimir-lib/model/placement"
	"github.com/stretchr/testify/assert"
	"testing"
)

func setupTwoGroupsTwoAssignments() (placer Placer, relocator Relocator, assignment1, assignment2 *placement.Assignment,
	free *placement.Group) {
	placer, relocator, assignment1, free, unassigned := setupTwoGroupsOneAssignment()
	assignment2 = placement.NewAssignment(unassigned)

	// Assign the unassigned entity to the same group as that of assignment1
	placer.Place([]*placement.Assignment{assignment2}, []*placement.Group{assignment1.AssignedGroup})

	return
}

func TestRelocator_Relocate_will_give_rank_0_for_entities_with_optimal_placement(t *testing.T) {
	placer, relocator, groups, store1dbs, store2dbs := setup()
	entities := append(store1dbs, store2dbs...)

	assignments := []*placement.Assignment{}
	for _, entity := range entities {
		assignments = append(assignments, placement.NewAssignment(entity))
	}
	placer.Place(assignments, groups)
	for _, assigment := range assignments {
		assert.False(t, assigment.Failed)
	}

	relocations := []*placement.RelocationRank{}
	for _, assignment := range assignments {
		relocations = append(relocations, placement.NewRelocationRank(assignment.Entity, assignment.AssignedGroup))
	}
	relocator.Relocate(relocations, groups)
	for _, relocation := range relocations {
		assert.Equal(t, 0, relocation.Rank)
	}
}

func TestRelocator_Relocate_will_increase_rank_for_every_better_group(t *testing.T) {
	_, relocator, assignment1, _, free := setupTwoGroupsTwoAssignments()

	rank := placement.NewRelocationRank(assignment1.Entity, assignment1.AssignedGroup)

	relocator.Relocate([]*placement.RelocationRank{rank}, []*placement.Group{assignment1.AssignedGroup, free})

	assert.Equal(t, 1, rank.Rank)
}

func TestRelocator_Relocate_will_skip_entities_that_are_not_relocateable(t *testing.T) {
	_, relocator, assignment1, _, free := setupTwoGroupsTwoAssignments()

	assignment1.Entity.Relocateable = false
	rank := placement.NewRelocationRank(assignment1.Entity, assignment1.AssignedGroup)

	relocator.Relocate([]*placement.RelocationRank{rank}, []*placement.Group{assignment1.AssignedGroup, free})

	assert.Equal(t, 0, rank.Rank)
}

func TestRelocator_Relocate_will_restore_group_metrics_and_relations_after_a_call(t *testing.T) {
	_, relocator, assignment1, _, free := setupTwoGroupsTwoAssignments()

	rank := placement.NewRelocationRank(assignment1.Entity, assignment1.AssignedGroup)

	memoryUsedBefore1 := assignment1.AssignedGroup.Metrics.Get(metrics.MemoryUsed)
	diskUsedBefore1 := assignment1.AssignedGroup.Metrics.Get(metrics.DiskUsed)
	memoryUsedBefore2 := free.Metrics.Get(metrics.MemoryUsed)
	diskUsedBefore2 := free.Metrics.Get(metrics.DiskUsed)

	relationsBefore1 := assignment1.AssignedGroup.Relations.Size()
	relationsBefore2 := free.Relations.Size()

	relocator.Relocate([]*placement.RelocationRank{rank}, []*placement.Group{assignment1.AssignedGroup, free})

	memoryUsedAfter1 := assignment1.AssignedGroup.Metrics.Get(metrics.MemoryUsed)
	diskUsedAfter1 := assignment1.AssignedGroup.Metrics.Get(metrics.DiskUsed)
	memoryUsedAfter2 := free.Metrics.Get(metrics.MemoryUsed)
	diskUsedAfter2 := free.Metrics.Get(metrics.DiskUsed)

	relationsAfter1 := assignment1.AssignedGroup.Relations.Size()
	relationsAfter2 := free.Relations.Size()

	assert.Equal(t, memoryUsedBefore1, memoryUsedAfter1)
	assert.Equal(t, memoryUsedBefore2, memoryUsedAfter2)
	assert.Equal(t, diskUsedBefore1, diskUsedAfter1)
	assert.Equal(t, diskUsedBefore2, diskUsedAfter2)

	assert.Equal(t, relationsBefore1, relationsAfter1)
	assert.Equal(t, relationsBefore2, relationsAfter2)
}
