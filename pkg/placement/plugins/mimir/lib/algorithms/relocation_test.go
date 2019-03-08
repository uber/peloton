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
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/metrics"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
)

func setupTwoGroupsTwoAssignments(concurrency int) (placer Placer, relocator Relocator, assignment1, assignment2 *placement.Assignment,
	free *placement.Group) {
	placer, relocator, assignment1, free, unassigned := setupTwoGroupsOneAssignment(concurrency)
	assignment2 = placement.NewAssignment(unassigned)

	// Assign the unassigned entity to the same group as that of assignment1
	groups := []*placement.Group{assignment1.AssignedGroup}
	scopeSet := placement.NewScopeSet(groups)
	placer.Place([]*placement.Assignment{assignment2}, groups, scopeSet)

	return
}

func TestRelocator_Relocate_takes_all_groups_into_account_when_checking_if_there_is_a_better_group_concurrently(t *testing.T) {
	_, relocator, groups, store1dbs, _ := setup(2)
	groups[0].Entities.Add(store1dbs[0])
	groups[0].Entities.Add(store1dbs[1])
	groups[0].Update()

	relocations := []*placement.RelocationRank{placement.NewRelocationRank(store1dbs[0], groups[0])}
	scopeSet := placement.NewScopeSet(groups[0:3])
	relocator.Relocate(relocations, groups[0:3], scopeSet)

	assert.Equal(t, 2, relocations[0].Rank)
}

func TestRelocator_Relocate_will_give_rank_0_for_entities_with_optimal_placement(t *testing.T) {
	for concurrency := 1; concurrency <= 2; concurrency++ {
		placer, relocator, groups, store1dbs, store2dbs := setup(concurrency)
		entities := append(store1dbs, store2dbs...)

		var assignments []*placement.Assignment
		for _, entity := range entities {
			assignments = append(assignments, placement.NewAssignment(entity))
		}
		scopeSet := placement.NewScopeSet(groups)
		placer.Place(assignments, groups, scopeSet)
		for _, assigment := range assignments {
			assert.False(t, assigment.Failed)
		}

		var relocations []*placement.RelocationRank
		for _, assignment := range assignments {
			relocations = append(relocations, placement.NewRelocationRank(assignment.Entity, assignment.AssignedGroup))
		}
		relocator.Relocate(relocations, groups, scopeSet)
		for _, relocation := range relocations {
			assert.Equal(t, 0, relocation.Rank)
		}
	}
}

func TestRelocator_Relocate_will_increase_rank_for_every_better_group(t *testing.T) {
	for concurrency := 1; concurrency <= 2; concurrency++ {
		_, relocator, assignment1, _, free := setupTwoGroupsTwoAssignments(concurrency)

		rank := placement.NewRelocationRank(assignment1.Entity, assignment1.AssignedGroup)

		groups := []*placement.Group{assignment1.AssignedGroup, free}
		scopeSet := placement.NewScopeSet(groups)
		relocator.Relocate([]*placement.RelocationRank{rank}, groups, scopeSet)

		assert.Equal(t, 1, rank.Rank)
	}
}

func TestRelocator_Relocate_will_restore_group_metrics_and_relations_after_a_call(t *testing.T) {
	for concurrency := 1; concurrency <= 2; concurrency++ {
		_, relocator, assignment1, _, free := setupTwoGroupsTwoAssignments(concurrency)

		rank := placement.NewRelocationRank(assignment1.Entity, assignment1.AssignedGroup)

		memoryUsedBefore1 := assignment1.AssignedGroup.Metrics.Get(metrics.MemoryUsed)
		diskUsedBefore1 := assignment1.AssignedGroup.Metrics.Get(metrics.DiskUsed)
		memoryUsedBefore2 := free.Metrics.Get(metrics.MemoryUsed)
		diskUsedBefore2 := free.Metrics.Get(metrics.DiskUsed)

		relationsBefore1 := assignment1.AssignedGroup.Relations.Size()
		relationsBefore2 := free.Relations.Size()

		groups := []*placement.Group{assignment1.AssignedGroup, free}
		scopeSet := placement.NewScopeSet(groups)
		relocator.Relocate([]*placement.RelocationRank{rank}, groups, scopeSet)

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
}
