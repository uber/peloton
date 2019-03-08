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
	"math"

	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
)

// Relocator can create assignments for entities.
type Relocator interface {
	// Relocate takes a slice of relocation ranks with entities attached and groups, it then updates the relocation
	// ranks with how many other groups that are better to place the entities on.
	Relocate(relocationRanks []*placement.RelocationRank, groups []*placement.Group, scopeSet *placement.ScopeSet)
}

// NewRelocator creates a new relocator. If the concurrency is <= 0 then concurrency is disabled no matter, what
// the concurrency number is set to. The minimum size is the minimal number of groups there should be before
// concurrency is enabled.
func NewRelocator(concurrency, minimumSize int) Relocator {
	return &relocator{
		concurrency: concurrency,
		minimumSize: minimumSize,
	}
}

type relocator struct {
	concurrency int
	minimumSize int
}

func (_relocator *relocator) relocateOnce(relocationRank *placement.RelocationRank,
	groups []*placement.Group, scopeSet *placement.ScopeSet, transcript *placement.Transcript) int {
	currentGroup := relocationRank.CurrentGroup
	entity := relocationRank.Entity
	rankIncrease := 0
	for _, group := range groups {
		if !entity.Requirement.Passed(group, scopeSet, entity, transcript) {
			continue
		}
		if placement.Less(entity.Ordering.Tuple(group, scopeSet, entity),
			entity.Ordering.Tuple(currentGroup, scopeSet, entity)) {
			// If the group is better than the current group then increase the rank
			rankIncrease++
		}
	}
	return rankIncrease
}

type relocateResult struct {
	transcript   *placement.Transcript
	rankIncrease int
}

func (_relocator *relocator) relocateConcurrent(relocationRank *placement.RelocationRank,
	groups []*placement.Group, scopeSet *placement.ScopeSet) {
	if _relocator.concurrency <= 1 || len(groups) < _relocator.minimumSize {
		relocationRank.Rank = _relocator.relocateOnce(relocationRank, groups, scopeSet, relocationRank.Transcript)
		return
	}

	results := make(chan relocateResult, _relocator.concurrency)
	index := 0
	for i := 0; i < _relocator.concurrency; i++ {
		length := int(math.Ceil(float64(len(groups)) / float64(_relocator.concurrency)))
		if index+length >= len(groups) {
			length = len(groups) - index
		}
		go func(selectedGroups []*placement.Group, scopeSet *placement.ScopeSet, transcript *placement.Transcript) {
			result := relocateResult{
				transcript:   transcript,
				rankIncrease: _relocator.relocateOnce(relocationRank, selectedGroups, scopeSet, transcript),
			}
			results <- result
		}(groups[index:index+length], scopeSet.Copy(), relocationRank.Transcript.Copy())
		index += length
	}

	for i := 0; i < _relocator.concurrency; i++ {
		select {
		case result := <-results:
			relocationRank.Rank += result.rankIncrease
			relocationRank.Transcript.Add(result.transcript)
		}
	}
}

func (_relocator *relocator) Relocate(relocationRanks []*placement.RelocationRank, groups []*placement.Group,
	scopeSet *placement.ScopeSet) {
	for _, relocationRank := range relocationRanks {
		currentGroup := relocationRank.CurrentGroup
		entity := relocationRank.Entity

		// Remove the entity from the current group before comparing the current group with other groups
		currentGroup.Entities.Remove(entity)
		currentGroup.Update()

		_relocator.relocateConcurrent(relocationRank, groups, scopeSet)

		// Add the entity back to the current group after having updated its relocation rank
		currentGroup.Entities.Add(entity)
		currentGroup.Update()
	}
}
