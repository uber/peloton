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

// Placer can create assignments for entities.
type Placer interface {
	// Place takes a slice of assignments with entities attached and groups it then updates the assignments with the
	// groups where it will be beneficial to place the entities.
	Place(assignments []*placement.Assignment, groups []*placement.Group, scopeSet *placement.ScopeSet)
}

// NewPlacer creates a new placer. If the concurrency is <= 0 then concurrency is disabled no matter, what
// the concurrency number is set to.
func NewPlacer(concurrency, minimumSize int) Placer {
	return &placer{
		concurrency: concurrency,
		minimumSize: minimumSize,
	}
}

type placer struct {
	concurrency int
	minimumSize int
}

func (_placer *placer) placeOnce(assignment *placement.Assignment, groups []*placement.Group, scopeSet *placement.ScopeSet,
	transcript *placement.Transcript) *placement.Group {
	bestGroup := assignment.AssignedGroup
	entity := assignment.Entity
	for _, group := range groups {
		if !entity.Requirement.Passed(group, scopeSet, entity, transcript) {
			continue
		}
		if bestGroup == nil {
			bestGroup = group
		} else if placement.Less(entity.Ordering.Tuple(group, scopeSet, entity),
			entity.Ordering.Tuple(bestGroup, scopeSet, entity)) {
			bestGroup = group
		}
	}
	return bestGroup
}

type placementResult struct {
	transcript    *placement.Transcript
	assignedGroup *placement.Group
}

func (_placer *placer) placeConcurrent(assignment *placement.Assignment, groups []*placement.Group, scopeSet *placement.ScopeSet) {
	bestGroup := assignment.AssignedGroup
	entity := assignment.Entity

	if _placer.concurrency <= 1 || len(groups) < _placer.minimumSize {
		bestGroup = _placer.placeOnce(assignment, groups, scopeSet, assignment.Transcript)
	} else {
		results := make(chan placementResult, _placer.concurrency)
		index := 0
		for i := 0; i < _placer.concurrency; i++ {
			length := int(math.Ceil(float64(len(groups)) / float64(_placer.concurrency)))
			if index+length >= len(groups) {
				length = len(groups) - index
			}
			go func(selectedGroups []*placement.Group, scopeSet *placement.ScopeSet, transcript *placement.Transcript) {
				result := placementResult{
					transcript:    transcript,
					assignedGroup: _placer.placeOnce(assignment, selectedGroups, scopeSet, transcript),
				}
				results <- result
			}(groups[index:index+length], scopeSet.Copy(), assignment.Transcript.Copy())
			index += length
		}

		for i := 0; i < _placer.concurrency; i++ {
			select {
			case result := <-results:
				assignment.Transcript.Add(result.transcript)
				if bestGroup == nil {
					bestGroup = result.assignedGroup
				} else if result.assignedGroup != nil && placement.Less(
					entity.Ordering.Tuple(result.assignedGroup, scopeSet, entity),
					entity.Ordering.Tuple(bestGroup, scopeSet, entity)) {
					bestGroup = result.assignedGroup
				}
			}
		}
	}

	if assignment.AssignedGroup != nil {
		assignment.AssignedGroup.Entities.Remove(entity)
		assignment.AssignedGroup.Update()
	}
	if bestGroup != nil {
		assignment.AssignedGroup = bestGroup
		bestGroup.Entities.Add(entity)
		bestGroup.Update()
		assignment.Failed = false
	}
}

func (_placer *placer) Place(assignments []*placement.Assignment, groups []*placement.Group, scopeSet *placement.ScopeSet) {
	for _, assignment := range assignments {
		_placer.placeConcurrent(assignment, groups, scopeSet)
	}
}
