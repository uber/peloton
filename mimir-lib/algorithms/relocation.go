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

import "code.uber.internal/infra/peloton/mimir-lib/model/placement"

// Relocator can create assignments for entities.
type Relocator interface {
	// Relocate takes a slice of relocation ranks with entities attached and groups, it then updates the relocation
	// ranks with how many other groups that are better to place the entities on.
	Relocate(relocationRanks []*placement.RelocationRank, groups, scopeGroups []*placement.Group)
}

// NewRelocator creates a new relocator given a deriver for computing derived metrics.
func NewRelocator() Relocator {
	return &relocator{}
}

type relocator struct{}

func (_relocator *relocator) Relocate(relocationRanks []*placement.RelocationRank,
	groups, scopeGroups []*placement.Group) {
	for _, relocationRank := range relocationRanks {
		currentGroup := relocationRank.CurrentGroup
		entity := relocationRank.Entity
		// Remove the entity from the current group before comparing the current group with other groups
		currentGroup.Entities.Remove(entity)
		currentGroup.Update()
		for _, group := range groups {
			if !entity.Requirement.Passed(group, scopeGroups, entity, relocationRank.Transcript) {
				continue
			}
			if entity.Ordering.Less(group, relocationRank.CurrentGroup, scopeGroups, entity) {
				// If the group is better than the current group then increase the rank
				relocationRank.Rank++
			}
		}
		// Add the entity back to the current group after having updated its relocation rank
		currentGroup.Entities.Add(entity)
		currentGroup.Update()
	}
}
