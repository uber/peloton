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
)

// Placer can create assignments for entities.
type Placer interface {
	// Place takes a slice of assignments with entities attached and groups it then updates the assignments with the
	// groups where it will be beneficial to place the entities.
	Place(assignments []*placement.Assignment, groups []*placement.Group)
}

// NewPlacer creates a new placer given a deriver for computing derived metrics.
func NewPlacer(deriver metrics.Deriver) Placer {
	return &placer{
		deriver: deriver,
	}
}

type placer struct {
	deriver metrics.Deriver
}

func (_placer *placer) Place(assignments []*placement.Assignment, groups []*placement.Group) {
	for _, assignment := range assignments {
		bestGroup := assignment.AssignedGroup
		entity := assignment.Entity
		for _, group := range groups {
			if !entity.Fulfilled(group, assignment.Transcript) {
				continue
			}
			if bestGroup == nil {
				bestGroup = group
			} else if entity.Ordering.Less(group, bestGroup, entity) {
				bestGroup = group
			}
		}
		if assignment.AssignedGroup != nil {
			assignment.AssignedGroup.Entities.Remove(entity)
			assignment.AssignedGroup.Update(_placer.deriver)
		}
		if bestGroup != nil {
			assignment.AssignedGroup = bestGroup
			bestGroup.Entities.Add(entity)
			bestGroup.Update(_placer.deriver)
			assignment.Failed = false
		}
	}
}
