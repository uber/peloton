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

package requirements

import (
	"fmt"

	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/labels"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
)

// RelationRequirement represents a requirement on the number of occurrences for a specific relation on a group.
// The requirement will only apply if the group contains the applies to label.
//
// An example initialization could be:
//	requirement := NewRelationRequirement(
//		labels.NewLabel("rack", "dc1-a009"),
//		labels.NewLabel("redis", "instance", "store1"),
//		LessThanEqual,
//		0,
//	)
// which only applies to groups in rack dc1-a009 and requires that there are 0 or less occurrences of the
// relation redis.instance.store1 on the group.
type RelationRequirement struct {
	Scope       *labels.Label
	Relation    *labels.Label
	Comparison  Comparison
	Occurrences int
}

// NewRelationRequirement creates a new relation requirement.
func NewRelationRequirement(scope, relation *labels.Label, comparison Comparison, occurrences int) *RelationRequirement {
	return &RelationRequirement{
		Scope:       scope,
		Relation:    relation,
		Comparison:  comparison,
		Occurrences: occurrences,
	}
}

// Passed checks if the requirement is fulfilled by the given group within the scope groups.
func (requirement *RelationRequirement) Passed(group *placement.Group, scopeSet *placement.ScopeSet,
	entity *placement.Entity, transcript *placement.Transcript) bool {
	occurrences := scopeSet.RelationScope(group, requirement.Scope).Count(requirement.Relation)
	fulfilled, err := requirement.Comparison.Compare(float64(occurrences), float64(requirement.Occurrences))
	if err != nil || !fulfilled {
		transcript.IncFailed()
		return false
	}
	transcript.IncPassed()
	return true
}

func (requirement *RelationRequirement) String() string {
	return fmt.Sprintf("requires that the occurrences of the relation %v should be %v %v in scope %v",
		requirement.Relation, requirement.Comparison, requirement.Occurrences, requirement.Scope)
}

// Composite returns false as the requirement is not composite and the name of the requirement type.
func (requirement *RelationRequirement) Composite() (bool, string) {
	return false, "relation"
}
