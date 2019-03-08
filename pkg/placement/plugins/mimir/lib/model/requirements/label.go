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

// LabelRequirement represents a requirement in relation to a specific group having a specific label,
// i.e. we want to be placed in a given data center or we do not want to be placed on a specific rack, etc.
//
// An example initialization could be:
//	requirement := NewLabelRequirement(
//		labels.NewLabel("host", "*"),
//		labels.NewLabel("volume-types", "zfs"),
//		Equal,
//		1,
//	)
// which applies to any group that is a host and requires that there is exactly 1 occurrence of the
// label volume-types.zfs on the group.
type LabelRequirement struct {
	Scope       *labels.Label
	Label       *labels.Label
	Comparison  Comparison
	Occurrences int
}

// NewLabelRequirement creates a new label requirement.
func NewLabelRequirement(scope, label *labels.Label, comparison Comparison, occurrences int) *LabelRequirement {
	return &LabelRequirement{
		Scope:       scope,
		Label:       label,
		Comparison:  comparison,
		Occurrences: occurrences,
	}
}

// Passed checks if the requirement is fulfilled by the given group within the scope groups.
func (requirement *LabelRequirement) Passed(group *placement.Group, scopeSet *placement.ScopeSet,
	entity *placement.Entity, transcript *placement.Transcript) bool {
	occurrences := scopeSet.LabelScope(group, requirement.Scope).Count(requirement.Label)
	fulfilled, err := requirement.Comparison.Compare(float64(occurrences), float64(requirement.Occurrences))
	if err != nil || !fulfilled {
		transcript.IncFailed()
		return false
	}
	transcript.IncPassed()
	return true
}

func (requirement *LabelRequirement) String() string {
	return fmt.Sprintf("requires that the occurrences of the label %v should be %v %v in scope %v",
		requirement.Label, requirement.Comparison, requirement.Occurrences, requirement.Scope)
}

// Composite returns false as the requirement is not composite and the name of the requirement type.
func (requirement *LabelRequirement) Composite() (bool, string) {
	return false, "label"
}
