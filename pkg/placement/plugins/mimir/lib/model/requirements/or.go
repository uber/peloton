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
	"strings"

	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
)

// OrRequirement represents an "or" of a set of sub affinity requirements which can be another or, and, label or relation
// requirement.
//
// An example initialization could be:
//	requirement := NewOrRequirement(
//		subRequirement1,
//		subRequirement2,
//		...
//	)
type OrRequirement struct {
	Requirements []placement.Requirement
}

// NewOrRequirement creates a new or requirement.
func NewOrRequirement(requirements ...placement.Requirement) *OrRequirement {
	return &OrRequirement{
		Requirements: requirements,
	}
}

// Passed checks if the requirement is fulfilled by the given group within the scope groups.
func (requirement *OrRequirement) Passed(group *placement.Group, scopeSet *placement.ScopeSet,
	entity *placement.Entity, transcript *placement.Transcript) bool {
	result := false
	for _, subRequirement := range requirement.Requirements {
		if subRequirement.Passed(group, scopeSet, entity, transcript.Subscript(subRequirement)) {
			result = true
		}
	}
	if result {
		transcript.IncPassed()
	} else {
		transcript.IncFailed()
	}
	return result
}

func (requirement *OrRequirement) String() string {
	subRequirements := make([]string, 0, len(requirement.Requirements))
	for _, subRequirement := range requirement.Requirements {
		subRequirements = append(subRequirements, subRequirement.String())
	}
	return fmt.Sprintf("at least one of the requirements; %v, should be true",
		strings.Join(subRequirements, ", "))
}

// Composite returns true as the requirement is composite and the name of its composite nature.
func (requirement *OrRequirement) Composite() (bool, string) {
	return true, "or"
}
