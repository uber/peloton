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

package requirements

import (
	"code.uber.internal/infra/peloton/mimir-lib/model/labels"
	"fmt"
	"strings"
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
	AffinityRequirements []AffinityRequirement
}

// NewOrRequirement creates a new or requirement.
func NewOrRequirement(requirements ...AffinityRequirement) *OrRequirement {
	return &OrRequirement{
		AffinityRequirements: requirements,
	}
}

// Fulfilled checks if the requirement is fulfilled by the given label and relation bags.
func (requirement *OrRequirement) Fulfilled(labelBag, relationBag *labels.LabelBag, transcript *Transcript) bool {
	for _, subRequirement := range requirement.AffinityRequirements {
		if subRequirement.Fulfilled(labelBag, relationBag, transcript.Subscript(subRequirement)) {
			transcript.IncPassed()
			return true
		}
	}
	transcript.IncFailed()
	return false
}

func (requirement *OrRequirement) String() string {
	subRequirements := make([]string, 0, len(requirement.AffinityRequirements))
	for _, subRequirement := range requirement.AffinityRequirements {
		subRequirements = append(subRequirements, subRequirement.String())
	}
	return fmt.Sprintf("at least one of the requirements; %v, should be true",
		strings.Join(subRequirements, ", "))
}

// Composite returns true as the requirement is composite and the name of its composite nature.
func (requirement *OrRequirement) Composite() (bool, string) {
	return true, "or"
}
