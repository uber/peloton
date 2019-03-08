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
	"testing"

	"strings"

	"github.com/stretchr/testify/assert"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/labels"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
)

func setupAndRequirement() *AndRequirement {
	return NewAndRequirement(
		NewLabelRequirement(
			nil,
			labels.NewLabel("issues", "*"),
			LessThanEqual,
			0,
		),
		NewLabelRequirement(
			nil,
			labels.NewLabel("rack", "dc1-a007"),
			GreaterThanEqual,
			1,
		),
	)
}

func TestAndRequirement_String_and_Composite(t *testing.T) {
	requirement := setupAndRequirement()

	assert.Equal(t, fmt.Sprintf("all of the requirements; %v, %v, should be true",
		requirement.Requirements[0].String(),
		requirement.Requirements[1].String()),
		requirement.String())
	composite, name := requirement.Composite()
	assert.True(t, composite)
	assert.Equal(t, "and", name)
}

func TestAndRequirement_Fulfilled_updates_transcript_and_delegates_updates(t *testing.T) {
	group := placement.NewGroup("group")
	group.Labels, group.Relations = hostWithIssue()
	scopeSet := placement.NewScopeSet(nil)

	requirement := setupAndRequirement()

	transcript := placement.NewTranscript("transcript")
	requirement.Passed(group, scopeSet, nil, transcript)
	assert.Equal(t, 0, transcript.GroupsPassed)
	assert.Equal(t, 1, transcript.GroupsFailed)
	assert.Equal(t, 2, len(transcript.Subscripts))
	for description, subscript := range transcript.Subscripts {
		if strings.Contains(description.String(), "issues") {
			assert.Equal(t, 0, subscript.GroupsPassed)
			assert.Equal(t, 1, subscript.GroupsFailed)
		} else {
			assert.Equal(t, 1, subscript.GroupsPassed)
			assert.Equal(t, 0, subscript.GroupsFailed)
		}
	}
}

func TestAndRequirement_Fulfilled_returns_true_if_subrequirements_are_true(t *testing.T) {
	group := placement.NewGroup("group")
	group.Labels, group.Relations = hostWithoutIssue()
	scopeSet := placement.NewScopeSet(nil)

	requirement := setupAndRequirement()

	transcript := placement.NewTranscript("transcript")
	assert.True(t, requirement.Passed(group, scopeSet, nil, transcript))
}

func TestAndRequirement_Fulfilled_returns_false_if_any_subrequirements_are_false(t *testing.T) {
	group := placement.NewGroup("group")
	group.Labels, group.Relations = hostWithoutIssue()
	scopeSet := placement.NewScopeSet(nil)

	requirement := NewAndRequirement(
		NewLabelRequirement(
			nil,
			labels.NewLabel("issues", "*"),
			LessThanEqual,
			0,
		),
		NewLabelRequirement(
			nil,
			labels.NewLabel("rack", "dc1-a007"),
			LessThanEqual,
			0,
		),
	)

	assert.False(t, requirement.Passed(group, scopeSet, nil, nil))
}
