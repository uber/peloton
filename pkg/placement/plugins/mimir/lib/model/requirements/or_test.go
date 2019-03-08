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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/labels"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
)

func setupOrRequirement() *OrRequirement {
	return NewOrRequirement(
		NewLabelRequirement(
			nil,
			labels.NewLabel("volume-types", "zfs"),
			GreaterThanEqual,
			1,
		),
		NewLabelRequirement(
			nil,
			labels.NewLabel("volume-types", "local"),
			GreaterThanEqual,
			1,
		),
	)
}

func TestOrRequirement_String_and_Composite(t *testing.T) {
	requirement := setupOrRequirement()

	assert.Equal(t, fmt.Sprintf("at least one of the requirements; %v, %v, should be true",
		requirement.Requirements[0].String(),
		requirement.Requirements[1].String()),
		requirement.String())
	composite, name := requirement.Composite()
	assert.True(t, composite)
	assert.Equal(t, "or", name)
}

func TestOrRequirement_Fulfilled_updates_transcript_and_delegates_updates(t *testing.T) {
	group := placement.NewGroup("group")
	group.Labels, group.Relations = hostWithZFSVolume()
	scopeSet := placement.NewScopeSet(nil)

	requirement := setupOrRequirement()

	transcript := placement.NewTranscript("transcript")
	requirement.Passed(group, scopeSet, nil, transcript)
	assert.Equal(t, 1, transcript.GroupsPassed)
	assert.Equal(t, 0, transcript.GroupsFailed)
	assert.Equal(t, 2, len(transcript.Subscripts))
	for description, subscript := range transcript.Subscripts {
		if strings.Contains(description.String(), "zfs") {
			assert.Equal(t, 1, subscript.GroupsPassed)
			assert.Equal(t, 0, subscript.GroupsFailed)
		} else {
			assert.Equal(t, 0, subscript.GroupsPassed)
			assert.Equal(t, 1, subscript.GroupsFailed)
		}
	}
}

func TestOrRequirement_Fulfilled_returns_true_if_any_subrequirement_is_true(t *testing.T) {
	group := placement.NewGroup("group")
	group.Labels, group.Relations = hostWithZFSVolume()
	scopeSet := placement.NewScopeSet(nil)

	requirement := setupOrRequirement()

	assert.True(t, requirement.Passed(group, scopeSet, nil, nil))
}

func TestOrRequirement_Fulfilled_returns_false_if_all_subrequirements_are_false(t *testing.T) {
	group := placement.NewGroup("group")
	group.Labels, group.Relations = hostWithoutIssue()
	scopeSet := placement.NewScopeSet(nil)

	requirement := setupOrRequirement()

	assert.False(t, requirement.Passed(group, scopeSet, nil, nil))
}
