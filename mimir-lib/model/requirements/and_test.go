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
	"github.com/stretchr/testify/assert"
	"testing"
)

func setupAndRequirement() *AndRequirement {
	return NewAndRequirement(
		NewLabelRequirement(
			labels.NewLabel("rack", "*"),
			labels.NewLabel("issues", "*"),
			LessThanEqual,
			0,
		),
		NewLabelRequirement(
			labels.NewLabel("rack", "*"),
			labels.NewLabel("rack", "dc1-a009"),
			LessThanEqual,
			0,
		),
	)
}

func TestAndRequirement_String_and_Composite(t *testing.T) {
	requirement := setupAndRequirement()

	assert.Equal(t, fmt.Sprintf("all of the requirements; %v, %v, should be true",
		requirement.AffinityRequirements[0].String(),
		requirement.AffinityRequirements[1].String()),
		requirement.String())
	composite, name := requirement.Composite()
	assert.True(t, composite)
	assert.Equal(t, "and", name)
}

func TestAndRequirement_Fulfilled_updates_transcript_and_delegates_updates(t *testing.T) {
	labelBag, relationBag := hostWithoutIssue()

	requirement := setupAndRequirement()

	transcript := NewTranscript("transcript")
	requirement.Fulfilled(labelBag, relationBag, transcript)
	assert.Equal(t, 1, transcript.GroupsPassed)
	assert.Equal(t, 0, transcript.GroupsFailed)
	assert.Equal(t, 2, len(transcript.Subscripts))
	for _, subscript := range transcript.Subscripts {
		assert.Equal(t, 1, subscript.GroupsPassed)
		assert.Equal(t, 0, subscript.GroupsFailed)
	}
}

func TestAndRequirement_Fulfilled_returns_true_if_subrequirements_are_true(t *testing.T) {
	labelBag, relationBag := hostWithoutIssue()

	requirement := setupAndRequirement()

	transcript := NewTranscript("transcript")
	assert.True(t, requirement.Fulfilled(labelBag, relationBag, transcript))
}

func TestAndRequirement_Fulfilled_returns_false_if_any_subrequirements_are_false(t *testing.T) {
	labelBag, relationBag := hostWithoutIssue()

	requirement := NewAndRequirement(
		NewLabelRequirement(
			labels.NewLabel("rack", "*"),
			labels.NewLabel("issues", "*"),
			LessThanEqual,
			0,
		),
		NewLabelRequirement(
			labels.NewLabel("rack", "*"),
			labels.NewLabel("rack", "dc1-a007"),
			LessThanEqual,
			0,
		),
	)

	assert.False(t, requirement.Fulfilled(labelBag, relationBag, nil))
}
