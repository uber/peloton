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
	"github.com/stretchr/testify/assert"
	"testing"
)

func hostWithoutIssue() (*labels.LabelBag, *labels.LabelBag) {
	labelBag := labels.NewLabelBag()
	labelBag.Add(labels.NewLabel("datacenter", "dc1"))
	labelBag.Add(labels.NewLabel("rack", "dc1-a007"))
	labelBag.Add(labels.NewLabel("host", "host42-dc1"))

	relationBag := labels.NewLabelBag()
	relationBag.Add(labels.NewLabel("redis", "instance", "store1"))

	return labelBag, relationBag
}

func hostWithIssue() (*labels.LabelBag, *labels.LabelBag) {
	labelBag := labels.NewLabelBag()
	labelBag.Add(labels.NewLabel("datacenter", "dc1"))
	labelBag.Add(labels.NewLabel("rack", "dc1-a007"))
	labelBag.Add(labels.NewLabel("host", "host42-dc1"))
	labelBag.Add(labels.NewLabel("issues", "someissue"))

	relationBag := labels.NewLabelBag()

	return labelBag, relationBag
}

func hostWithZFSVolume() (*labels.LabelBag, *labels.LabelBag) {
	labelBag := labels.NewLabelBag()
	labelBag.Add(labels.NewLabel("datacenter", "dc1"))
	labelBag.Add(labels.NewLabel("rack", "dc1-a007"))
	labelBag.Add(labels.NewLabel("host", "host42-dc1"))
	labelBag.Add(labels.NewLabel("volume-types", "zfs"))

	relationBag := labels.NewLabelBag()

	return labelBag, relationBag
}

func TestLabelRequirement_String_and_Composite(t *testing.T) {
	requirement := NewLabelRequirement(
		labels.NewLabel("rack", "dc1-a009"),
		labels.NewLabel("issues", "*"),
		LessThanEqual,
		0,
	)

	assert.Equal(t, "applies to rack.dc1-a009 and requires that the occurrences of the label issues.* should"+
		" be less_than_equal 0", requirement.String())
	composite, name := requirement.Composite()
	assert.False(t, composite)
	assert.Equal(t, "label", name)
}

func TestLabelRequirement_Fulfilled_FulfilledOnBagWhereTheRequirementDoesNotApply(t *testing.T) {
	labelBag, relationBag := hostWithIssue()

	requirement := NewLabelRequirement(
		labels.NewLabel("rack", "dc1-a009"),
		labels.NewLabel("issues", "*"),
		LessThanEqual,
		0,
	)
	transcript := NewTranscript("transcript")
	assert.True(t, requirement.Fulfilled(labelBag, relationBag, transcript))
	assert.Equal(t, 1, transcript.GroupsPassed)
	assert.Equal(t, 0, transcript.GroupsFailed)
}

func TestLabelRequirement_Fulfilled_FulfilledOnBagWithoutIssues(t *testing.T) {
	labelBag, relationBag := hostWithoutIssue()

	requirement := NewLabelRequirement(
		labels.NewLabel("host", "*"),
		labels.NewLabel("issues", "*"),
		LessThanEqual,
		0,
	)
	transcript := NewTranscript("transcript")
	assert.True(t, requirement.Fulfilled(labelBag, relationBag, transcript))
	assert.Equal(t, 1, transcript.GroupsPassed)
	assert.Equal(t, 0, transcript.GroupsFailed)
}

func TestLabelRequirement_Fulfilled_NotFulfilledOnBagWithIssues(t *testing.T) {
	labelBag, relationBag := hostWithIssue()

	requirement := NewLabelRequirement(
		labels.NewLabel("host", "*"),
		labels.NewLabel("issues", "*"),
		LessThanEqual,
		0,
	)
	transcript := NewTranscript("transcript")
	assert.False(t, requirement.Fulfilled(labelBag, relationBag, transcript))
	assert.Equal(t, 0, transcript.GroupsPassed)
	assert.Equal(t, 1, transcript.GroupsFailed)
}

func TestLabelRequirement_Fulfilled_ForHostWithZFSVolumeTypes(t *testing.T) {
	labelBag, relationBag := hostWithZFSVolume()

	requirement := NewLabelRequirement(
		labels.NewLabel("host", "*"),
		labels.NewLabel("volume-types", "zfs"),
		Equal,
		1,
	)
	assert.True(t, requirement.Fulfilled(labelBag, relationBag, nil))
}

func TestLabelRequirement_Fulfilled_IsUnfulfilledForInvalidComparison(t *testing.T) {
	labelBag, relationBag := hostWithZFSVolume()

	requirement := NewLabelRequirement(
		labels.NewLabel("host", "*"),
		labels.NewLabel("volume-types", "zfs"),
		Comparison("invalid"),
		1,
	)
	assert.False(t, requirement.Fulfilled(labelBag, relationBag, nil))
}
