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

package requirements

import (
	"testing"

	"code.uber.internal/infra/peloton/mimir-lib/model/labels"
	"code.uber.internal/infra/peloton/mimir-lib/model/placement"
	"github.com/stretchr/testify/assert"
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
	labelBag.Add(labels.NewLabel("host", "host43-dc1"))
	labelBag.Add(labels.NewLabel("issues", "someissue"))

	relationBag := labels.NewLabelBag()

	return labelBag, relationBag
}

func hostWithZFSVolume() (*labels.LabelBag, *labels.LabelBag) {
	labelBag := labels.NewLabelBag()
	labelBag.Add(labels.NewLabel("datacenter", "dc1"))
	labelBag.Add(labels.NewLabel("rack", "dc1-a007"))
	labelBag.Add(labels.NewLabel("host", "host44-dc1"))
	labelBag.Add(labels.NewLabel("volume-types", "zfs"))

	relationBag := labels.NewLabelBag()

	return labelBag, relationBag
}

func TestLabelRequirement_String_and_Composite(t *testing.T) {
	requirement := NewLabelRequirement(
		nil,
		labels.NewLabel("issues", "*"),
		LessThanEqual,
		0,
	)

	assert.Equal(t, "requires that the occurrences of the label issues.* should"+
		" be less_than_equal 0", requirement.String())
	composite, name := requirement.Composite()
	assert.False(t, composite)
	assert.Equal(t, "label", name)
}

func TestLabelRequirement_Fulfilled_NotFulfilledOnScopedBagWithIssues(t *testing.T) {
	group1 := placement.NewGroup("group1")
	group1.Labels, group1.Relations = hostWithoutIssue()
	group2 := placement.NewGroup("group2")
	group2.Labels, group2.Relations = hostWithIssue()
	scope := []*placement.Group{group1, group2}
	requirement := NewLabelRequirement(
		labels.NewLabel("rack", "*"),
		labels.NewLabel("issues", "*"),
		LessThanEqual,
		0,
	)
	transcript := placement.NewTranscript("transcript")
	assert.False(t, requirement.Passed(group1, scope, nil, transcript))
	assert.Equal(t, 0, transcript.GroupsPassed)
	assert.Equal(t, 1, transcript.GroupsFailed)
}

func TestLabelRequirement_Fulfilled_FulfilledOnScopedBagWithoutIssues(t *testing.T) {
	group1 := placement.NewGroup("group1")
	group1.Labels, group1.Relations = hostWithoutIssue()
	group2 := placement.NewGroup("group2")
	group2.Labels, group2.Relations = hostWithoutIssue()
	scope := []*placement.Group{group1, group2}

	requirement := NewLabelRequirement(
		labels.NewLabel("rack", "*"),
		labels.NewLabel("issues", "*"),
		LessThanEqual,
		0,
	)
	transcript := placement.NewTranscript("transcript")
	assert.True(t, requirement.Passed(group1, scope, nil, transcript))
	assert.Equal(t, 1, transcript.GroupsPassed)
	assert.Equal(t, 0, transcript.GroupsFailed)
}

func TestLabelRequirement_Fulfilled_FulfilledOnBagWithoutIssues(t *testing.T) {
	group := placement.NewGroup("group")
	group.Labels, group.Relations = hostWithoutIssue()

	requirement := NewLabelRequirement(
		nil,
		labels.NewLabel("issues", "*"),
		LessThanEqual,
		0,
	)
	transcript := placement.NewTranscript("transcript")
	assert.True(t, requirement.Passed(group, nil, nil, transcript))
	assert.Equal(t, 1, transcript.GroupsPassed)
	assert.Equal(t, 0, transcript.GroupsFailed)
}

func TestLabelRequirement_Fulfilled_NotFulfilledOnBagWithIssues(t *testing.T) {
	group := placement.NewGroup("group")
	group.Labels, group.Relations = hostWithIssue()

	requirement := NewLabelRequirement(
		nil,
		labels.NewLabel("issues", "*"),
		LessThanEqual,
		0,
	)
	transcript := placement.NewTranscript("transcript")
	assert.False(t, requirement.Passed(group, nil, nil, transcript))
	assert.Equal(t, 0, transcript.GroupsPassed)
	assert.Equal(t, 1, transcript.GroupsFailed)
}

func TestLabelRequirement_Fulfilled_ForHostWithZFSVolumeTypes(t *testing.T) {
	group := placement.NewGroup("group")
	group.Labels, group.Relations = hostWithZFSVolume()

	requirement := NewLabelRequirement(
		nil,
		labels.NewLabel("volume-types", "zfs"),
		Equal,
		1,
	)
	assert.True(t, requirement.Passed(group, nil, nil, nil))
}

func TestLabelRequirement_Fulfilled_IsUnfulfilledForInvalidComparison(t *testing.T) {
	group := placement.NewGroup("group")
	group.Labels, group.Relations = hostWithZFSVolume()

	requirement := NewLabelRequirement(
		nil,
		labels.NewLabel("volume-types", "zfs"),
		Comparison("invalid"),
		1,
	)
	assert.False(t, requirement.Passed(group, nil, nil, nil))
}
