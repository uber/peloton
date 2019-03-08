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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/labels"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
)

func TestRelationRequirement_String_and_Composite(t *testing.T) {
	requirement := NewRelationRequirement(
		nil,
		labels.NewLabel("redis", "instance", "store1"),
		LessThanEqual,
		0,
	)

	assert.Equal(t, "requires that the occurrences of the relation"+
		" redis.instance.store1 should be less_than_equal 0 in scope <nil>", requirement.String())
	composite, name := requirement.Composite()
	assert.False(t, composite)
	assert.Equal(t, "relation", name)
}

func TestRelationRequirement_Fulfilled_FulfilledOnScopedBagWithoutTheRelation(t *testing.T) {
	group1 := placement.NewGroup("group1")
	group1.Labels, group1.Relations = hostWithoutIssue()
	group2 := placement.NewGroup("group2")
	group2.Labels, group2.Relations = hostWithZFSVolume()
	groups := []*placement.Group{group1, group2}
	scopeSet := placement.NewScopeSet(groups)

	requirement := NewRelationRequirement(
		labels.NewLabel("rack", "*"),
		labels.NewLabel("redis", "instance", "store2"),
		LessThanEqual,
		0,
	)
	transcript := placement.NewTranscript("transcript")
	assert.True(t, requirement.Passed(group1, scopeSet, nil, transcript))
	assert.Equal(t, 1, transcript.GroupsPassed)
	assert.Equal(t, 0, transcript.GroupsFailed)
}

func TestRelationRequirement_Fulfilled_NotFulfilledOnScopedBagWithTheRelation(t *testing.T) {
	group1 := placement.NewGroup("group1")
	group1.Labels, group1.Relations = hostWithZFSVolume()
	group2 := placement.NewGroup("group2")
	group2.Labels, group2.Relations = hostWithoutIssue()
	groups := []*placement.Group{group1, group2}
	scopeSet := placement.NewScopeSet(groups)

	requirement := NewRelationRequirement(
		labels.NewLabel("rack", "*"),
		labels.NewLabel("redis", "instance", "store1"),
		LessThanEqual,
		0,
	)
	transcript := placement.NewTranscript("transcript")
	assert.False(t, requirement.Passed(group1, scopeSet, nil, transcript))
	assert.Equal(t, 0, transcript.GroupsPassed)
	assert.Equal(t, 1, transcript.GroupsFailed)
}

func TestRelationRequirement_Fulfilled_FulfilledOnBagWithoutTheRelation(t *testing.T) {
	group := placement.NewGroup("group")
	group.Labels, group.Relations = hostWithoutIssue()
	scopeSet := placement.NewScopeSet(nil)

	requirement := NewRelationRequirement(
		nil,
		labels.NewLabel("redis", "instance", "store2"),
		LessThanEqual,
		0,
	)
	transcript := placement.NewTranscript("transcript")
	assert.True(t, requirement.Passed(group, scopeSet, nil, transcript))
	assert.Equal(t, 1, transcript.GroupsPassed)
	assert.Equal(t, 0, transcript.GroupsFailed)
}

func TestRelationRequirement_Fulfilled_NotFulfilledOnBagWithTheRelation(t *testing.T) {
	group := placement.NewGroup("group")
	group.Labels, group.Relations = hostWithoutIssue()
	scopeSet := placement.NewScopeSet(nil)

	requirement := NewRelationRequirement(
		nil,
		labels.NewLabel("redis", "instance", "store1"),
		LessThanEqual,
		0,
	)
	transcript := placement.NewTranscript("transcript")
	assert.False(t, requirement.Passed(group, scopeSet, nil, transcript))
	assert.Equal(t, 0, transcript.GroupsPassed)
	assert.Equal(t, 1, transcript.GroupsFailed)
}

func TestRelationRequirement_Fulfilled_IsUnfulfilledForInvalidComparison(t *testing.T) {
	group := placement.NewGroup("group")
	group.Labels, group.Relations = hostWithoutIssue()
	scopeSet := placement.NewScopeSet(nil)

	requirement := NewRelationRequirement(
		nil,
		labels.NewLabel("redis", "instance", "store1"),
		Comparison("invalid"),
		0,
	)
	assert.False(t, requirement.Passed(group, scopeSet, nil, nil))
}
