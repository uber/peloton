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

package placement_test

import (
	"testing"

	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/labels"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/orderings"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/requirements"

	"github.com/stretchr/testify/assert"
)

func hostWithoutIssue() *placement.Group {
	group := placement.NewGroup("host-without-issue")
	labelBag := labels.NewBag()
	labelBag.Add(labels.NewLabel("datacenter", "dc1"))
	labelBag.Add(labels.NewLabel("rack", "dc1-a007"))
	labelBag.Add(labels.NewLabel("host", "host42-dc1"))

	relationBag := labels.NewBag()
	relationBag.Add(labels.NewLabel("redis", "instance", "store1"))

	group.Labels = labelBag
	group.Relations = relationBag
	return group
}

func hostWithIssue() *placement.Group {
	group := placement.NewGroup("host-with-issue")
	labelBag := labels.NewBag()
	labelBag.Add(labels.NewLabel("datacenter", "dc1"))
	labelBag.Add(labels.NewLabel("rack", "dc1-a007"))
	labelBag.Add(labels.NewLabel("host", "host43-dc1"))
	labelBag.Add(labels.NewLabel("issues", "someissue"))

	relationBag := labels.NewBag()

	group.Labels = labelBag
	group.Relations = relationBag
	return group
}

func TestScopeSet_ScopeGroups_returns_a_copy_of_the_scope_groups_list(t *testing.T) {
	group1 := hostWithoutIssue()
	group2 := hostWithIssue()
	groups := []*placement.Group{group1, group2}
	scopeSet := placement.NewScopeSet(groups)

	scopeGroups := scopeSet.ScopeGroups()
	assert.Equal(t, len(groups), len(scopeGroups))
	scopeGroups[0] = nil
	assert.NotNil(t, groups[0])
}

func TestScopeSet_CompleteScope_for_requirements(t *testing.T) {
	group1 := hostWithoutIssue()
	group2 := hostWithIssue()
	groups := []*placement.Group{group1, group2}
	entity := placement.NewEntity("entity")

	scope := labels.NewLabel("rack", "dc1-a007")
	entity.Requirement = requirements.NewLabelRequirement(
		scope, labels.NewLabel("issues", "*"), requirements.LessThanEqual, 0)
	scopeSet := placement.NewScopeSet(groups)
	entity.Requirement.Passed(group1, scopeSet, entity, nil)

	scopeGroups := scopeSet.CompleteScope()

	assert.Equal(t, 2, len(scopeGroups))
	assert.NotNil(t, scopeGroups["host-with-issue"])
	assert.NotNil(t, scopeGroups["host-without-issue"])
}

func TestScopeSet_CompleteScope_for_orderings(t *testing.T) {
	group1 := hostWithoutIssue()
	group2 := hostWithIssue()
	groups := []*placement.Group{group1, group2}
	entity := placement.NewEntity("entity")

	scope := labels.NewLabel("rack", "dc1-a007")
	entity.Ordering = orderings.Label(scope, labels.NewLabel("issues", "*"))
	scopeSet := placement.NewScopeSet(groups)
	entity.Ordering.Tuple(group1, scopeSet, entity)

	scopeGroups := scopeSet.CompleteScope()

	assert.Equal(t, 2, len(scopeGroups))
	assert.NotNil(t, scopeGroups["host-with-issue"])
	assert.NotNil(t, scopeGroups["host-without-issue"])
}

func TestScopeSet_LabelScope_LabelsWithScope(t *testing.T) {
	group1 := hostWithoutIssue()
	group2 := hostWithIssue()
	scopeGroups := []*placement.Group{group1, group2}

	scope := labels.NewLabel("*", "*")
	scopeSet := placement.NewScopeSet(scopeGroups)
	scopeLabels := scopeSet.LabelScope(group1, scope)
	assert.Equal(t, 5, scopeLabels.Size())
	assert.Equal(t, 2, scopeLabels.Count(labels.NewLabel("datacenter", "dc1")))
	assert.Equal(t, 2, scopeLabels.Count(labels.NewLabel("rack", "dc1-a007")))
	assert.Equal(t, 1, scopeLabels.Count(labels.NewLabel("host", "host42-dc1")))
	assert.Equal(t, 1, scopeLabels.Count(labels.NewLabel("host", "host43-dc1")))
	assert.Equal(t, 1, scopeLabels.Count(labels.NewLabel("issues", "someissue")))
}

func TestScopeSet_LabelScope_CachesResult(t *testing.T) {
	group1 := hostWithoutIssue()
	group2 := hostWithIssue()
	scopeGroups := []*placement.Group{group1, group2}

	scope := labels.NewLabel("*", "*")
	scopeSet := placement.NewScopeSet(scopeGroups)
	scopeLabels1 := scopeSet.LabelScope(group1, scope)
	scopeLabels2 := scopeSet.LabelScope(group1, scope)

	assert.True(t, scopeLabels1 == scopeLabels2)
}

func TestScopeSet_LabelScope_LabelsWithNoScope(t *testing.T) {
	group1 := hostWithoutIssue()
	group2 := hostWithIssue()
	scopeGroups := []*placement.Group{group1, group2}

	scopeSet := placement.NewScopeSet(scopeGroups)
	scopeLabels := scopeSet.LabelScope(group1, nil)
	assert.Equal(t, 3, scopeLabels.Size())
	assert.Equal(t, 1, scopeLabels.Count(labels.NewLabel("datacenter", "dc1")))
	assert.Equal(t, 1, scopeLabels.Count(labels.NewLabel("rack", "dc1-a007")))
	assert.Equal(t, 1, scopeLabels.Count(labels.NewLabel("host", "host42-dc1")))
}

func TestScopeSet_RelationScope_RelationsWithScope(t *testing.T) {
	group1 := hostWithoutIssue()
	group2 := hostWithoutIssue()
	scopeGroups := []*placement.Group{group1, group2}

	scope := labels.NewLabel("*", "*")
	scopeSet := placement.NewScopeSet(scopeGroups)
	scopeRelations := scopeSet.RelationScope(group1, scope)
	assert.Equal(t, 1, scopeRelations.Size())
	assert.Equal(t, 2, scopeRelations.Count(labels.NewLabel("redis", "instance", "store1")))
}

func TestScopeSet_RelationScope_CachesResult(t *testing.T) {
	group1 := hostWithoutIssue()
	group2 := hostWithoutIssue()
	scopeGroups := []*placement.Group{group1, group2}

	scope := labels.NewLabel("*", "*")
	scopeSet := placement.NewScopeSet(scopeGroups)
	scopeRelations1 := scopeSet.RelationScope(group1, scope)
	scopeRelations2 := scopeSet.RelationScope(group1, scope)

	assert.True(t, scopeRelations1 == scopeRelations2)
}

func TestScopeSet_RelationScope_RelationsWithNoScope(t *testing.T) {
	group1 := hostWithoutIssue()
	group2 := hostWithoutIssue()
	scopeGroups := []*placement.Group{group1, group2}

	scopeSet := placement.NewScopeSet(scopeGroups)
	scopeRelations := scopeSet.RelationScope(group1, nil)
	assert.Equal(t, 1, scopeRelations.Size())
	assert.Equal(t, 1, scopeRelations.Count(labels.NewLabel("redis", "instance", "store1")))
}

func TestScopeSet_Copy(t *testing.T) {
	group1 := hostWithoutIssue()
	group2 := hostWithIssue()
	scope := labels.NewLabel("*", "*")
	scopeGroups := []*placement.Group{group1, group2}
	scopeSet1 := placement.NewScopeSet(scopeGroups)

	scopeLabels1 := scopeSet1.LabelScope(group1, scope)
	scopeRelations1 := scopeSet1.RelationScope(group1, scope)

	scopeSet2 := scopeSet1.Copy()

	scopeLabels2 := scopeSet2.LabelScope(group1, scope)
	scopeRelations2 := scopeSet2.RelationScope(group1, scope)

	assert.True(t, scopeLabels1 == scopeLabels2)
	assert.True(t, scopeRelations1 == scopeRelations2)
	assert.False(t, scopeSet1 == scopeSet2)
}
