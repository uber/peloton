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

package model_test

import (
	"testing"

	"code.uber.internal/infra/peloton/mimir-lib/model"
	"code.uber.internal/infra/peloton/mimir-lib/model/labels"
	"code.uber.internal/infra/peloton/mimir-lib/model/placement"
	"github.com/stretchr/testify/assert"
)

func hostWithoutIssue() *placement.Group {
	group := placement.NewGroup("host-without-issue")
	labelBag := labels.NewLabelBag()
	labelBag.Add(labels.NewLabel("datacenter", "dc1"))
	labelBag.Add(labels.NewLabel("rack", "dc1-a007"))
	labelBag.Add(labels.NewLabel("host", "host42-dc1"))

	relationBag := labels.NewLabelBag()
	relationBag.Add(labels.NewLabel("redis", "instance", "store1"))

	group.Labels = labelBag
	group.Relations = relationBag
	return group
}

func hostWithIssue() *placement.Group {
	group := placement.NewGroup("host-with-issue")
	labelBag := labels.NewLabelBag()
	labelBag.Add(labels.NewLabel("datacenter", "dc1"))
	labelBag.Add(labels.NewLabel("rack", "dc1-a007"))
	labelBag.Add(labels.NewLabel("host", "host43-dc1"))
	labelBag.Add(labels.NewLabel("issues", "someissue"))

	relationBag := labels.NewLabelBag()

	group.Labels = labelBag
	group.Relations = relationBag
	return group
}

func TestCopyScope_LabelsWithScope(t *testing.T) {
	group1 := hostWithoutIssue()
	group2 := hostWithIssue()
	scopeGroups := []*placement.Group{group1, group2}

	scope := labels.NewLabel("*", "*")
	scopeLabels := model.CopyScope(scope, false, group1, scopeGroups)
	assert.Equal(t, 5, scopeLabels.Size())
	assert.Equal(t, 2, scopeLabels.Count(labels.NewLabel("datacenter", "dc1")))
	assert.Equal(t, 2, scopeLabels.Count(labels.NewLabel("rack", "dc1-a007")))
	assert.Equal(t, 1, scopeLabels.Count(labels.NewLabel("host", "host42-dc1")))
	assert.Equal(t, 1, scopeLabels.Count(labels.NewLabel("host", "host43-dc1")))
	assert.Equal(t, 1, scopeLabels.Count(labels.NewLabel("issues", "someissue")))
}

func TestCopyScope_LabelsWithNoScope(t *testing.T) {
	group1 := hostWithoutIssue()
	group2 := hostWithIssue()
	scopeGroups := []*placement.Group{group1, group2}

	scopeLabels := model.CopyScope(nil, false, group1, scopeGroups)
	assert.Equal(t, 3, scopeLabels.Size())
	assert.Equal(t, 1, scopeLabels.Count(labels.NewLabel("datacenter", "dc1")))
	assert.Equal(t, 1, scopeLabels.Count(labels.NewLabel("rack", "dc1-a007")))
	assert.Equal(t, 1, scopeLabels.Count(labels.NewLabel("host", "host42-dc1")))
}

func TestCopyScope_RelationsWithScope(t *testing.T) {
	group1 := hostWithoutIssue()
	group2 := hostWithoutIssue()
	scopeGroups := []*placement.Group{group1, group2}

	scope := labels.NewLabel("*", "*")
	scopeRelations := model.CopyScope(scope, true, group1, scopeGroups)
	assert.Equal(t, 1, scopeRelations.Size())
	assert.Equal(t, 2, scopeRelations.Count(labels.NewLabel("redis", "instance", "store1")))
}

func TestCopyScope_RelationsWithNoScope(t *testing.T) {
	group1 := hostWithoutIssue()
	group2 := hostWithoutIssue()
	scopeGroups := []*placement.Group{group1, group2}

	scopeRelations := model.CopyScope(nil, true, group1, scopeGroups)
	assert.Equal(t, 1, scopeRelations.Size())
	assert.Equal(t, 1, scopeRelations.Count(labels.NewLabel("redis", "instance", "store1")))
}
