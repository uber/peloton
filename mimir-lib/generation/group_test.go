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

package generation

import (
	"testing"

	"code.uber.internal/infra/peloton/mimir-lib/model/labels"
	"github.com/stretchr/testify/assert"
)

func TestGroupBuilder_Generate(t *testing.T) {
	random := NewRandom(42)
	builder, templates := CreateHostGroupsBuilder()
	templates.Bind(Datacenter.Name(), "dc1")
	groups := CreateHostGroups(random, builder, templates, 2, 8)

	assert.Equal(t, 8, len(groups))
	rackCounts := map[string]int{}
	datacenterPattern := labels.NewLabel(Datacenter.Name(), "*")
	rackPattern := labels.NewLabel(Rack.Name(), "*")
	for _, group := range groups {
		assert.Equal(t, 1, group.Labels.Count(datacenterPattern))
		for _, label := range group.Labels.Find(rackPattern) {
			rackCounts[label.String()] += group.Labels.Count(label)
		}
	}
	assert.Equal(t, 2, len(rackCounts))
	for _, count := range rackCounts {
		assert.Equal(t, 4, count)
	}
}
