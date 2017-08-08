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

package generation

import (
	"code.uber.internal/infra/peloton/mimir-lib/model/labels"
	"code.uber.internal/infra/peloton/mimir-lib/model/metrics"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

func TestGroupBuilder_Generate(t *testing.T) {
	random := rand.New(rand.NewSource(42))
	builder, templates := CreateHostGroupsBuilder()
	deriver := metrics.NewDeriver([]metrics.FreeMetricTuple{
		metrics.NewFreeMetricTuple(metrics.MemoryFree, metrics.MemoryUsed, metrics.MemoryTotal),
		metrics.NewFreeMetricTuple(metrics.DiskFree, metrics.DiskUsed, metrics.DiskTotal),
	})
	templates.Bind(Datacenter.Name(), "dc1")
	groups := CreateHostGroups(random, builder, templates, deriver, 2, 8)

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
