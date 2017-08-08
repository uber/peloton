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

package placement

import (
	"code.uber.internal/infra/peloton/mimir-lib/model/labels"
	"code.uber.internal/infra/peloton/mimir-lib/model/metrics"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGroup_Update(t *testing.T) {
	deriver := metrics.NewDeriver([]metrics.FreeMetricTuple{
		metrics.NewFreeMetricTuple(metrics.MemoryFree, metrics.MemoryUsed, metrics.MemoryTotal),
	})
	group := NewGroup("some-group")
	group.Metrics.Add(metrics.MemoryTotal, 128*metrics.GiB)

	entity1 := NewEntity("entity1")
	entity1.Relations.Add(labels.NewLabel("redis", "instance", "store1"))
	entity1.Metrics.Add(metrics.MemoryUsed, 16*metrics.GiB)

	entity2 := NewEntity("entity2")
	entity1.Relations.Add(labels.NewLabel("redis", "instance", "store2"))
	entity2.Metrics.Add(metrics.MemoryUsed, 8*metrics.GiB)

	group.Entities.Add(entity1)
	group.Entities.Add(entity2)
	group.Update(deriver)

	assert.Equal(t, 24*metrics.GiB, group.Metrics.Get(metrics.MemoryUsed))
	assert.Equal(t, 104*metrics.GiB, group.Metrics.Get(metrics.MemoryFree))

	group.Entities.Remove(entity2)
	group.Update(deriver)

	assert.Equal(t, 16*metrics.GiB, group.Metrics.Get(metrics.MemoryUsed))
	assert.Equal(t, 112*metrics.GiB, group.Metrics.Get(metrics.MemoryFree))
}
