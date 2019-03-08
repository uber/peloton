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

package placement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/labels"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/metrics"
)

func TestGroup_Update_remove_entity_will_decrease_load_on_group(t *testing.T) {
	group := NewGroup("some-group")
	group.Metrics.Add(metrics.MemoryTotal, 128*metrics.GiB)
	group.Metrics.Set(metrics.MemoryFree, 0.0)

	entity1 := NewEntity("entity1")
	label1 := labels.NewLabel("redis", "instance", "store1")
	entity1.Relations.Add(label1)
	entity1.Metrics.Add(metrics.MemoryUsed, 16*metrics.GiB)

	entity2 := NewEntity("entity2")
	label2 := labels.NewLabel("redis", "instance", "store2")
	entity2.Relations.Add(label2)
	entity2.Metrics.Add(metrics.MemoryUsed, 8*metrics.GiB)

	group.Entities.Add(entity1)
	group.Entities.Add(entity2)
	group.Update()

	assert.Equal(t, 24*metrics.GiB, group.Metrics.Get(metrics.MemoryUsed))
	assert.Equal(t, 104*metrics.GiB, group.Metrics.Get(metrics.MemoryFree))
	assert.Equal(t, 1, group.Relations.Count(label1))
	assert.Equal(t, 1, group.Relations.Count(label2))

	group.Entities.Remove(entity2)
	group.Update()

	assert.Equal(t, 16*metrics.GiB, group.Metrics.Get(metrics.MemoryUsed))
	assert.Equal(t, 112*metrics.GiB, group.Metrics.Get(metrics.MemoryFree))
	assert.Equal(t, 1, group.Relations.Count(label1))
	assert.Equal(t, 0, group.Relations.Count(label2))
}

func TestGroup_Update_remove_all_entities_will_give_group_zero_load(t *testing.T) {
	group := NewGroup("some-group")
	group.Metrics.Set(metrics.MemoryTotal, 128*metrics.GiB)
	group.Metrics.Set(metrics.MemoryFree, 0.0)

	entity := NewEntity("entity")
	label := labels.NewLabel("redis", "instance", "store1")
	entity.Relations.Add(label)
	entity.Metrics.Add(metrics.MemoryUsed, 16*metrics.GiB)

	group.Entities.Add(entity)
	group.Update()

	assert.Equal(t, 16*metrics.GiB, group.Metrics.Get(metrics.MemoryUsed))
	assert.Equal(t, 112*metrics.GiB, group.Metrics.Get(metrics.MemoryFree))
	assert.Equal(t, 1, group.Relations.Count(label))

	group.Entities.Remove(entity)
	group.Update()

	assert.Equal(t, 0*metrics.GiB, group.Metrics.Get(metrics.MemoryUsed))
	assert.Equal(t, 128*metrics.GiB, group.Metrics.Get(metrics.MemoryFree))
	assert.Equal(t, 0, group.Relations.Count(label))
}
