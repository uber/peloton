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

package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadCombine_ors_together_all_flags(t *testing.T) {
	assert.Equal(t, all, readCombine())
	assert.Equal(t, ephemeral, readCombine(Ephemeral()))
	assert.Equal(t, nonEphemeral, readCombine(NonEphemeral()))
	assert.Equal(t, all, readCombine(Ephemeral(), NonEphemeral()))
	assert.Equal(t, all, readCombine(Ephemeral(), NonEphemeral(), All()))
}

func TestWriteCombine_finds_the_top_flag(t *testing.T) {
	assert.Equal(t, nonEphemeral, writeCombine())
	assert.Equal(t, ephemeral, writeCombine(Ephemeral()))
	assert.Equal(t, nonEphemeral, writeCombine(NonEphemeral()))
	assert.Equal(t, nonEphemeral, writeCombine(Ephemeral(), NonEphemeral()))
	assert.Equal(t, nonEphemeral, writeCombine(Ephemeral(), NonEphemeral(), All()))
}

func TestMetricSet_Add(t *testing.T) {
	set := NewMetricSet()

	set.Add(CPUUsed, 1.0*GiBit)
	assert.Equal(t, 1.0*GiBit, set.Get(CPUUsed))

	set.Add(CPUUsed, 1.0*GiBit)
	assert.Equal(t, 2.0*GiBit, set.Get(CPUUsed))
}

func TestMetricSet_Types(t *testing.T) {
	set := NewMetricSet()
	set.Add(CPUFree, 100.0)
	set.Add(MemoryFree, 16*GiB)

	types := set.Types()
	assert.Equal(t, 2, len(types))
	for _, metricType := range types {
		switch metricType {
		case CPUFree:
		case MemoryFree:
		default:
			assert.True(t, false)
		}
	}
}

func TestMetricSet_Set(t *testing.T) {
	set := NewMetricSet()

	set.Set(DiskFree, 1.0*TiB)
	assert.Equal(t, 1.0*TiB, set.Get(DiskFree))

	set.Set(DiskFree, 1.0*TiB)
	assert.Equal(t, 1.0*TiB, set.Get(DiskFree))
}

func TestMetricSet_AddAll(t *testing.T) {
	set1 := NewMetricSet()
	set1.Add(CPUFree, 100.0)
	set1.Add(MemoryFree, 16*GiB)
	set2 := NewMetricSet()
	set2.Add(CPUFree, 200.0)

	set1.AddAll(set2)

	assert.Equal(t, 300.0, set1.Get(CPUFree))
	assert.Equal(t, 16*GiB, set1.Get(MemoryFree))
}

func TestMetricSet_SetAll(t *testing.T) {
	set1 := NewMetricSet()
	set1.Add(CPUFree, 100.0)
	set1.Add(MemoryFree, 16*GiB)
	set2 := NewMetricSet()
	set2.Add(CPUFree, 200.0)

	set1.SetAll(set2)

	assert.Equal(t, 200.0, set1.Get(CPUFree))
	assert.Equal(t, 16*GiB, set1.Get(MemoryFree))
}

func TestMetricSet_ClearAndSize(t *testing.T) {
	set := NewMetricSet()
	assert.Equal(t, 0, set.Size())
	set.Add(NetworkTotal, 1.0*GiBit)
	assert.Equal(t, 1, set.Size())
	set.Clear(NetworkTotal)
	assert.Equal(t, 0, set.Size())
}

func TestMetricSet_ClearAll(t *testing.T) {
	set := NewMetricSet()

	set.Add(NetworkTotal, 1.0*GiBit, Ephemeral())
	set.Add(DiskTotal, 1.0*TiB, NonEphemeral())

	assert.Equal(t, 2, set.Size())
	assert.Equal(t, 2, len(set.Types()))

	set.ClearAll(All())

	assert.Equal(t, 0, set.Size())
	assert.Equal(t, 0, len(set.Types()))
}

func TestMetricSet_ClearAll_non_ephemeral(t *testing.T) {
	set := NewMetricSet()

	set.Add(NetworkTotal, 1.0*GiBit, Ephemeral())
	set.Add(DiskTotal, 1.0*TiB)

	assert.Equal(t, 2, set.Size())
	assert.Equal(t, 2, len(set.Types()))

	set.ClearAll(NonEphemeral())

	assert.Equal(t, 1, set.Size())
	assert.Equal(t, 1, len(set.Types()))
	assert.Equal(t, NetworkTotal, set.Types()[0])
}

func TestMetricSet_ClearAll_ephemeral(t *testing.T) {
	set := NewMetricSet()

	set.Add(NetworkTotal, 1.0*GiBit, Ephemeral())
	set.Add(DiskTotal, 1.0*TiB)

	assert.Equal(t, 2, set.Size())
	assert.Equal(t, 2, len(set.Types()))

	set.ClearAll(Ephemeral())

	assert.Equal(t, 1, set.Size())
	assert.Equal(t, 1, len(set.Types()))
	assert.Equal(t, DiskTotal, set.Types()[0])
}

func TestMetricSet_Update(t *testing.T) {
	set := NewMetricSet()
	set.Add(CPUTotal, 100.0)
	set.Add(CPUUsed, 25.0)
	set.Add(CPUFree, 0.0)

	set.Update()

	assert.Equal(t, 75.0, set.Get(CPUFree))
}
