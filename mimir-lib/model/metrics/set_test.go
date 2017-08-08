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

package metrics

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMetricSet_Add(t *testing.T) {
	set := NewMetricSet()

	set.Add(CPUUsed, 1.0*GiBit)
	assert.Equal(t, 1.0*GiBit, set.Get(CPUUsed))

	set.Add(CPUUsed, 1.0*GiBit)
	assert.Equal(t, 2.0*GiBit, set.Get(CPUUsed))
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
