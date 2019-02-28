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

package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSet_Add(t *testing.T) {
	set := NewSet()

	set.Add(CPUUsed, 1.0*GiBit)
	assert.Equal(t, 1.0*GiBit, set.Get(CPUUsed))

	set.Add(CPUUsed, 1.0*GiBit)
	assert.Equal(t, 2.0*GiBit, set.Get(CPUUsed))
}

func TestSet_Types(t *testing.T) {
	set := NewSet()
	set.Add(MemoryFree, 16*GiB)
	set.Add(CPUFree, 100.0)

	types := set.Types()
	assert.Equal(t, 2, len(types))
	assert.Equal(t, CPUFree, types[0])
	assert.Equal(t, MemoryFree, types[1])
}

func TestSet_Set(t *testing.T) {
	set := NewSet()

	set.Set(DiskFree, 1.0*TiB)
	assert.Equal(t, 1.0*TiB, set.Get(DiskFree))

	set.Set(DiskFree, 1.0*TiB)
	assert.Equal(t, 1.0*TiB, set.Get(DiskFree))
}

func TestSet_AddAll(t *testing.T) {
	set1 := NewSet()
	set1.Add(CPUFree, 100.0)
	set1.Add(MemoryFree, 16*GiB)
	set2 := NewSet()
	set2.Add(CPUFree, 200.0)

	set1.AddAll(set2)

	assert.Equal(t, 300.0, set1.Get(CPUFree))
	assert.Equal(t, 16*GiB, set1.Get(MemoryFree))
}

func TestSet_SetAll(t *testing.T) {
	set1 := NewSet()
	set1.Add(CPUFree, 100.0)
	set1.Add(MemoryFree, 16*GiB)
	set2 := NewSet()
	set2.Add(CPUFree, 200.0)

	set1.SetAll(set2)

	assert.Equal(t, 200.0, set1.Get(CPUFree))
	assert.Equal(t, 16*GiB, set1.Get(MemoryFree))
}

func TestSet_ClearAndSize(t *testing.T) {
	set := NewSet()
	assert.Equal(t, 0, set.Size())
	set.Add(NetworkTotal, 1.0*GiBit)
	assert.Equal(t, 1, set.Size())
	set.Clear(NetworkTotal)
	assert.Equal(t, 0, set.Size())
}

func TestSet_ClearAll(t *testing.T) {
	set := NewSet()

	set.Add(NetworkTotal, 1.0*GiBit)
	set.Add(DiskTotal, 1.0*TiB)

	assert.Equal(t, 2, set.Size())
	assert.Equal(t, 2, len(set.Types()))

	set.ClearAll(true, true)

	assert.Equal(t, 0, set.Size())
	assert.Equal(t, 0, len(set.Types()))
}

func TestSet_ClearAll_non_inherited(t *testing.T) {
	set := NewSet()

	set.Add(NetworkTotal, 1.0*GiBit)
	set.Add(NetworkUsed, 0.5*GiBit)

	assert.Equal(t, 2, set.Size())
	assert.Equal(t, 2, len(set.Types()))

	set.ClearAll(false, true)

	assert.Equal(t, 1, set.Size())
	assert.Equal(t, 1, len(set.Types()))
	assert.Equal(t, NetworkUsed, set.Types()[0])
}

func TestSet_ClearAll_inherited(t *testing.T) {
	set := NewSet()

	set.Add(NetworkTotal, 1.0*GiBit)
	set.Add(NetworkUsed, 0.5*GiBit)

	assert.Equal(t, 2, set.Size())
	assert.Equal(t, 2, len(set.Types()))

	set.ClearAll(true, false)

	assert.Equal(t, 1, set.Size())
	assert.Equal(t, 1, len(set.Types()))
	assert.Equal(t, NetworkTotal, set.Types()[0])
}

func TestSet_Update(t *testing.T) {
	set := NewSet()
	set.Add(CPUTotal, 100.0)
	set.Add(CPUUsed, 25.0)
	set.Add(CPUFree, 0.0)

	set.Update()

	assert.Equal(t, 75.0, set.Get(CPUFree))
}
