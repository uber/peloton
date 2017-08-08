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

func TestDeriver_Derive(t *testing.T) {
	set := NewMetricSet()
	set.Set(CPUTotal, 1000.0)
	set.Set(CPUUsed, 500.0)
	set.Set(MemoryTotal, 128.0*GiB)
	set.Set(MemoryUsed, 64.0*GiB)
	set.Set(DiskTotal, 4.0*TiB)
	set.Set(DiskUsed, 1.0*TiB)
	set.Set(NetworkTotal, 1.0*GiBit)
	set.Set(NetworkUsed, 200*MiBit)

	deriver := NewDeriver([]FreeMetricTuple{
		NewFreeMetricTuple(CPUFree, CPUUsed, CPUTotal),
		NewFreeMetricTuple(MemoryFree, MemoryUsed, MemoryTotal),
		NewFreeMetricTuple(NetworkFree, NetworkUsed, NetworkTotal),
		NewFreeMetricTuple(DiskFree, DiskUsed, DiskTotal),
	})
	deriver.Derive(set)

	assert.Equal(t, 500.0, set.Get(CPUFree))
	assert.Equal(t, 64*GiB, set.Get(MemoryFree))
	assert.Equal(t, 3*TiB, set.Get(DiskFree))
	assert.Equal(t, 824*MiBit, set.Get(NetworkFree))
}
