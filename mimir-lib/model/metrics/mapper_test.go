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

func TestChanger_Change(t *testing.T) {
	set := NewMetricSet()
	set.Set(MemoryFree, 1.0*GiB)
	set.Set(NetworkFree, 1.0*GiBit)

	changer := NewTypeMapper(map[MetricType]MetricType{
		MemoryFree:  MemoryUsed,
		NetworkFree: NetworkUsed,
	})
	changer.Map(set)

	assert.Equal(t, 0.0, set.Get(MemoryFree))
	assert.Equal(t, 1.0*GiB, set.Get(MemoryUsed))
	assert.Equal(t, 0.0, set.Get(NetworkFree))
	assert.Equal(t, 1.0*GiBit, set.Get(NetworkUsed))
}
