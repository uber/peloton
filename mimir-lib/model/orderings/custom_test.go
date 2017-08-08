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

package orderings

import (
	"code.uber.internal/infra/peloton/mimir-lib/model/placement"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

type testCustom struct{}

func (custom *testCustom) Tuple(group *placement.Group, entity *placement.Entity) []float64 {
	f, _ := strconv.ParseFloat(group.Name, 64)
	return []float64{f}
}

func (custom *testCustom) Clone() Custom {
	return custom
}

func TestCustomOrdering_Less(t *testing.T) {
	group1 := placement.NewGroup("1")
	group2 := placement.NewGroup("2")
	entity := placement.NewEntity("entity")

	ordering := NewCustomOrdering(&testCustom{})

	assert.True(t, ordering.Less(group1, group2, entity))
	assert.False(t, ordering.Less(group2, group1, entity))
}
