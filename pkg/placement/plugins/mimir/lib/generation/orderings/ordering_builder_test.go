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

package orderings

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/generation"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/internal"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/metrics"
	mOrderings "github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/orderings"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
)

func TestOrderingBuilder_Generate(t *testing.T) {
	ordering := NewOrderingBuilder(Metric(mOrderings.GroupSource, metrics.DiskFree)).
		Generate(generation.NewRandom(42), time.Duration(0))
	group1, group2, groups, entity := internal.SetupTwoGroupsAndEntity()
	scopeSet := placement.NewScopeSet(groups)

	tuple1 := ordering.Tuple(group1, scopeSet, entity)
	tuple2 := ordering.Tuple(group2, scopeSet, entity)

	assert.True(t, placement.Less(tuple1, tuple2))
	assert.False(t, placement.Less(tuple2, tuple1))
}
