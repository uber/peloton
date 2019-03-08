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
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/labels"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
)

// Relation will create an ordering which will order groups based on the number of their relations matching
// the given pattern.
func Relation(scope, pattern *labels.Label) placement.Ordering {
	return &RelationCustom{
		Scope:   scope,
		Pattern: pattern,
	}
}

// RelationCustom can create a tuple of one float which is the number of occurrences of relations that match the pattern
// in the given scope.
type RelationCustom struct {
	Scope   *labels.Label
	Pattern *labels.Label
}

// Tuple returns a tuple of floats created from the group, scope groups and the entity.
func (custom *RelationCustom) Tuple(group *placement.Group, scopeSet *placement.ScopeSet, entity *placement.Entity) []float64 {
	occurrences := scopeSet.RelationScope(group, custom.Scope).Count(custom.Pattern)
	return []float64{float64(occurrences)}
}
