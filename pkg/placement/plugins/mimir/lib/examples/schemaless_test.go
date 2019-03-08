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

package examples

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/algorithms"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/generation"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/generation/orderings"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/metrics"
	mOrderings "github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/orderings"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
)

func Test(t *testing.T) {
	random := generation.NewRandom(42)
	entityBuilder, entityTemplates := CreateSchemalessEntityBuilder()
	entityBuilder.Ordering(orderings.NewOrderingBuilder(orderings.Negate(orderings.Metric(mOrderings.GroupSource, metrics.DiskFree))))

	entityTemplates.
		Bind(Instance.Name(), "store1").
		Bind(Datacenter.Name(), "dc1")
	entities := CreateSchemalessEntities(
		random, entityBuilder, entityTemplates, 4, 4)

	entityTemplates.
		Bind(Instance.Name(), "store2").
		Bind(Datacenter.Name(), "dc1")
	entities = append(entities, CreateSchemalessEntities(random, entityBuilder, entityTemplates, 4, 4)...)

	groupBuilder, groupTemplates := CreateHostGroupsBuilder()
	groupTemplates.Bind(Datacenter.Name(), "dc1")
	groups := CreateHostGroups(random, groupBuilder, groupTemplates, 4, 16)
	placer := algorithms.NewPlacer(1, 1)

	var assignments []*placement.Assignment
	for _, entity := range entities {
		assignments = append(assignments, placement.NewAssignment(entity))
	}
	scopeSet := placement.NewScopeSet(groups)
	placer.Place(assignments, groups, scopeSet)
	for _, assignment := range assignments {
		assert.False(t, assignment.Failed)
	}
}
