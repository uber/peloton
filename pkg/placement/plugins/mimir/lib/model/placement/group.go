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
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/labels"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/metrics"
)

// Group represents a host or other physical entity which can contain entities.
type Group struct {
	Name      string
	Labels    *labels.Bag
	Metrics   *metrics.Set
	Relations *labels.Bag
	Entities  Entities
}

// NewGroup will create a new group with the given name.
func NewGroup(name string) *Group {
	return &Group{
		Name:      name,
		Labels:    labels.NewBag(),
		Relations: labels.NewBag(),
		Metrics:   metrics.NewSet(),
		Entities:  Entities{},
	}
}

// Update will update the relations and metrics of the group from those of its entities.
func (group *Group) Update() {
	newRelations := labels.NewBag()
	newMetrics := metrics.NewSet()
	for _, entity := range group.Entities {
		newRelations.AddAll(entity.Relations)
		newMetrics.AddAll(entity.Metrics)
	}
	group.Relations = newRelations
	group.Metrics.ClearAll(true, false)
	group.Metrics.SetAll(newMetrics)
	group.Metrics.Update()
}
