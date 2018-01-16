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

package placement

import (
	"code.uber.internal/infra/peloton/mimir-lib/model/labels"
	"code.uber.internal/infra/peloton/mimir-lib/model/metrics"
)

// Group represents a host or other physical entity which can contain entities.
type Group struct {
	Name      string
	Labels    *labels.LabelBag
	Metrics   *metrics.MetricSet
	Relations *labels.LabelBag
	Entities  Entities
}

// NewGroup will create a new group with the given name.
func NewGroup(name string) *Group {
	return &Group{
		Name:      name,
		Labels:    labels.NewLabelBag(),
		Relations: labels.NewLabelBag(),
		Metrics:   metrics.NewMetricSet(),
		Entities:  Entities{},
	}
}

// Update will update the relations and metrics of the group from those of its entities.
func (group *Group) Update() {
	newRelations := labels.NewLabelBag()
	newMetrics := metrics.NewMetricSet()
	for _, entity := range group.Entities {
		newRelations.AddAll(entity.Relations)
		newMetrics.AddAll(entity.Metrics, metrics.Ephemeral())
	}
	group.Relations = newRelations
	group.Metrics.ClearAll(metrics.Ephemeral())
	group.Metrics.SetAll(newMetrics, metrics.Ephemeral())
	group.Metrics.Update()
}
