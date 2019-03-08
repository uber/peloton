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
	"time"

	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/generation"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/labels"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/metrics"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
)

// EntityBuilder is used to generate new entities for use in tests and benchmarks.
type EntityBuilder interface {
	// Name will use the Label.String() method value of the generated value from the label builder.
	Name(template labels.Template) EntityBuilder

	// Ordering will set the ordering builder to the given builder.
	Ordering(builder OrderingBuilder) EntityBuilder

	// AddRelation will add a relation generated from the given label builder,
	AddRelation(template labels.Template) EntityBuilder

	// AddMetric will add the metric generated from the given distribution.
	AddMetric(metricType metrics.Type, distribution generation.Distribution) EntityBuilder

	// Requirement will set the requirement builder to the given builder.
	Requirement(builder RequirementBuilder) EntityBuilder

	// Generate will generate an entity that depends on the random source and the time.
	Generate(random generation.Random, time time.Duration) *placement.Entity
}

// NewEntityBuilder will create a new entity builder for generating entities.
func NewEntityBuilder() EntityBuilder {
	return &entityBuilder{
		name:        labels.NewTemplate(),
		relations:   map[labels.Template]struct{}{},
		metrics:     map[metrics.Type]generation.Distribution{},
		requirement: &emptyRequirement{},
		ordering:    &nameOrdering{},
	}
}

type entityBuilder struct {
	name        labels.Template
	relations   map[labels.Template]struct{}
	metrics     map[metrics.Type]generation.Distribution
	requirement RequirementBuilder
	ordering    OrderingBuilder
}

func (builder *entityBuilder) Name(template labels.Template) EntityBuilder {
	builder.name = template
	return builder
}

func (builder *entityBuilder) Ordering(subBuilder OrderingBuilder) EntityBuilder {
	builder.ordering = subBuilder
	return builder
}

func (builder *entityBuilder) AddRelation(template labels.Template) EntityBuilder {
	builder.relations[template] = struct{}{}
	return builder
}

func (builder *entityBuilder) AddMetric(metricType metrics.Type, distribution generation.Distribution) EntityBuilder {
	builder.metrics[metricType] = distribution
	return builder
}

func (builder *entityBuilder) Requirement(subBuilder RequirementBuilder) EntityBuilder {
	builder.requirement = subBuilder
	return builder
}

func (builder *entityBuilder) Generate(random generation.Random, time time.Duration) *placement.Entity {
	result := placement.NewEntity(builder.name.Instantiate().String())
	result.Ordering = builder.ordering.Generate(random, time)
	result.Requirement = builder.requirement.Generate(random, time)
	for relation := range builder.relations {
		result.Relations.Add(relation.Instantiate())
	}
	for metric, distribution := range builder.metrics {
		result.Metrics.Add(metric, distribution.Value(random, time))
	}
	return result
}
