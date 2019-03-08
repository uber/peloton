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
	"time"

	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/generation"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/labels"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/metrics"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/orderings"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
)

// Metric creates a custom ordering builder which orders groups based on their value of the given metric type.
func Metric(source orderings.Source, metricType metrics.Type) OrderingBuilder {
	return &metricBuilder{
		source:     source,
		metricType: metricType,
	}
}

// Relation creates a custom ordering builder which orders groups based on the number of their relations
// matching the given pattern.
func Relation(scope, pattern labels.Template) OrderingBuilder {
	return &relationBuilder{
		scope:   scope,
		pattern: pattern,
	}
}

// Label creates a custom ordering builder which order groups based on the number of their labels matching
// the given pattern.
func Label(scope, pattern labels.Template) OrderingBuilder {
	return &labelBuilder{
		scope:   scope,
		pattern: pattern,
	}
}

// Constant creates a custom ordering builder that returns a tuple score which will always return a tuple of length one
// with the given constant.
func Constant(constant float64) OrderingBuilder {
	return &constantBuilder{
		constant: constant,
	}
}

// Negate creates a custom ordering builder that negates the given tuple.
func Negate(subBuilder OrderingBuilder) OrderingBuilder {
	return &negateBuilder{
		subBuilder: subBuilder,
	}
}

// Inverse a custom ordering builder that will invert the given tuple.
func Inverse(subBuilder OrderingBuilder) OrderingBuilder {
	return &inverseBuilder{
		subBuilder: subBuilder,
	}
}

// Sum creates a custom ordering builder that takes the tuples of the sub expressions and return a tuple which
// will have the length of the smallest tuple returned from the sub expressions where each entry is the summation of the
// corresponding entry in the tuple from the sub expressions.
func Sum(subBuilders ...OrderingBuilder) OrderingBuilder {
	return &summationBuilder{
		subBuilders: subBuilders,
	}
}

// Multiply creates a custom ordering builder that takes the tuples of the sub expressions and return a tuple which will have the length of the smallest
// tuple returned from the sub expressions where each entry is the multiplication of the corresponding entry in the
// tuple from the sub expressions.
func Multiply(subBuilders ...OrderingBuilder) OrderingBuilder {
	return &multiplyBuilder{
		subBuilders: subBuilders,
	}
}

// Map creates a custom ordering builder that changes the tuple according to which bucket each entry of the tuple falls.
func Map(mapping *orderings.Mapping, subBuilder OrderingBuilder) OrderingBuilder {
	return &mapBuilder{
		mapping:    mapping,
		subBuilder: subBuilder,
	}
}

// Concatenate creates a custom ordering builder that takes a list of orderings and then make a concatenation that will
// behave like a lexicographic ordering.
func Concatenate(subBuilders ...OrderingBuilder) OrderingBuilder {
	return &concatenateBuilder{
		subBuilders: subBuilders,
	}
}

type metricBuilder struct {
	source     orderings.Source
	metricType metrics.Type
}

func (builder *metricBuilder) Generate(random generation.Random, time time.Duration) placement.Ordering {
	return orderings.Metric(builder.source, builder.metricType)
}

type relationBuilder struct {
	scope   labels.Template
	pattern labels.Template
}

func (builder *relationBuilder) Generate(random generation.Random, time time.Duration) placement.Ordering {
	var scope *labels.Label
	if builder.scope != nil {
		scope = builder.scope.Instantiate()
	}
	return orderings.Relation(scope, builder.pattern.Instantiate())
}

type labelBuilder struct {
	scope   labels.Template
	pattern labels.Template
}

func (builder *labelBuilder) Generate(random generation.Random, time time.Duration) placement.Ordering {
	var scope *labels.Label
	if builder.scope != nil {
		scope = builder.scope.Instantiate()
	}
	return orderings.Label(scope, builder.pattern.Instantiate())
}

type constantBuilder struct {
	constant float64
}

func (builder *constantBuilder) Generate(random generation.Random, time time.Duration) placement.Ordering {
	return orderings.Constant(builder.constant)
}

type negateBuilder struct {
	subBuilder OrderingBuilder
}

func (builder *negateBuilder) Generate(random generation.Random, time time.Duration) placement.Ordering {
	return orderings.Negate(builder.subBuilder.Generate(random, time))
}

type inverseBuilder struct {
	subBuilder OrderingBuilder
}

func (builder *inverseBuilder) Generate(random generation.Random, time time.Duration) placement.Ordering {
	return orderings.Inverse(builder.subBuilder.Generate(random, time))
}

type summationBuilder struct {
	subBuilders []OrderingBuilder
}

func (builder *summationBuilder) Generate(random generation.Random, time time.Duration) placement.Ordering {
	subOrderings := make([]placement.Ordering, 0, len(builder.subBuilders))
	for _, subExpression := range builder.subBuilders {
		subOrderings = append(subOrderings, subExpression.Generate(random, time))
	}
	return orderings.Sum(subOrderings...)
}

type multiplyBuilder struct {
	subBuilders []OrderingBuilder
}

func (builder *multiplyBuilder) Generate(random generation.Random, time time.Duration) placement.Ordering {
	subOrderings := make([]placement.Ordering, 0, len(builder.subBuilders))
	for _, subExpression := range builder.subBuilders {
		subOrderings = append(subOrderings, subExpression.Generate(random, time))
	}
	return orderings.Multiply(subOrderings...)
}

type mapBuilder struct {
	mapping    *orderings.Mapping
	subBuilder OrderingBuilder
}

func (builder *mapBuilder) Generate(random generation.Random, time time.Duration) placement.Ordering {
	return orderings.Map(builder.mapping, builder.subBuilder.Generate(random, time))
}

type concatenateBuilder struct {
	subBuilders []OrderingBuilder
}

func (builder *concatenateBuilder) Generate(random generation.Random, time time.Duration) placement.Ordering {
	subOrderings := make([]placement.Ordering, 0, len(builder.subBuilders))
	for _, subExpression := range builder.subBuilders {
		subOrderings = append(subOrderings, subExpression.Generate(random, time))
	}
	return orderings.Concatenate(subOrderings...)
}
