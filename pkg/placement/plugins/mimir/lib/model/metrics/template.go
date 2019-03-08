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

package metrics

import (
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/generation"
)

// Template represents a metric template which can be instantiated with different values. A template can create
// a metric type and a value for it. The value of the metric type can bound later.
type Template interface {
	// Bind will bind the template with to the given value.
	Bind(value float64) Template

	// Mapping will return a the metric type and its current value.
	Mapping() (Type, float64)

	// Instantiate will create a new metric type with a given distribution.
	Instantiate() (Type, generation.Distribution)
}

// NewTemplate will create a new template for a given metric type, the value of the metric can be bound later.
func NewTemplate(metricType Type) Template {
	return &metricTemplate{
		metricType: metricType,
		value:      generation.NewConstant(0.0),
	}
}

type metricTemplate struct {
	metricType Type
	value      *generation.Constant
}

func (template *metricTemplate) Bind(value float64) Template {
	template.value.NewValue(value)
	return template
}

func (template *metricTemplate) Mapping() (Type, float64) {
	return template.metricType, template.value.CurrentValue()
}

func (template *metricTemplate) Instantiate() (Type, generation.Distribution) {
	return template.metricType, template.value
}
