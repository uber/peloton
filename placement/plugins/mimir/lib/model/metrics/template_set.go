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

import "sync"

// TemplateSet represents a set of metric templates where the values of all templates can be bound later for all
// metric templates.
type TemplateSet interface {
	// Bind will bind the template with the given metric type to the given value if a template exists for that metric
	// type, else nothing will happen.
	Bind(metricType Type, value float64) TemplateSet

	// Add will add a metric template whose value will be set by this bindings of this set. If a template already exists
	// for the metric type of the template then the old template will be overridden.
	Add(template Template) TemplateSet

	// AddAll adds all metric templates from the given template set to this template set. If a template already exists
	// for a metric type in the new set then the old template will be overridden.
	AddAll(set TemplateSet) TemplateSet

	// Templates returns all templates of the template set.
	Templates() map[Type]Template

	// Mappings will return a map of all the current metric types and their values.
	Mappings() map[Type]float64
}

// NewTemplateSet will create a new template set where templates can be added and metric types can be bound for all the
// templates.
func NewTemplateSet() TemplateSet {
	return &templateSet{
		templates: map[Type]Template{},
	}
}

type templateSet struct {
	templates map[Type]Template
	lock      sync.Mutex
}

func (set *templateSet) Bind(metricType Type, value float64) TemplateSet {
	defer set.lock.Unlock()
	set.lock.Lock()

	if template, exists := set.templates[metricType]; exists {
		template.Bind(value)

	}
	return set
}

func (set *templateSet) Add(template Template) TemplateSet {
	defer set.lock.Unlock()
	set.lock.Lock()

	metricType, _ := template.Mapping()
	set.templates[metricType] = template
	return set
}

func (set *templateSet) AddAll(other TemplateSet) TemplateSet {
	defer set.lock.Unlock()
	set.lock.Lock()

	for _, template := range other.Templates() {
		metricType, _ := template.Mapping()
		set.templates[metricType] = template
	}
	return set
}

func (set *templateSet) Templates() map[Type]Template {
	defer set.lock.Unlock()
	set.lock.Lock()

	templates := make(map[Type]Template, len(set.templates))
	for metricType, template := range set.templates {
		templates[metricType] = template
	}
	return templates
}

func (set *templateSet) Mappings() map[Type]float64 {
	defer set.lock.Unlock()
	set.lock.Lock()

	mappings := map[Type]float64{}
	for _, template := range set.templates {
		metricType, value := template.Mapping()
		mappings[metricType] = value
	}
	return mappings
}
