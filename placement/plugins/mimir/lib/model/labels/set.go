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

package labels

import "sync"

// TemplateSet represents a set of label templates where the variables of all templates can be bound for all label
// templates.
type TemplateSet interface {
	// Bind will bind the variable with the given name to the given value.
	Bind(name, value string) TemplateSet

	// Add will add a label template whose variables will be set by this variable set.
	Add(template Template) TemplateSet

	// AddAll adds all label templates from the given template set to this template set.
	AddAll(set TemplateSet) TemplateSet

	// Templates returns all templates of the template set.
	Templates() []Template

	// Mappings will return a map of all the current variables and their values.
	Mappings() map[string]string
}

// NewTemplateSet will create a new template set where templates can be added and variables can be bound for all
// templates at the same time.
func NewTemplateSet() TemplateSet {
	return &templateSet{
		templates: []Template{},
	}
}

type templateSet struct {
	templates []Template
	lock      sync.Mutex
}

func (set *templateSet) Bind(name, value string) TemplateSet {
	defer set.lock.Unlock()
	set.lock.Lock()

	for _, template := range set.templates {
		template.Bind(name, value)
	}
	return set
}

func (set *templateSet) Add(template Template) TemplateSet {
	defer set.lock.Unlock()
	set.lock.Lock()

	set.templates = append(set.templates, template)
	return set
}

func (set *templateSet) AddAll(other TemplateSet) TemplateSet {
	defer set.lock.Unlock()
	set.lock.Lock()

	for _, template := range other.Templates() {
		set.templates = append(set.templates, template)
	}
	return set
}

func (set *templateSet) Templates() []Template {
	defer set.lock.Unlock()
	set.lock.Lock()

	templates := make([]Template, 0, len(set.templates))
	for _, template := range set.templates {
		templates = append(templates, template)
	}
	return templates
}

func (set *templateSet) Mappings() map[string]string {
	defer set.lock.Unlock()
	set.lock.Lock()

	mappings := map[string]string{}
	for _, template := range set.templates {
		for variable, value := range template.Mappings() {
			mappings[variable] = value
		}
	}
	return mappings
}
