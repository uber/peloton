// @generated AUTO GENERATED - DO NOT EDIT!
// Copyright (c) 2017 Uber Technologies, Inc.
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

// TemplateSet represents a set of label templates where the variables of all templates can be bound for all label
// templates.
type TemplateSet interface {
	// Bind will bind the variable with the given name to the given value.
	Bind(name, value string) TemplateSet

	// Add will add a label template whose variables will be set by this variable set.
	Add(template LabelTemplate) TemplateSet

	// Mappings will return a map of all the current variables and their values.
	Mappings() map[string]string
}

// NewTemplateSet will create a new template set where templates can be added and variables can be bound for all
// templates at the same time.
func NewTemplateSet() TemplateSet {
	return &templateSet{
		templates: []LabelTemplate{},
	}
}

type templateSet struct {
	templates []LabelTemplate
}

func (set *templateSet) Bind(name, value string) TemplateSet {
	for _, template := range set.templates {
		template.Bind(name, value)
	}
	return set
}

func (set *templateSet) Add(template LabelTemplate) TemplateSet {
	set.templates = append(set.templates, template)
	return set
}

func (set *templateSet) Mappings() map[string]string {
	mappings := map[string]string{}
	for _, template := range set.templates {
		for variable, value := range template.Mappings() {
			mappings[variable] = value
		}
	}
	return mappings
}
