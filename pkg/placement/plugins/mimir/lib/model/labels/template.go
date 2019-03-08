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

import (
	"fmt"
	"strings"
	"sync"
)

// Template represents a label template which can be instantiated with different values. A template can create
// labels like schemaless.cluster.percona-cluster-$instance-name$-$zone$-db$cluster$ where the labelTemplate substrings
// <instance-name>, <zone> and <cluster> can then be bound later, and re-bound to instantiate different labels.
type Template interface {
	// Bind will bind the template with the given name to the given value.
	Bind(name, value string) Template

	// Mappings will return a map of all the current variables and their values.
	Mappings() map[string]string

	// Instantiate will create a new label where all templates have been replaced with their currently bound value.
	Instantiate() *Label
}

// NewTemplate will create a new label template which can be used to create labels with. Each name in the slice of
// supplied names can use template substrings like percona-cluster-$instance-name$-$zone$-db$cluster$ and then later
// bind the template names <instance-name>, <zone> and <cluster> using the bind method.
func NewTemplate(names ...string) Template {
	return &labelTemplate{
		names:     names,
		variables: map[string]string{},
	}
}

type labelTemplate struct {
	names     []string
	variables map[string]string
	lock      sync.Mutex
}

func (template *labelTemplate) replace(name string) string {
	for variable, value := range template.variables {
		v := fmt.Sprintf("$%v$", variable)
		if strings.Contains(name, v) {
			name = strings.Replace(name, v, value, -1)
		}
	}
	return name
}

func (template *labelTemplate) Instantiate() *Label {
	defer template.lock.Unlock()
	template.lock.Lock()

	names := make([]string, len(template.names))
	for i, name := range template.names {
		names[i] = template.replace(name)
	}
	return NewLabel(names...)
}

func (template *labelTemplate) Bind(name, value string) Template {
	defer template.lock.Unlock()
	template.lock.Lock()

	template.variables[name] = value
	return template
}

func (template *labelTemplate) Mappings() map[string]string {
	defer template.lock.Unlock()
	template.lock.Lock()

	mappings := make(map[string]string, len(template.variables))
	for _, name := range template.names {
		for i, variable := range strings.Split(name, "$") {
			// A variable foo should always be used as $foo$ so every other string in the above split is a variable name
			if i%2 != 1 {
				continue
			}
			mappings[variable] = template.variables[variable]
		}
	}
	return mappings
}
