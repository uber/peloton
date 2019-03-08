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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTemplateSet_Bind(t *testing.T) {
	template := NewTemplate("foo", "$bar$", "$baz$")
	variables := NewTemplateSet()
	variables.Add(template).Bind("bar", "bar")

	assert.Equal(t, "foo.bar.$baz$", template.Instantiate().String())
}

func TestTemplateSet_AddAll(t *testing.T) {
	template1 := NewTemplate("1")
	template2 := NewTemplate("2")
	set1 := NewTemplateSet()
	set1.Add(template1)
	set1.Add(template2)
	set2 := NewTemplateSet().(*templateSet)
	set2.AddAll(set1)

	assert.Equal(t, 2, len(set2.templates))
}

func TestTemplateSet_Templates(t *testing.T) {
	template1 := NewTemplate("1")
	template2 := NewTemplate("2")
	set := NewTemplateSet()
	set.Add(template1)
	set.Add(template2)

	assert.Equal(t, 2, len(set.Templates()))
}

func TestTemplateSet_Mappings(t *testing.T) {
	template := NewTemplate("foo", "$bar$", "$baz$")
	template.Bind("bar", "bar")
	variables := NewTemplateSet().Add(template)

	assert.Equal(t, template.Mappings(), variables.Mappings())
}
