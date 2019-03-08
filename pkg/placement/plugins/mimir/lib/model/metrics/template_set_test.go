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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTemplateSet_Bind(t *testing.T) {
	set := NewTemplateSet()
	template := NewTemplate(MemoryUsed)
	set.Add(template)

	mapping := set.Mappings()
	assert.Equal(t, 1, len(mapping))
	assert.Equal(t, 0.0, mapping[MemoryUsed])
	set.Bind(MemoryUsed, 42.0)
	mapping = set.Mappings()
	assert.Equal(t, 1, len(mapping))
	assert.Equal(t, 42.0, mapping[MemoryUsed])
}

func TestTemplateSet_Add(t *testing.T) {
	set := NewTemplateSet()
	template := NewTemplate(MemoryUsed)
	template.Bind(42.0)
	set.Add(template)

	mapping := set.Mappings()
	assert.Equal(t, 1, len(mapping))
	assert.Equal(t, 42.0, mapping[MemoryUsed])
}

func TestTemplateSet_Add_adding_the_same_template_twice_overrides_the_old_template(t *testing.T) {
	set := NewTemplateSet()
	template := NewTemplate(MemoryUsed)
	template.Bind(42.0)
	set.Add(template)
	set.Add(template)

	mapping := set.Mappings()
	assert.Equal(t, 1, len(mapping))
	assert.Equal(t, 42.0, mapping[MemoryUsed])
}

func TestTemplateSet_AddAll(t *testing.T) {
	set1 := NewTemplateSet()
	memoryUsedTemplate := NewTemplate(MemoryUsed)
	memoryUsedTemplate.Bind(2 * GiB)
	diskUsedTemplate := NewTemplate(DiskUsed)
	diskUsedTemplate.Bind(128 * GiB)
	set1.Add(memoryUsedTemplate)
	set1.Add(diskUsedTemplate)

	set2 := NewTemplateSet()
	mapping := set2.Mappings()
	assert.Equal(t, 0, len(mapping))

	set2.AddAll(set1)

	mapping = set2.Mappings()
	assert.Equal(t, 2, len(mapping))
	assert.Equal(t, 2*GiB, mapping[MemoryUsed])
	assert.Equal(t, 128*GiB, mapping[DiskUsed])
}

func TestTemplateSet_Templates(t *testing.T) {
	set := NewTemplateSet()
	memoryUsedTemplate := NewTemplate(MemoryUsed)
	diskUsedTemplate := NewTemplate(DiskUsed)
	set.Add(memoryUsedTemplate)
	set.Add(diskUsedTemplate)

	templates := set.Templates()
	assert.Equal(t, 2, len(templates))
	expected := map[Type]Template{
		MemoryUsed: memoryUsedTemplate,
		DiskUsed:   diskUsedTemplate,
	}
	assert.Equal(t, expected, templates)
}

func TestTemplateSet_Mappings(t *testing.T) {
	set := NewTemplateSet()
	memoryUsedTemplate := NewTemplate(MemoryUsed)
	memoryUsedTemplate.Bind(2 * GiB)
	diskUsedTemplate := NewTemplate(DiskUsed)
	diskUsedTemplate.Bind(128 * GiB)
	set.Add(memoryUsedTemplate)
	set.Add(diskUsedTemplate)

	mapping := set.Mappings()
	expected := map[Type]float64{
		MemoryUsed: 2 * GiB,
		DiskUsed:   128 * GiB,
	}
	assert.Equal(t, expected, mapping)
}
