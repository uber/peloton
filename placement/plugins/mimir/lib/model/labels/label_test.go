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

func TestLabel_Match_WithoutWildcard(t *testing.T) {
	pattern := NewLabel("foo", "bar", "baz")
	assert.False(t, pattern.Wildcard())

	label1 := NewLabel("foo", "bar", "baz")
	label2 := NewLabel("bar")

	assert.True(t, pattern.Match(label1))
	assert.False(t, pattern.Match(label2))
}

func TestLabel_Match_WithWildcardInPattern(t *testing.T) {
	pattern := NewLabel("foo", "*", "bar", "baz")
	assert.True(t, pattern.Wildcard())

	label1 := NewLabel("foo", "bar", "baz")
	label2 := NewLabel("foo", "some", "bar", "baz")

	assert.False(t, pattern.Match(label1))
	assert.True(t, pattern.Match(label2))

}

func TestLabel_Match_WithWildcardInOtherLabels(t *testing.T) {
	pattern := NewLabel("foo", "bar", "baz")
	assert.False(t, pattern.Wildcard())

	label1 := NewLabel("*", "bar", "baz")
	label2 := NewLabel("foo", "some", "bar", "baz")
	label3 := NewLabel("foo", "*", "baz")
	label4 := NewLabel("foo", "bar", "*")

	assert.True(t, pattern.Match(label1))
	assert.False(t, pattern.Match(label2))
	assert.True(t, pattern.Match(label3))
	assert.True(t, pattern.Match(label4))
}

func TestLabel_Match_WithWildcardInPatternAndOtherLabels(t *testing.T) {
	pattern := NewLabel("foo", "*", "baz")
	assert.True(t, pattern.Wildcard())

	label1 := NewLabel("*", "bar", "baz")
	label2 := NewLabel("foo", "*", "baz")
	label3 := NewLabel("foo", "bar", "*")
	label4 := NewLabel("foo", "bar", "bax")

	assert.True(t, pattern.Match(label1))
	assert.True(t, pattern.Match(label2))
	assert.True(t, pattern.Match(label3))
	assert.False(t, pattern.Match(label4))
}

func TestLabel_Names_ReturnsACopyOfNames(t *testing.T) {
	label := NewLabel("foo", "bar", "baz")
	names := label.Names()

	assert.Equal(t, label.names, names)
	names[1] = "42"
	assert.NotEqual(t, names, label.names)
}

func TestLabel_String_JoinsTheLabelNames(t *testing.T) {
	label := NewLabel("foo", "bar", "baz")

	assert.Equal(t, "foo.bar.baz", label.String())
}

func TestLabel_Match_ReturnsTrueWhenSamePointer(t *testing.T) {
	label := NewLabel("foo", "bar", "baz")
	assert.True(t, label.Match(label))
}
