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
	"strings"
)

// Label represents an immutable label which consists of a list of names. Since a label is a list of names the label
// have a hierarchy, e.g. the labels foo.bar and foo.baz can be thought of as being nested under the label foo.*.
type Label struct {
	// names is a list of names, e.g. foo, bar and baz
	names []string
	// simpleName is a concatenation of names with . in between, e.g. foo.bar.baz
	simpleName string
	// wildcard is true iff the label contains a wildcard
	wildcard bool
}

// NewLabel creates a new label from the given names.
func NewLabel(names ...string) *Label {
	simpleName := strings.Join(names, ".")
	return &Label{
		names:      names,
		simpleName: simpleName,
		wildcard:   strings.Contains(simpleName, "*"),
	}
}

// Wildcard returns true iff the label contains a wildcard.
func (label *Label) Wildcard() bool {
	return label.wildcard
}

// Names returns a copy of the names in the label.
func (label *Label) Names() []string {
	result := make([]string, 0, len(label.names))
	result = append(result, label.names...)
	return result
}

// String returns a concatenation of the names in the label with dot as a separator.
func (label *Label) String() string {
	return label.simpleName
}

func (label *Label) nameMatch(name1, name2 string) bool {
	if name1 == "*" || name2 == "*" {
		return true
	}
	return name1 == name2
}

// Match returns true iff the label matches the other label or vice versa taking wildcards into account.
// When matching one label to another label then a wildcard will match any name in the other label at the same position.
func (label *Label) Match(other *Label) bool {
	if label == other {
		return true
	}
	if !label.wildcard && !other.wildcard {
		return label.simpleName == other.simpleName
	}
	if len(label.names) != len(other.names) {
		return false
	}
	for i := range label.names {
		if !label.nameMatch(label.names[i], other.names[i]) {
			return false
		}
	}
	return true
}
