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

import "sync"

// LabelBag represents a bag of labels and their counts, i.e. it is a multi-bag.
type LabelBag struct {
	bag  map[string]*labelCount
	lock sync.RWMutex
}

// NewLabelBag will create a new label bag.
func NewLabelBag() *LabelBag {
	return &LabelBag{
		bag: map[string]*labelCount{},
	}
}

type labelCount struct {
	label *Label
	count int
}

func (pair *labelCount) clone() *labelCount {
	return &labelCount{
		label: pair.label,
		count: pair.count,
	}
}

// Size will return the number of different labels in the label bag.
func (bag *LabelBag) Size() int {
	bag.lock.RLock()
	defer bag.lock.RUnlock()

	return len(bag.bag)
}

// Add adds a label to this label bag with a count of 1.
func (bag *LabelBag) Add(labels ...*Label) {
	bag.lock.Lock()
	defer bag.lock.Unlock()

	for _, label := range labels {
		key := label.String()
		if oldPair, found := bag.bag[key]; found {
			oldPair.count++
		} else {
			bag.bag[key] = &labelCount{
				label: label,
				count: 1,
			}
		}
	}
}

func copyContent(src *LabelBag) map[string]*labelCount {
	src.lock.RLock()
	defer src.lock.RUnlock()
	dst := make(map[string]*labelCount, len(src.bag))
	for _, pair := range src.bag {
		dst[pair.label.String()] = pair.clone()
	}
	return dst
}

// AddAll adds all labels in the given label bag to this label bag.
func (bag *LabelBag) AddAll(other *LabelBag) {
	otherCopy := copyContent(other)

	bag.lock.Lock()
	defer bag.lock.Unlock()

	for _, pair := range otherCopy {
		if oldPair, found := bag.bag[pair.label.String()]; found {
			oldPair.count += pair.count
		} else {
			bag.bag[pair.label.String()] = pair
		}
	}
}

// SetAll sets all labels in the given label bag to the labels and counts in the other label bag, any labels that
// exists in the label bag but not in other will still be present with the same count.
func (bag *LabelBag) SetAll(other *LabelBag) {
	otherCopy := copyContent(other)

	bag.lock.Lock()
	defer bag.lock.Unlock()

	for _, pair := range otherCopy {
		if oldPair, found := bag.bag[pair.label.String()]; found {
			oldPair.count = pair.count
		} else {
			bag.bag[pair.label.String()] = pair
		}
	}
}

// Find will find all labels matching the given label.
func (bag *LabelBag) Find(label *Label) []*Label {
	bag.lock.RLock()
	defer bag.lock.RUnlock()

	if label.Wildcard() {
		return bag.findByPattern(label)
	}
	var result []*Label
	if pair, exists := bag.bag[label.String()]; exists {
		result = append(result, pair.label)
	}
	return result
}

func (bag *LabelBag) findByPattern(pattern *Label) []*Label {
	var result []*Label
	for _, pair := range bag.bag {
		if !pattern.Match(pair.label) {
			continue
		}
		result = append(result, pair.label)
	}
	return result
}

// Count counts the number of labels that this label matches.
func (bag *LabelBag) Count(label *Label) int {
	bag.lock.RLock()
	defer bag.lock.RUnlock()

	if label.Wildcard() {
		return bag.countByPattern(label)
	}
	pair, exists := bag.bag[label.String()]
	if exists {
		return pair.count
	}
	return 0
}

func (bag *LabelBag) countByPattern(pattern *Label) int {
	counts := 0
	for _, pair := range bag.bag {
		if !pattern.Match(pair.label) {
			continue
		}
		counts += pair.count
	}
	return counts
}
