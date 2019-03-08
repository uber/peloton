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
	"sort"
	"sync"
)

// Bag represents a bag of labels and their counts, i.e. it is a multi-bag.
type Bag struct {
	bag  map[string]*labelCount
	lock sync.RWMutex
}

// NewBag will create a new label bag.
func NewBag() *Bag {
	return &Bag{
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
func (bag *Bag) Size() int {
	bag.lock.RLock()
	defer bag.lock.RUnlock()

	return len(bag.bag)
}

// Add adds a label to this label bag with a count of 1.
func (bag *Bag) Add(labels ...*Label) {
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

func copyContent(src *Bag) map[string]*labelCount {
	src.lock.RLock()
	defer src.lock.RUnlock()
	dst := make(map[string]*labelCount, len(src.bag))
	for _, pair := range src.bag {
		dst[pair.label.String()] = pair.clone()
	}
	return dst
}

// AddAll adds all labels in the given label bag to this label bag.
func (bag *Bag) AddAll(other *Bag) {
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

// Set adds the value in the label bag and sets it count.
func (bag *Bag) Set(label *Label, count int) {
	bag.lock.Lock()
	defer bag.lock.Unlock()

	if oldPair, found := bag.bag[label.String()]; found {
		oldPair.count = count
	} else {
		bag.bag[label.String()] = &labelCount{
			label: label,
			count: count,
		}
	}
}

// SetAll sets all labels in the given label bag to the labels and counts in the other label bag, any labels that
// exists in the label bag but not in other will still be present with the same count.
func (bag *Bag) SetAll(other *Bag) {
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

type sortedLabels []*Label

func (labels sortedLabels) Len() int {
	return len(labels)
}

func (labels sortedLabels) Less(i, j int) bool {
	return labels[i].simpleName < labels[j].simpleName
}

func (labels sortedLabels) Swap(i, j int) {
	labels[i], labels[j] = labels[j], labels[i]
}

// Labels returns a new slice of all the labels in the bag.
func (bag *Bag) Labels() []*Label {
	bag.lock.RLock()
	defer bag.lock.RUnlock()

	result := make(sortedLabels, 0, len(bag.bag))
	for _, pair := range bag.bag {
		result = append(result, pair.label)
	}
	sort.Sort(result)
	return result
}

// Find will find all labels matching the given label.
func (bag *Bag) Find(label *Label) []*Label {
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

func (bag *Bag) findByPattern(pattern *Label) []*Label {
	var result []*Label
	for _, pair := range bag.bag {
		if !pattern.Match(pair.label) {
			continue
		}
		result = append(result, pair.label)
	}
	return result
}

// Contains returns true iff the count of the label is strictly higher than zero.
func (bag *Bag) Contains(label *Label) bool {
	return bag.Count(label) > 0
}

// Count counts the number of labels that this label matches.
func (bag *Bag) Count(label *Label) int {
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

func (bag *Bag) countByPattern(pattern *Label) int {
	counts := 0
	for _, pair := range bag.bag {
		if !pattern.Match(pair.label) {
			continue
		}
		counts += pair.count
	}
	return counts
}
