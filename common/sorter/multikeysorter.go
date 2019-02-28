// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sorter

import "sort"

type lessFunc func(p1, p2 interface{}) bool

// MultiKeySorter implements the Sort interface, sorting the changes within.
// it will sort the array of interfaces based on ordered list
type MultiKeySorter struct {
	// List of  interfaces
	List []interface{}
	// list of functions which will be called by OrderedBy
	less []lessFunc
}

// Sort sorts the list according to
// the less functions passed to OrderedBy.
func (ms *MultiKeySorter) Sort(List []interface{}) {
	ms.List = List
	sort.Sort(ms)
}

// OrderedBy returns a MultiKeySorter that sorts
// using the less functions, "in order".
// Call its Sort method to sort the data.
func OrderedBy(less ...lessFunc) *MultiKeySorter {
	return &MultiKeySorter{
		less: less,
	}
}

// Len is part of sort.Interface.
// returns the length of list
func (ms *MultiKeySorter) Len() int {
	return len(ms.List)
}

// Swap is part of sort.Interface.
// swaps the elements the list
func (ms *MultiKeySorter) Swap(i, j int) {
	ms.List[i], ms.List[j] = ms.List[j], ms.List[i]
}

// Less is part of sort.Interface. It is implemented by looping along the
// less functions until it finds a comparison that discriminates between
// the two items (one is less than the other).
func (ms *MultiKeySorter) Less(i, j int) bool {
	p, q := ms.List[i], ms.List[j]
	// Try all but the last comparison.
	// based on the sort implementation last
	// element is the pivot for quick sort
	// so it goes till second last element.
	var k int
	for k = 0; k < len(ms.less)-1; k++ {
		less := ms.less[k]
		switch {
		case less(p, q):
			// p < q, so we have a decision.
			return true
		case less(q, p):
			// p > q, so we have a decision.
			// that p greater then q
			return false
		}
		// p == q; try the next comparison.
	}
	// All comparisons to here said "equal", so just return whatever
	// the final comparison reports.
	return ms.less[k](p, q)
}
