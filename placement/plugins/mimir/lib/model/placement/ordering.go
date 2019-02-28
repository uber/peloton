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

package placement

// Ordering returns a tuple of floats given a group relative to an entity and a scope of groups.
type Ordering interface {
	// Tuple returns a tuple of floats created from the group, scope groups and the entity.
	Tuple(group *Group, scopeSet *ScopeSet, entity *Entity) []float64
}

// Less returns true iff tuple1 is strictly less than tuple2, i.e. tuple1 < tuple2 when comparing the entries
// lexicographically
func Less(tuple1, tuple2 []float64) bool {
	if tuple1 == nil {
		return false
	}
	if tuple2 == nil {
		return true
	}
	length := len(tuple1)
	if len(tuple2) < length {
		length = len(tuple2)
	}
	for i := 0; i < length; i++ {
		if tuple1[i] < tuple2[i] {
			return true
		} else if tuple1[i] > tuple2[i] {
			return false
		}

		// in case of equal, compare the rest of the elements
	}
	if len(tuple1) < len(tuple2) {
		return true
	}
	return false
}

// NameOrdering returns a new ordering by the group name.
func NameOrdering() Ordering {
	return name{}
}

type name struct{}

func (ordering name) Tuple(group *Group, scopeSet *ScopeSet, entity *Entity) []float64 {
	result := make([]float64, len(group.Name))
	for i, char := range group.Name {
		result[i] = float64(char)
	}
	return result
}
