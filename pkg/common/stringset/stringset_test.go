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

package stringset

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	testItem = "testItem"
)

func TestStringSet_New(t *testing.T) {
	testSet := New()
	assert.NotNil(t, testSet)
}

func TestStringSet_Add(t *testing.T) {
	// Create a new StringSet
	testSet := &stringSet{
		m: make(map[string]bool),
	}
	// Add test task to StringSet
	testSet.Add(testItem)
	assert.Equal(t, true, testSet.m[testItem])
}

func TestStringSet_Contains(t *testing.T) {
	// Create a new StringSet
	testSet := &stringSet{
		m: make(map[string]bool),
	}
	assert.Equal(t, false, testSet.Contains(testItem))

	// Add test task to StringSet
	testSet.m[testItem] = true
	assert.Equal(t, true, testSet.Contains(testItem))
}

func TestStringSet_Remove(t *testing.T) {
	// Create a new StringSet
	testSet := &stringSet{
		m: make(map[string]bool),
	}
	// Add test task to StringSet
	testSet.m[testItem] = true
	assert.Equal(t, true, testSet.m[testItem])

	testSet.Remove(testItem)
	assert.Equal(t, false, testSet.m[testItem])
}

func TestStringSet_Clear(t *testing.T) {
	// Create a new StringSet
	testSet := &stringSet{
		m: make(map[string]bool),
	}
	// Add test task to StringSet
	testSet.m[testItem] = true
	assert.Equal(t, 1, len(testSet.m))

	testSet.Clear()
	assert.Equal(t, 0, len(testSet.m))
}

func TestStringSet_ToSlice(t *testing.T) {
	// Create a new StringSet
	testSet := &stringSet{
		m: make(map[string]bool),
	}

	testItems := []string{
		"testitem1",
		"testitem2",
	}
	// Add testItems to StringSet
	for _, item := range testItems {
		testSet.m[item] = true
	}
	items := testSet.ToSlice()
	assert.Len(t, items, len(testItems))
	for _, item := range items {
		assert.True(t, testSet.Contains(item))
	}
}

var flagtests = []struct {
	name      string
	testdata1 []string
	testdata2 []string
	out       []string
}{
	{"both sets void", []string{}, []string{}, []string{}},
	{"first set void", []string{}, []string{"testitem1", "testitem2"}, []string{}},
	{"second set void", []string{"testitem2", "testitem1"}, []string{}, []string{}},
	{"intersect all first set", []string{"testitem1"}, []string{"testitem1", "testitem2"}, []string{"testitem1"}},
	{"intersect all second set", []string{"testitem1", "testitem2"}, []string{"testitem1"}, []string{"testitem1"}},
	{"interscet subset of first set", []string{"testitem1", "testitem2", "testitem3"}, []string{"testitem1", "testitem2"}, []string{"testitem1", "testitem2"}},
	{"interscet subset of second set", []string{"testitem1", "testitem2"}, []string{"testitem1", "testitem2", "testitem3"}, []string{"testitem1", "testitem2"}},
}

func TestStringSet_Intersect(t *testing.T) {

	for _, tt := range flagtests {
		t.Run(tt.name, func(t *testing.T) {

			// Creates first testSet
			testSet1 := &stringSet{
				m: make(map[string]bool),
			}

			// Add testItems to testSet1
			for _, item := range tt.testdata1 {
				testSet1.m[item] = true
			}

			// Creates second testSet
			testSet2 := &stringSet{
				m: make(map[string]bool),
			}

			// Add testItems to testSet2
			for _, item := range tt.testdata2 {
				testSet2.m[item] = true
			}
			s := testSet1.Intersect(testSet2)
			slice := s.ToSlice()
			sort.Strings(slice)
			sort.Strings(tt.out)

			for i := range slice {
				if slice[i] != tt.out[i] {
					t.Errorf("got %q, want %q", slice[i], tt.out[i])
				}
			}
		})
	}
}
