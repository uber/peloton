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
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBag_Size(t *testing.T) {
	bag := NewBag()
	label := NewLabel("some", "label")
	assert.Equal(t, 0, bag.Size())
	bag.Add(label)
	assert.Equal(t, 1, bag.Size())
	bag.Add(label)
	assert.Equal(t, 1, bag.Size())
}

func TestBag_Contains(t *testing.T) {
	bag := NewBag()
	label := NewLabel("some", "label")
	assert.False(t, bag.Contains(label))
	bag.Add(label)
	assert.True(t, bag.Contains(label))
}

func TestBag_AddWillAddALabelToTheBag(t *testing.T) {
	bag := NewBag()
	label := NewLabel("some", "label")
	bag.Add(label)
	assert.Equal(t, 1, bag.Count(label))
}

func TestBag_AddAllWillAddAllLabelsToTheBag(t *testing.T) {
	bag1 := NewBag()
	bag2 := NewBag()
	label1 := NewLabel("some", "label", "1")
	label2 := NewLabel("some", "label", "2")
	bag2.Add(label1)
	bag2.Add(label2)
	bag2.Add(label2)

	bag1.Add(label2)
	bag1.AddAll(bag2)
	assert.Equal(t, 1, bag1.Count(label1))
	assert.Equal(t, 3, bag1.Count(label2))
}

func TestBag_Set(t *testing.T) {
	bag := NewBag()
	label1 := NewLabel("some", "label", "1")
	label2 := NewLabel("some", "label", "2")
	bag.Add(label1)

	bag.Set(label1, 3)
	bag.Set(label2, 2)
	assert.Equal(t, 3, bag.Count(label1))
	assert.Equal(t, 2, bag.Count(label2))
}

func TestBag_SetAllReplacesAllLabels(t *testing.T) {
	bag1 := NewBag()
	bag2 := NewBag()
	label1 := NewLabel("some", "label", "1")
	label2 := NewLabel("some", "label", "2")
	bag2.Add(label1)
	bag2.Add(label1)
	bag2.Add(label1)
	bag2.Add(label2)
	bag2.Add(label2)
	bag2.Add(label2)

	bag1.Add(label1)
	assert.Equal(t, 1, bag1.Count(label1))
	assert.Equal(t, 0, bag1.Count(label2))

	bag1.SetAll(bag2)

	assert.Equal(t, 3, bag1.Count(label1))
	assert.Equal(t, 3, bag1.Count(label2))
}

func TestBag_Labels(t *testing.T) {
	bag := NewBag()
	for i := 3; i > 0; i-- {
		bag.Add(NewLabel("some", "label", strconv.Itoa(i)))
	}

	labels := bag.Labels()
	assert.Equal(t, 3, len(labels))
	for _, label := range labels {
		assert.True(t, bag.Contains(label))
	}
}

func TestBag_Find(t *testing.T) {
	bag := NewBag()
	label := NewLabel("some", "label")
	bag.Add(label)

	assert.Equal(t, 1, len(bag.Find(label)))
}

func TestBag_FindWithWildCards(t *testing.T) {
	bag := NewBag()
	pattern1 := NewLabel("some", "label", "*")
	pattern2 := NewLabel("*", "label", "*")
	label1 := NewLabel("some", "label", "1")
	label2 := NewLabel("my", "label", "2")
	label3 := NewLabel("some", "label", "3")
	label4 := NewLabel("not", "matched")
	bag.Add(label1)
	bag.Add(label2)
	bag.Add(label2)
	bag.Add(label3)
	bag.Add(label4)

	found1 := map[string]string{}
	for _, label := range bag.Find(pattern1) {
		found1[label.String()] = label.String()
	}
	assert.Equal(t, 2, len(found1))
	assert.True(t, found1["some.label.1"] == "some.label.1")
	assert.True(t, found1["some.label.3"] == "some.label.3")

	found2 := map[string]string{}
	for _, label := range bag.Find(pattern2) {
		found2[label.String()] = label.String()
	}
	assert.Equal(t, 3, len(found2))
	assert.True(t, found2["some.label.1"] == "some.label.1")
	assert.True(t, found2["my.label.2"] == "my.label.2")
	assert.True(t, found2["some.label.3"] == "some.label.3")
}

func TestBag_CountWillCountAllMatchingLabels(t *testing.T) {
	bag := NewBag()
	pattern1 := NewLabel("some", "label", "*")
	pattern2 := NewLabel("*", "label", "*")
	label1 := NewLabel("some", "label", "1")
	label2 := NewLabel("my", "label", "2")
	label3 := NewLabel("some", "label", "3")
	label4 := NewLabel("not", "matched")
	bag.Add(label1)
	bag.Add(label2)
	bag.Add(label2)
	bag.Add(label3)
	bag.Add(label4)

	assert.Equal(t, 2, bag.Count(pattern1))
	assert.Equal(t, 4, bag.Count(pattern2))
}
