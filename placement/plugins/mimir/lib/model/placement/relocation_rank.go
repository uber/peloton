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

import "fmt"

// RelocationRank contains a rank (number) that represents how many other groups, different than the currently
// assigned, that are better to place this entity on.
type RelocationRank struct {
	// Entity to be relocated.
	Entity *Entity

	// CurrentGroup the group where the entity is currently placed.
	CurrentGroup *Group

	// Rank is a lower bound on the number of groups different from the current group which are better than the current
	// group.
	Rank int

	// Transcript holds the relocation transcript of the relocation rank of the entity.
	Transcript *Transcript
}

// NewRelocationRank creates a new relocation rank for the entity.
func NewRelocationRank(entity *Entity, current *Group) *RelocationRank {
	return &RelocationRank{
		Entity:       entity,
		CurrentGroup: current,
		Transcript:   NewTranscript(fmt.Sprintf("requirements fulfillment for %v", entity.Name)),
	}
}
