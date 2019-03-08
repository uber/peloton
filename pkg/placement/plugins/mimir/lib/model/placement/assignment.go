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

import (
	"fmt"
)

// Assignment represents a placement of an entity in a given group or the failure to be able to place the
// entity in a group that satisfies all requirements of the entity.
type Assignment struct {
	// Entity to be placed.
	Entity *Entity

	// AssignedGroup the Group that the Entity got assigned to.
	AssignedGroup *Group

	// Failed is true if the assignment failed.
	Failed bool

	// Transcript holds the placement transcript of the placement of the entity.
	Transcript *Transcript
}

// NewAssignment creates a new empty assignment for the entity.
func NewAssignment(entity *Entity) *Assignment {
	return &Assignment{
		Entity:     entity,
		Failed:     true,
		Transcript: NewTranscript(fmt.Sprintf("requirements fulfillment for %v", entity.Name)),
	}
}
