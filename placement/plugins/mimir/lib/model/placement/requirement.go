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

// Requirement tells if a given group passes the requirements for a given entity in relation to the given scope.
type Requirement interface {
	// Passed returns true iff group passes the requirement in relation to the given scope.
	Passed(group *Group, scopeSet *ScopeSet, entity *Entity, transcript *Transcript) bool

	// The requirement should be transcript-able, so we can get metrics about how many groups pass and fail the
	// requirement.
	Transcriptable
}

// FailedRequirement returns a requirement that never passes.
func FailedRequirement() Requirement {
	return failed{}
}

type failed struct {
	empty
}

func (requirement failed) Passed(group *Group, scopeSet *ScopeSet, entity *Entity, transcript *Transcript) bool {
	return false
}
