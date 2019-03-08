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

// Transcriptable represents something we can create a transcript for, it is usually a requirement which can be composed
// of sub-requirements, hence we want a human readable name in the string-method and to know whatever it is composite
// in the composite-method and the type of composition.
type Transcriptable interface {
	// String will return a human readable string representation of the transcript-able.
	String() string

	// Composite will return true iff the transcript-able is composed of other transcript-ables and a type name describing
	// the type of transcript-able.
	Composite() (composite bool, typeName string)
}

// EmptyTranscript returns an empty transcript.
func EmptyTranscript() Transcriptable {
	return empty{}
}

type empty struct{}

func (transcript empty) String() string {
	return "empty"
}

func (transcript empty) Composite() (bool, string) {
	return false, "empty-type"
}
