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

package requirements

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTranscript_IncFailed(t *testing.T) {
	transcript := NewTranscript("transcript")
	transcript.IncFailed()

	assert.Equal(t, 1, transcript.GroupsFailed)
}

func TestTranscript_IncPassed(t *testing.T) {
	transcript := NewTranscript("transcript")
	transcript.IncPassed()

	assert.Equal(t, 1, transcript.GroupsPassed)
}

func TestTranscript_Subscript(t *testing.T) {
	transcript := NewTranscript("transcript")

	assert.Equal(t, 0, len(transcript.Subscripts))
	requirement := NewAndRequirement()
	subscript := transcript.Subscript(requirement)
	assert.Equal(t, 1, len(transcript.Subscripts))
	assert.Equal(t, subscript, transcript.Subscripts[requirement])
}
