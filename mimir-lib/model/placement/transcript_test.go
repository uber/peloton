// @generated AUTO GENERATED - DO NOT EDIT! 9f8b9e47d86b5e1a3668856830c149e768e78415
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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTranscript_IncFailed_on_nil_transcript(t *testing.T) {
	var transcript *Transcript
	transcript.IncFailed()
}

func TestTranscript_IncFailed(t *testing.T) {
	transcript := NewTranscript("transcript")
	transcript.IncFailed()

	assert.Equal(t, 1, transcript.GroupsFailed)
}

func TestTranscript_IncPassed_on_nil_transcript(t *testing.T) {
	var transcript *Transcript
	transcript.IncPassed()
}

func TestTranscript_IncPassed(t *testing.T) {
	transcript := NewTranscript("transcript")
	transcript.IncPassed()

	assert.Equal(t, 1, transcript.GroupsPassed)
}

type mockRequirement struct{}

func (requirement *mockRequirement) Passed(group *Group, scopeGroups []*Group, entity *Entity,
	transcript *Transcript) bool {
	return false
}

func (requirement *mockRequirement) String() string {
	return "sub requirement"
}

func (requirement *mockRequirement) Composite() (composite bool, typeName string) {
	return false, "type"
}

func TestTranscript_Subscript_on_nil_transcript(t *testing.T) {
	var transcript *Transcript

	requirement := &mockRequirement{}
	subscript := transcript.Subscript(requirement)
	assert.Nil(t, subscript)
}

func TestTranscript_Subscript(t *testing.T) {
	transcript := NewTranscript("transcript")

	assert.Equal(t, 0, len(transcript.Subscripts))
	requirement := &mockRequirement{}
	subscript := transcript.Subscript(requirement)
	assert.Equal(t, 1, len(transcript.Subscripts))
	assert.Equal(t, subscript, transcript.Subscripts[requirement])
}

func TestTranscript_Print(t *testing.T) {
	requirement := &mockRequirement{}
	transcript := NewTranscript("transcript")
	transcript.Subscript(requirement)
	result := transcript.String()
	assert.Contains(t, result, "transcript passed 0 times and failed 0 times")
	assert.Contains(t, result, "sub requirement passed 0 times and failed 0 times")
}
