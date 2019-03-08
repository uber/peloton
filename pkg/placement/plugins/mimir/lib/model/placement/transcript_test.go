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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTranscript_IncPassed_on_nil_transcript(t *testing.T) {
	var transcript *Transcript
	transcript.IncPassed()
}

func TestTranscript_IncPassed(t *testing.T) {
	transcript := NewTranscript("transcript")
	transcript.IncPassed()

	assert.Equal(t, 1, transcript.GroupsPassed)
}

func TestTranscript_IncFailed_on_nil_transcript(t *testing.T) {
	var transcript *Transcript
	transcript.IncFailed()
}

func TestTranscript_IncFailed(t *testing.T) {
	transcript := NewTranscript("transcript")
	transcript.IncFailed()

	assert.Equal(t, 1, transcript.GroupsFailed)
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

func TestTranscript_Subscript_adds_a_new_subscript_for_the_given_transcriptable(t *testing.T) {
	transcript := NewTranscript("transcript")

	assert.Equal(t, 0, len(transcript.Subscripts))
	requirement := &mockRequirement{}
	subscript := transcript.Subscript(requirement)
	assert.Equal(t, 1, len(transcript.Subscripts))
	assert.Equal(t, subscript, transcript.Subscripts[requirement])
}

func TestTranscript_String_returns_a_human_readable_representation_of_the_transcript(t *testing.T) {
	requirement := &mockRequirement{}
	transcript := NewTranscript("transcript")
	transcript.Subscript(requirement)
	result := transcript.String()
	assert.Contains(t, result, "transcript passed 0 times and failed 0 times")
	assert.Contains(t, result, "sub requirement passed 0 times and failed 0 times")
}

func TestTranscript_Copy_returns_nil_on_a_nil_transcript(t *testing.T) {
	var transcript *Transcript
	assert.Nil(t, transcript.Copy())
}

func TestTranscript_Copy_makes_a_deep_copy_of_the_transcript_and_subscripts_and_shallow_copy_of_transcriptables(t *testing.T) {
	transcript := NewTranscript("transcript")

	requirement := &mockRequirement{}
	transcript.Subscript(requirement)

	copy := transcript.Copy()

	assert.Equal(t, transcript.Requirement, copy.Requirement)
	assert.Equal(t, transcript.GroupsPassed, copy.GroupsPassed)
	assert.Equal(t, transcript.GroupsFailed, copy.GroupsFailed)
	assert.Equal(t, len(transcript.Subscripts), len(copy.Subscripts))
	for transcriptable, subscript := range transcript.Subscripts {
		copySubscript, exists := copy.Subscripts[transcriptable]
		require.True(t, exists)
		assert.True(t, subscript != copySubscript)
		assert.Equal(t, subscript.Requirement, copySubscript.Requirement)
		assert.Equal(t, subscript.GroupsPassed, copySubscript.GroupsPassed)
		assert.Equal(t, subscript.GroupsFailed, copySubscript.GroupsFailed)
		assert.Equal(t, len(subscript.Subscripts), len(copySubscript.Subscripts))
	}
}

func TestTranscript_Add_does_nothing_on_nil_transcripts(t *testing.T) {
	var transcript1, transcript2 *Transcript
	transcript1.Add(transcript2)

	assert.Nil(t, transcript1)
	assert.Nil(t, transcript2)
}

func TestTranscript_Add_adds_other_transcript_to_this_transcript(t *testing.T) {
	transcript1 := NewTranscript("transcript")
	transcript2 := NewTranscript("transcript")

	requirement := &mockRequirement{}
	transcript2.Subscript(requirement)

	transcript1.Add(transcript2)

	assert.Equal(t, transcript1.Requirement, transcript2.Requirement)
	assert.Equal(t, transcript1.GroupsPassed, transcript2.GroupsPassed)
	assert.Equal(t, transcript1.GroupsFailed, transcript2.GroupsFailed)
	assert.Equal(t, len(transcript1.Subscripts), len(transcript2.Subscripts))
	for transcriptable, subscript := range transcript1.Subscripts {
		copySubscript, exists := transcript2.Subscripts[transcriptable]
		require.True(t, exists)
		assert.True(t, subscript != copySubscript)
		assert.Equal(t, subscript.Requirement, copySubscript.Requirement)
		assert.Equal(t, subscript.GroupsPassed, copySubscript.GroupsPassed)
		assert.Equal(t, subscript.GroupsFailed, copySubscript.GroupsFailed)
		assert.Equal(t, len(subscript.Subscripts), len(copySubscript.Subscripts))
	}
}
