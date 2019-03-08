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
	"strings"
)

// Transcript represents a transcript of which requirements passed and failed when evaluating groups for an entity.
type Transcript struct {
	Requirement  string
	GroupsPassed int
	GroupsFailed int
	Subscripts   map[Transcriptable]*Transcript
}

// NewTranscript creates a new transcript with a description.
func NewTranscript(name string) *Transcript {
	return &Transcript{
		Requirement: name,
		Subscripts:  map[Transcriptable]*Transcript{},
	}
}

// IncPassed will increment the number of groups that passed the requirements.
func (transcript *Transcript) IncPassed() {
	if transcript == nil {
		return
	}
	transcript.GroupsPassed++
}

// IncFailed will increment the number of groups that failed the requirements.
func (transcript *Transcript) IncFailed() {
	if transcript == nil {
		return
	}
	transcript.GroupsFailed++
}

// Subscript will create a sub transcript with the given description if one does not exist.
func (transcript *Transcript) Subscript(transcriptable Transcriptable) *Transcript {
	if transcript == nil {
		return nil
	}
	subscript, exists := transcript.Subscripts[transcriptable]
	if !exists {
		composite, name := transcriptable.Composite()
		if !composite {
			name = transcriptable.String()
		}
		subscript = NewTranscript(name)
		transcript.Subscripts[transcriptable] = subscript
	}
	return subscript
}

// Copy creates a deep copy of the transcript and all sub-transcripts, but with a shallow copy of all
// the transcriptables.
func (transcript *Transcript) Copy() *Transcript {
	if transcript == nil {
		return nil
	}
	subscripts := make(map[Transcriptable]*Transcript, len(transcript.Subscripts))
	for transcriptable, subscript := range transcript.Subscripts {
		subscripts[transcriptable] = subscript.Copy()
	}
	return &Transcript{
		Requirement:  transcript.Requirement,
		GroupsPassed: transcript.GroupsPassed,
		GroupsFailed: transcript.GroupsFailed,
		Subscripts:   subscripts,
	}
}

// Add will add the other transcript into this transcript by merging them.
func (transcript *Transcript) Add(other *Transcript) {
	if transcript == nil || other == nil {
		return
	}
	if transcript.Requirement == other.Requirement {
		transcript.GroupsPassed += other.GroupsPassed
		transcript.GroupsFailed += other.GroupsFailed
	}
	for transcriptable, subScriptToAdd := range other.Subscripts {
		subScript := transcript.Subscript(transcriptable)
		subScript.Add(subScriptToAdd)
	}
}

func (transcript *Transcript) string(indent int) string {
	space := strings.Repeat(" ", indent)
	result := fmt.Sprintf("%v%v passed %v times and failed %v times\n",
		space, transcript.Requirement, transcript.GroupsPassed, transcript.GroupsFailed)
	for _, subTranscript := range transcript.Subscripts {
		result += subTranscript.string(indent + 1)
	}
	return result
}

// String will create a human readable representation of the transcript.
func (transcript *Transcript) String() string {
	return transcript.string(0)
}
