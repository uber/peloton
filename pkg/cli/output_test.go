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

package cli

import (
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

var respose = &job.GetResponse{
	JobInfo: &job.JobInfo{
		Id: &peloton.JobID{
			Value: testJobID,
		},
		Config: &job.JobConfig{
			Name:          "test job",
			Description:   "test job",
			OwningTeam:    "test team",
			InstanceCount: 1,
		},
	},
}

func TestMarshallResponse(t *testing.T) {

	ret, err := marshallResponse(jsonResponseFormat, respose)
	assert.Nil(t, err)
	expected := "{\n  \"jobInfo\": {\n    \"config\": {\n" +
		"      \"description\": \"test job\",\n      \"instanceCount\": 1,\n " +
		"     \"name\": \"test job\",\n      \"owner\": \"\",\n   " +
		"   \"owningTeam\": \"test team\",\n      " +
		"\"placementStrategy\": \"PLACEMENT_STRATEGY_INVALID\",\n      " +
		"\"type\": \"BATCH\"\n   " +
		" },\n    \"id\": {\n      \"value\": \"481d565e-28da-457d-8434-f6bb7faa0e95\"\n   " +
		" }\n  }\n}"
	assert.Equal(t, expected, string(ret))

	ret, err = marshallResponse(defaultResponseFormat, respose)
	assert.Nil(t, err)
	expected = "jobInfo:\n  config:\n    description: test job\n    " +
		"instanceCount: 1\n    name: test job\n    owner: \"\"\n    " +
		"owningTeam: test team\n" +
		"    placementStrategy: PLACEMENT_STRATEGY_INVALID\n" +
		"    type: BATCH\n  id:\n    " +
		"value: 481d565e-28da-457d-8434-f6bb7faa0e95\n"
	assert.Equal(t, expected, string(ret))
}

type errEncoderDecoer struct {
	encErr error
	decErr error
}

func (ee errEncoderDecoer) MarshalToString(pb proto.Message) (string, error) {
	return "", ee.encErr
}

func (ee errEncoderDecoer) Unmarshal(data []byte, v interface{}) error {
	return ee.decErr
}

func (ee errEncoderDecoer) MarshalIndent(v interface{}, prefix,
	indent string) ([]byte, error) {
	return nil, ee.encErr
}

func TestMarshallResponseError(t *testing.T) {
	fakeErr := errors.New("fake error")
	ee := errEncoderDecoer{encErr: fakeErr}
	cliEncoder = ee
	ret, err := marshallResponse(jsonResponseFormat, respose)
	assert.Nil(t, ret, "response should ne bil")
	assert.EqualError(t, err, "Failed to marshal response : fake error")
}

func TestUnMarshallResponseError(t *testing.T) {
	fakeErr := errors.New("fake error")
	ee := errEncoderDecoer{decErr: fakeErr}
	cliEncoder = ee
	ret, err := marshallResponse(jsonResponseFormat, respose)
	assert.Nil(t, ret, "response should ne bil")
	assert.EqualError(t, err, "Failed to unmarshal response : fake error")
}

type fakeOutputter struct{ Out string }

func (fo *fakeOutputter) output(l string) {
	fo.Out = l
}

func TestErrPrintResponseJSON(t *testing.T) {
	fakeErr := errors.New("fake error")
	ee := errEncoderDecoer{encErr: fakeErr}
	cliEncoder = ee
	cliOutPutter = &fakeOutputter{}

	fo := cliOutPutter.(*fakeOutputter)
	printResponseJSON(respose)
	assert.Equal(t, fo.Out, "MarshalIndent err=fake error\n")
}

func TestPrintResponseJSON(t *testing.T) {
	cliEncoder = newJSONEncoderDecoder()
	cliOutPutter = &fakeOutputter{}

	fo := cliOutPutter.(*fakeOutputter)
	printResponseJSON(respose)
	assert.Equal(t, fo.Out, "{\n  \"jobInfo\": {\n    \"id\": {\n "+
		"     \"value\": \"481d565e-28da-457d-8434-f6bb7faa0e95\"\n    "+
		"},\n    \"config\": {\n      \"name\": \"test job\",\n      "+
		"\"owningTeam\": \"test team\",\n      \"description\": \"test job\",\n"+
		"      \"instanceCount\": 1\n    }\n  }\n}\n")
}
