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
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"gopkg.in/yaml.v2"
)

var (
	tabWriter = tabwriter.NewWriter(
		os.Stdout, 0, 0, 2, ' ', tabwriter.AlignRight|tabwriter.Debug,
	)
)

// used for testing
type encoderDecoder interface {
	MarshalToString(pb proto.Message) (string, error)
	MarshalIndent(v interface{}, prefix, indent string) ([]byte, error)
	Unmarshal(data []byte, v interface{}) error
}

type jsonEncoderDecoder struct {
	encoder jsonpb.Marshaler
}

func (ed jsonEncoderDecoder) MarshalToString(pb proto.Message) (string, error) {
	return ed.encoder.MarshalToString(pb)
}

func (ed jsonEncoderDecoder) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (ed jsonEncoderDecoder) MarshalIndent(v interface{}, prefix,
	indent string) ([]byte, error) {
	return json.MarshalIndent(v, prefix, indent)
}

func newJSONEncoderDecoder() encoderDecoder {
	return jsonEncoderDecoder{
		encoder:
		// use jsonpb encoder to convert enum int into string
		jsonpb.Marshaler{
			EnumsAsInts:  false,
			OrigName:     true,
			EmitDefaults: true,
		}}
}

type outputter interface {
	output(string)
}

type stdOutPutputter struct{}

func (o stdOutPutputter) output(l string) { fmt.Printf(l) }
func newStdOutOutputter() outputter       { return stdOutPutputter{} }

var (
	cliEncoder   = newJSONEncoderDecoder()
	cliOutPutter = newStdOutOutputter()
)

func printResponseJSON(response interface{}) {
	buffer, err := cliEncoder.MarshalIndent(response, "", "  ")
	if err == nil {
		cliOutPutter.output(fmt.Sprintf("%v\n", string(buffer)))
	} else {
		cliOutPutter.output(fmt.Sprintf("MarshalIndent err=%v\n", err))
	}
}

// Marshall a pb message response in the desired format
func marshallResponse(
	format string,
	message proto.Message) ([]byte, error) {

	body, err := cliEncoder.MarshalToString(message)
	if err != nil {
		return nil, fmt.Errorf(
			"Failed to marshal response : %v",
			err)
	}

	byt := []byte(body)
	var dat map[string]interface{}
	if err := cliEncoder.Unmarshal(byt, &dat); err != nil {
		return nil, fmt.Errorf(
			"Failed to unmarshal response : %v",
			err)
	}

	switch strings.ToLower(format) {
	case "yaml", "yml":
		return yaml.Marshal(dat)
	case "json":
		return json.MarshalIndent(
			dat,
			"",
			"  ")
	default:
		return nil, fmt.Errorf("Invalid format %s", format)
	}
}
