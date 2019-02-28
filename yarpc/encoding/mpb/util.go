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

package mpb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
)

// MarshalPbMessage marshal a protobuf message to string based on given
// content type.
func MarshalPbMessage(msg proto.Message, contentType string) (string, error) {
	if contentType == ContentTypeJSON {
		encoder := jsonpb.Marshaler{
			EnumsAsInts: false,
			OrigName:    true,
		}
		body, err := encoder.MarshalToString(msg)
		if err != nil {
			return "", fmt.Errorf(
				"Failed to marshal subscribe call to %v: %v",
				contentType,
				err)
		}
		return body, nil
	} else if contentType == ContentTypeProtobuf {
		body, err := proto.Marshal(msg)
		if err != nil {
			return "", fmt.Errorf(
				"Failed to marshal subscribe call to %v: %v",
				contentType,
				err)
		}
		return string(body), nil
	}
	return "", fmt.Errorf("Unsupported contentType %v", contentType)
}

// UnmarshalPbMessage unmarshals a protobuf message from a string based on given
// content type.
func UnmarshalPbMessage(
	data []byte, event reflect.Value, contentType string) error {

	if contentType == ContentTypeJSON {
		return json.NewDecoder(bytes.NewReader(data)).
			Decode(event.Interface())
	} else if contentType == ContentTypeProtobuf {
		return proto.Unmarshal(data, event.Interface().(proto.Message))
	}
	return fmt.Errorf("Unknown contentType %v", contentType)
}
