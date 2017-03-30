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
	if contentType == ContentTypeJson {
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

	if contentType == ContentTypeJson {
		return json.NewDecoder(bytes.NewReader(data)).
			Decode(event.Interface())
	} else if contentType == ContentTypeProtobuf {
		return proto.Unmarshal(data, event.Interface().(proto.Message))
	}
	return fmt.Errorf("Unknown contentType %v", contentType)
}
