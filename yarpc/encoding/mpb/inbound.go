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
	"fmt"
	"reflect"

	"context"

	"go.uber.org/yarpc/api/transport"
)

const _getTypeMethod = "GetType"

var _invalidMethod = reflect.Value{}

// EventType is an interface for auto-generated event types from
// Mesos HTTP API such as mesos/v1/scheduler.proto
type EventType interface {
	String() string
}

// MesosEvent is an interface for auto-generated events from Mesos HTTP API
// such as mesos/v1/scheduler.proto
type MesosEvent interface {
	GetType() EventType
}

// MesosEventReader decodes a Mesos Event object from RecordIO frame
type MesosEventReader struct {
	Event reflect.Value // Decoded Mesos event
	Type  EventType     // Mesos event type such as offers, rescind etc
}

// NewMesosEventReader creates a new MesosEventReader
func NewMesosEventReader(data []byte, typ reflect.Type, contentType string) (
	*MesosEventReader, error) {

	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("Wrong mesos event type: %s", typ)
	}

	// Decode the RecordIO frame to Protobuf event
	event := reflect.New(typ)
	if err := UnmarshalPbMessage(data, event, contentType); err != nil {
		return nil, err
	}

	// Get the event type of the Mesos event
	method := event.MethodByName(_getTypeMethod)
	if method == _invalidMethod {
		return nil, fmt.Errorf(
			"Event object does not have method %s",
			_getTypeMethod)
	}
	result := method.Call([]reflect.Value{})[0]
	eventType, ok := result.Interface().(EventType)
	if !ok {
		return nil, fmt.Errorf(
			"Result %v is not of event type %v", result, eventType)
	}

	// TODO: Decode the nested event object using reflect.MethodByName
	reader := &MesosEventReader{Event: event, Type: eventType}
	return reader, nil
}

func (r MesosEventReader) Read(p []byte) (_ int, _ error) {
	// TODO: make Request.Body in YARPC to be interface{}
	// instead of io.Reader
	panic("mesosEventReader does not support Read method")
}

// mpbHandler adapts a Mesos event handler into a transport-level
// Handler.
//
// The wrapped function must already be in the correct format:
//
// 	f(ctx context.Context, body $reqBody) error
type mpbHandler struct {
	handler reflect.Value
}

func (h mpbHandler) Handle(
	ctx context.Context,
	treq *transport.Request,
	rw transport.ResponseWriter) error {

	reader := treq.Body.(*MesosEventReader)
	results := h.handler.Call([]reflect.Value{reflect.ValueOf(ctx), reader.Event})

	if err := results[0].Interface(); err != nil {
		return err.(error)
	}

	return nil
}
