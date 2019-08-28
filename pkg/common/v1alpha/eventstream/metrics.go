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

package eventstream

import "github.com/uber-go/tally"

// HandlerMetrics is the metrics for event stream handler
type HandlerMetrics struct {
	Head     tally.Gauge
	Tail     tally.Gauge
	Size     tally.Gauge
	Capacity tally.Gauge

	UnexpectedClientError tally.Counter
	PurgeEventError       tally.Counter
	InvalidStreamIDError  tally.Counter

	AddEventAPI          tally.Counter
	AddEventSuccess      tally.Counter
	AddEventDeDupe       tally.Counter
	AddEventFail         tally.Counter
	InitStreamAPI        tally.Counter
	InitStreamSuccess    tally.Counter
	InitStreamFail       tally.Counter
	WaitForEventsAPI     tally.Counter
	WaitForEventsSuccess tally.Counter
	WaitForEventsFailed  tally.Counter
}

// NewHandlerMetrics creates a HandlerMetrics
func NewHandlerMetrics(scope tally.Scope) *HandlerMetrics {
	handlerAPIScope := scope.SubScope("api")
	handlerSuccessScope := scope.Tagged(map[string]string{"result": "success"})
	handlerFailScope := scope.Tagged(map[string]string{"result": "fail"})
	return &HandlerMetrics{
		Head:                  scope.Gauge("head"),
		Tail:                  scope.Gauge("tail"),
		Size:                  scope.Gauge("size"),
		Capacity:              scope.Gauge("capacity"),
		UnexpectedClientError: scope.Counter("unexpected_client_error"),
		PurgeEventError:       scope.Counter("purge_event_error"),
		InvalidStreamIDError:  scope.Counter("invalid_streamid_error"),
		AddEventAPI:           handlerAPIScope.Counter("add_event"),
		AddEventSuccess:       handlerSuccessScope.Counter("add_event"),
		AddEventDeDupe:        handlerSuccessScope.Counter("add_event_dedupe"),
		AddEventFail:          handlerFailScope.Counter("add_event"),
		InitStreamAPI:         handlerAPIScope.Counter("init_stream"),
		InitStreamSuccess:     handlerSuccessScope.Counter("init_stream"),
		InitStreamFail:        handlerFailScope.Counter("init_stream"),
		WaitForEventsAPI:      handlerAPIScope.Counter("wait_for_events"),
		WaitForEventsSuccess:  handlerSuccessScope.Counter("wait_for_events"),
		WaitForEventsFailed:   handlerFailScope.Counter("wait_for_events"),
	}
}

// ClientMetrics is the metrics for event stream client
type ClientMetrics struct {
	EventsConsumed tally.Counter
	StreamIDChange tally.Counter
	PurgeOffset    tally.Gauge

	InitStreamAPI        tally.Counter
	InitStreamSuccess    tally.Counter
	InitStreamFail       tally.Counter
	WaitForEventsAPI     tally.Counter
	WaitForEventsSuccess tally.Counter
	WaitForEventsFailed  tally.Counter
}

// NewClientMetrics creates a new ClientMetrics
func NewClientMetrics(scope tally.Scope) *ClientMetrics {
	clientAPIScope := scope.SubScope("v1alpha_api")
	clientSuccessScope := scope.Tagged(map[string]string{"result": "success"})
	clientFailScope := scope.Tagged(map[string]string{"result": "fail"})
	return &ClientMetrics{
		EventsConsumed:       scope.Counter("events_consumed"),
		StreamIDChange:       scope.Counter("stream_id_change"),
		PurgeOffset:          scope.Gauge("purge_offset"),
		InitStreamAPI:        clientAPIScope.Counter("init_stream"),
		InitStreamSuccess:    clientSuccessScope.Counter("init_stream"),
		InitStreamFail:       clientFailScope.Counter("init_stream"),
		WaitForEventsAPI:     clientAPIScope.Counter("wait_for_events"),
		WaitForEventsSuccess: clientSuccessScope.Counter("wait_for_events"),
		WaitForEventsFailed:  clientFailScope.Counter("wait_for_events"),
	}
}
