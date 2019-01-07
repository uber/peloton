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

package mhttp

import (
	"context"
	"net/http"
	"net/url"
	"reflect"
)

// MesosDriver is an interface used by Inbound to subscribe to a Mesos
// service endpoint such as scheduler, executor, master etc.
type MesosDriver interface {
	// Returns the name of Mesos driver such as scheduler or executor
	Name() string

	// Returns the Mesos endpoint to be connected to
	Endpoint() url.URL

	// Returns the Type of Mesos event message such as
	// mesos.v1.scheduler.Event or mesos.v1.executor.Event
	EventDataType() reflect.Type

	// Returns a subscribe Call message to be sent to Mesos for
	// setting up an event stream connection
	PrepareSubscribeRequest(ctx context.Context, mesosMasterHostPort string) (*http.Request, error)

	// Invoked after the subscription to Mesos is done
	PostSubscribe(ctx context.Context, mesosStreamID string)

	// GetContentEncoding returns the http content encoding of the Mesos
	// HTTP traffic
	GetContentEncoding() string
}
