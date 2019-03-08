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
	"strings"
	"time"

	"go.uber.org/yarpc/api/transport"
	"golang.org/x/net/context"

	"github.com/uber/peloton/.gen/mesos/v1/scheduler"
)

// SchedulerClient makes Mesos JSON requests to Mesos endpoint
type SchedulerClient interface {
	// Call performs an outbound Mesos JSON request.
	// Returns an error if the request failed.
	Call(mesosStreamID string, msg *mesos_v1_scheduler.Call) error
}

// NewSchedulerClient builds a new Mesos Scheduler JSON client.
func NewSchedulerClient(c transport.ClientConfig, contentType string) SchedulerClient {
	return &schedulerClient{
		cfg:         c,
		contentType: contentType,
	}
}

type schedulerClient struct {
	cfg         transport.ClientConfig
	contentType string
}

func (c *schedulerClient) Call(mesosStreamID string, msg *mesos_v1_scheduler.Call) error {
	headers := transport.NewHeaders().
		With("Mesos-Stream-Id", mesosStreamID).
		With("Content-Type", fmt.Sprintf("application/%s", c.contentType)).
		With("Accept", fmt.Sprintf("application/%s", c.contentType))

	body, err := MarshalPbMessage(msg, c.contentType)
	if err != nil {
		return fmt.Errorf(
			"failed to marshal subscribe call to contentType %s %v",
			c.contentType,
			err,
		)
	}

	treq := transport.Request{
		Caller:    c.cfg.Caller(),
		Service:   c.cfg.Service(),
		Encoding:  Encoding,
		Procedure: "Scheduler_Call",
		Headers:   transport.Headers(headers),
		Body:      strings.NewReader(body),
	}

	ctx, _ := context.WithTimeout(context.Background(), 100*1000*time.Millisecond)
	_, err = c.cfg.GetUnaryOutbound().Call(ctx, &treq)

	// All Mesos calls are one-way so no need to decode response body
	return err
}
