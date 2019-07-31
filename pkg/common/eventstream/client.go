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

import (
	"context"
	"time"

	pbeventstream "github.com/uber/peloton/.gen/peloton/private/eventstream"

	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/common/metrics"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
)

var (
	// TODO: move these into config, if necessary
	maxEventSize    = 100
	requestTimeout  = 10 * time.Second
	errorRetrySleep = 10 * time.Second
	noEventSleep    = 100 * time.Millisecond
)

// EventHandler is the interface for handling task update events
type EventHandler interface {
	// The event notification callback
	OnV0Event(event *pbeventstream.Event)

	// The events notification callback
	OnV0Events(events []*pbeventstream.Event)

	// Returns the event progress the handler has processed. The value
	// will be used by the client to determine the purgeOffset
	GetEventProgress() uint64
}

// Client is the event stream client
type Client struct {
	// The rpc client to pull events from event stream handler
	rpcClient pbeventstream.EventStreamServiceYARPCClient
	// the client name
	clientName string
	// the stream id of the event stream
	streamID string
	// previousSeverPurgeOffset is the purge offset
	// stores on the handler for the client when client inits
	previousSeverPurgeOffset uint64
	// beginOffset of the next pull event request
	beginOffset uint64
	// event purge offset to be send to the event stream handler
	purgeOffset uint64
	// event handler interface to process the received events
	eventHandler EventHandler
	// log.Entry used by client to share common log fields
	log *log.Entry

	lifeCycle lifecycle.LifeCycle

	metrics *ClientMetrics
}

// NewEventStreamClient creates a client that
// consumes from remote event stream handler
func NewEventStreamClient(
	d *yarpc.Dispatcher,
	clientName string,
	server string,
	taskUpdateHandler EventHandler,
	parentScope tally.Scope,
) *Client {
	client := &Client{
		clientName:   clientName,
		rpcClient:    pbeventstream.NewEventStreamServiceYARPCClient(d.ClientConfig(server)),
		eventHandler: taskUpdateHandler,
		lifeCycle:    lifecycle.NewLifeCycle(),
		metrics:      NewClientMetrics(parentScope.SubScope(metrics.SafeScopeName(clientName))),
		log: log.WithFields(log.Fields{
			"client": clientName,
			"server": server,
		}),
	}
	return client
}

// NewLocalEventStreamClient creates a local client that
// directly consumes from a local event stream handler
func NewLocalEventStreamClient(
	clientName string,
	handler *Handler,
	taskUpdateHandler EventHandler,
	parentScope tally.Scope,
) *Client {
	client := &Client{
		clientName:   clientName,
		rpcClient:    newLocalClient(handler),
		eventHandler: taskUpdateHandler,
		metrics:      NewClientMetrics(parentScope.SubScope(metrics.SafeScopeName(clientName))),
		lifeCycle:    lifecycle.NewLifeCycle(),
		log: log.WithFields(log.Fields{
			"client": clientName,
			"server": "local",
		}),
	}
	client.Start()
	return client
}

func newLocalClient(h *Handler) pbeventstream.EventStreamServiceYARPCClient {
	return &localClient{
		handler: h,
	}
}

// Local client implements EventStreamService client interface. It is a client
// adaptor on an event stream handler, it takes a event stream
// handler and consume event from it. Events from HM->RM would need this.
type localClient struct {
	handler *Handler
}

// InitStream forwards the call to the handler, dropping the options.
func (c *localClient) InitStream(
	ctx context.Context,
	request *pbeventstream.InitStreamRequest,
	opts ...yarpc.CallOption) (*pbeventstream.InitStreamResponse, error) {
	return c.handler.InitStream(ctx, request)
}

// WaitForEvents forwards the call to the handler, dropping the options.
func (c *localClient) WaitForEvents(
	ctx context.Context,
	request *pbeventstream.WaitForEventsRequest,
	opts ...yarpc.CallOption) (*pbeventstream.WaitForEventsResponse, error) {
	return c.handler.WaitForEvents(ctx, request)
}

func (c *Client) sendInitStreamRequest(clientName string) (*pbeventstream.InitStreamResponse, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), requestTimeout)
	defer cancelFunc()
	c.metrics.InitStreamAPI.Inc(1)
	request := &pbeventstream.InitStreamRequest{
		ClientName: clientName,
	}
	response, err := c.rpcClient.InitStream(ctx, request)
	if err != nil || response.Error != nil {
		c.log.WithError(err).Error("sendInitStreamRequest failed")
		c.metrics.InitStreamFail.Inc(1)
		return response, err
	}
	c.metrics.InitStreamSuccess.Inc(1)
	c.metrics.StreamIDChange.Inc(1)
	c.log.WithField("InitStreamResponse", response).Infoln()
	return response, nil
}

func (c *Client) initStream(clientName string, stopCh <-chan struct{}) {
	for {
		select {
		case <-stopCh:
			c.log.Info("initStream returned due to shutdown")
			return
		default:
			response, err := c.sendInitStreamRequest(clientName)
			if err != nil {
				c.log.WithError(err).Error("sendInitStreamRequest failed")
				time.Sleep(errorRetrySleep)
				continue
			}
			if response.Error != nil {
				c.log.WithField("error", response.Error).Error("sendInitStreamRequest failed")
				time.Sleep(errorRetrySleep)
				continue
			}
			c.streamID = response.StreamID
			c.beginOffset = response.PreviousPurgeOffset
			c.previousSeverPurgeOffset = response.PreviousPurgeOffset
			if response.PreviousPurgeOffset < response.MinOffset {
				c.log.WithField("previous_purge_offset", response.PreviousPurgeOffset).
					WithField("min_offset", response.MinOffset).
					Error("Need to adjust beginOffset")
				c.beginOffset = response.MinOffset
			}
			return
		}
	}
}

func (c *Client) sendWaitEventRequest(
	beginOffset uint64,
	purgeOffset uint64) (*pbeventstream.WaitForEventsResponse, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), requestTimeout)
	defer cancelFunc()

	// We need to make this adjust for the first event,
	// where c.eventHandler.GetEventProgress() and BeginOffset are both 0
	purgeOffset = c.eventHandler.GetEventProgress() + 1
	if purgeOffset > beginOffset {
		purgeOffset = beginOffset
	}
	// This could happen before client processes the first event
	// where c.eventHandler.GetEventProgress() returns 0.
	if c.previousSeverPurgeOffset > purgeOffset {
		purgeOffset = c.previousSeverPurgeOffset
	}
	c.metrics.WaitForEventsAPI.Inc(1)
	c.metrics.PurgeOffset.Update(float64(purgeOffset))
	request := &pbeventstream.WaitForEventsRequest{
		BeginOffset: beginOffset,
		// purgeOffset are used to move the circular buffer tail, thus plus 1
		PurgeOffset: purgeOffset,
		StreamID:    c.streamID,
		ClientName:  c.clientName,
		Limit:       int32(maxEventSize),
	}
	response, err := c.rpcClient.WaitForEvents(ctx, request)
	if err != nil {
		c.log.WithError(err).Error("sendWaitForEventsRequest failed")
		c.metrics.WaitForEventsFailed.Inc(1)
		return nil, err
	}
	c.metrics.WaitForEventsSuccess.Inc(1)
	return response, nil
}

func (c *Client) waitEventsLoop(stopCh <-chan struct{}) {
	for {
		select {
		case <-stopCh:
			c.log.Info("waitEventsLoop returned due to shutdown")
			return
		default:
			c.purgeOffset = c.beginOffset
			response, err := c.sendWaitEventRequest(c.beginOffset, c.purgeOffset)
			// Retry in case there is RPC error
			if err != nil {
				c.log.WithError(err).Error("sendWaitEventRequest failed")
				time.Sleep(errorRetrySleep)
				continue
			}
			if response.GetError() != nil {
				c.log.WithField("error", response.Error).Error("sendWaitEventRequest returns error")
				// If client is unsupported / streamID is invalid, Return and the outside loop should call InitStream
				// again
				if response.Error.GetClientUnsupported() != nil || response.Error.GetInvalidStreamID() != nil {
					return
				}
				// Note: InvalidPurgeOffset should never happen if the client does the right thing. For now, just log it
			}
			if len(response.GetEvents()) == 0 {
				time.Sleep(noEventSleep)
				continue
			}

			for _, event := range response.Events {
				c.log.WithField("event_offset", event.GetOffset()).Debug("Processing event")
				c.eventHandler.OnV0Event(event)
				c.beginOffset = event.GetOffset() + 1
			}
			c.eventHandler.OnV0Events(response.Events)
			c.metrics.EventsConsumed.Inc(int64(len(response.Events)))
		}
	}
}

// Start starts the client
func (c *Client) Start() {
	if !c.lifeCycle.Start() {
		return
	}

	c.log.Info("Event stream client start() called")
	stopCh := c.lifeCycle.StopCh()
	go func() {
		for {
			select {
			case <-stopCh:
				c.log.Info("Event stream client exits wait events")
				c.lifeCycle.StopComplete()
				return
			default:
				c.initStream(c.clientName, stopCh)
				c.waitEventsLoop(stopCh)
			}
		}
	}()
	c.log.Info("Event stream client started")
}

// Stop stops the client
func (c *Client) Stop() {
	if !c.lifeCycle.Stop() {
		return
	}

	c.log.Info("Event stream client stop() called")
	c.lifeCycle.Wait()
	// wait until the event consuming go routine returns
	c.log.Info("Event stream client stopped")
}
