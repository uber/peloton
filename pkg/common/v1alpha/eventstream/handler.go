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
	"math"
	"sync"

	pbevent "github.com/uber/peloton/.gen/peloton/private/eventstream/v1alpha/event"
	pbeventstreamsvc "github.com/uber/peloton/.gen/peloton/private/eventstream/v1alpha/eventstreamsvc"
	"github.com/uber/peloton/pkg/common/cirbuf"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

// PurgedEventsProcessor is the interface to handle the purged data.
type PurgedEventsProcessor interface {
	EventPurged(events []*cirbuf.CircularBufferItem)
}

// Handler holds a circular buffer and serves request to pull data.
// This component is used in hostmgr and resmgr.
type Handler struct {
	mu sync.RWMutex

	// The stream id of the event stream.
	streamID string

	// Clients that the stream expects them to consume.
	expectedClients []string

	circularBuffer *cirbuf.CircularBuffer

	// Tracks the purge offset per client.
	clientPurgeOffsets map[string]uint64

	purgedEventsProcessor PurgedEventsProcessor

	eventIndex map[string]struct{}

	metrics *HandlerMetrics
}

// NewEventStreamHandler creates an EventStreamHandler.
func NewEventStreamHandler(
	bufferSize int,
	expectedClients []string,
	purgedEventsProcessor PurgedEventsProcessor,
	parentScope tally.Scope,
) *Handler {
	scope := parentScope.SubScope("v1alpha_eventstream_handler")

	handler := Handler{
		streamID:              uuid.New(),
		circularBuffer:        cirbuf.NewCircularBuffer(bufferSize),
		clientPurgeOffsets:    make(map[string]uint64),
		purgedEventsProcessor: purgedEventsProcessor,
		expectedClients:       expectedClients,
		eventIndex:            make(map[string]struct{}),
		metrics:               NewHandlerMetrics(scope),
	}
	handler.metrics.Capacity.Update(float64(handler.circularBuffer.Capacity()))
	for _, client := range expectedClients {
		handler.clientPurgeOffsets[client] = 0
	}
	return &handler
}

// Check if the client is expected.
func (h *Handler) isClientExpected(
	clientName string,
) bool {
	for _, ok := h.clientPurgeOffsets[clientName]; ok; {
		return true
	}
	log.WithField("client_name", clientName).Error("Client not supported")
	h.metrics.UnexpectedClientError.Inc(1)
	return false
}

func (h *Handler) isDupEvent(e *pbevent.Event) bool {
	eventId := e.GetEventId()
	if eventId != "" {
		if _, ok := h.eventIndex[eventId]; ok {
			return true
		}
	}
	return false
}

func (h *Handler) addEventToDedupMap(e *pbevent.Event) {
	eventId := e.GetEventId()
	if eventId != "" {
		h.eventIndex[eventId] = struct{}{}
	}
}

// AddEvent adds a task Event or mesos status update into the inner circular
// buffer.
func (h *Handler) AddEvent(e *pbevent.Event) error {
	if e == nil {
		return yarpcerrors.InvalidArgumentErrorf("event is nil")
	}
	h.metrics.AddEventAPI.Inc(1)
	log.WithField("event", e).Debug("Adding event to eventstream")

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.isDupEvent(e) {
		h.metrics.AddEventDeDupe.Inc(1)
		return nil
	}

	item, err := h.circularBuffer.AddItem(e)
	if err != nil {
		h.metrics.AddEventFail.Inc(1)
		return err
	}

	h.metrics.AddEventSuccess.Inc(1)
	h.addEventToDedupMap(e)

	head, tail := h.circularBuffer.GetRange()
	h.metrics.Head.Update(float64(head))
	h.metrics.Tail.Update(float64(tail))
	h.metrics.Size.Update(float64(head - tail))
	log.WithField("current_head", item.SequenceID).Debug("Event added")
	return nil
}

// GetEvents returns all the events pending in circular buffer
// This method is primarily for debugging purpose
func (h *Handler) GetEvents() ([]*pbevent.Event, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Get and return data
	head, tail := h.circularBuffer.GetRange()
	items, err := h.circularBuffer.GetItemsByRange(tail, head)
	if err != nil {
		return nil, err
	}

	events := make([]*pbevent.Event, 0, len(items))
	for _, item := range items {
		if e, ok := item.Value.(*pbevent.Event); ok {
			events = append(events, e)
		}
	}
	return events, nil
}

// InitStream handles the initstream request.
func (h *Handler) InitStream(
	ctx context.Context,
	req *pbeventstreamsvc.InitStreamRequest,
) (*pbeventstreamsvc.InitStreamResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	log.WithField("request", req).Debug("InitStream request")
	h.metrics.InitStreamAPI.Inc(1)

	clientName := req.GetClientName()
	if !h.isClientExpected(clientName) {
		h.metrics.InitStreamFail.Inc(1)
		return nil, yarpcerrors.InvalidArgumentErrorf("Client %v not supported, valid clients : %v",
			clientName, h.expectedClients)
	}

	_, tail := h.circularBuffer.GetRange()
	response := pbeventstreamsvc.InitStreamResponse{
		StreamId:            h.streamID,
		MinOffset:           tail,
		PreviousPurgeOffset: h.clientPurgeOffsets[clientName],
	}
	log.WithField("response", response).Debug("InitStream")
	h.metrics.InitStreamSuccess.Inc(1)

	return &response, nil
}

// WaitForEvents handles the WaitForEvents request.
func (h *Handler) WaitForEvents(
	ctx context.Context,
	req *pbeventstreamsvc.WaitForEventsRequest,
) (*pbeventstreamsvc.WaitForEventsResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.metrics.WaitForEventsAPI.Inc(1)

	// Validate client.
	clientName := req.GetClientName()
	if !h.isClientExpected(clientName) {
		h.metrics.WaitForEventsFailed.Inc(1)
		return nil, yarpcerrors.InvalidArgumentErrorf("Client %v not supported, valid clients : %v",
			clientName, h.expectedClients)
	}

	// Validate stream id.
	streamID := req.GetStreamId()
	if streamID != h.streamID {
		log.WithFields(log.Fields{
			"request_streamID": streamID,
			"client_name":      clientName,
		}).Warn("Invalid streamID")
		h.metrics.WaitForEventsFailed.Inc(1)
		h.metrics.InvalidStreamIDError.Inc(1)
		return nil, yarpcerrors.InvalidArgumentErrorf("Invalid streamID, current streamID is %s",
			h.streamID)
	}

	// Validate purge offset.
	head, tail := h.circularBuffer.GetRange()
	if req.GetPurgeOffset() > req.GetBeginOffset() {
		log.WithFields(log.Fields{
			"purge_offset": req.GetPurgeOffset(),
			"begin_offset": req.GetBeginOffset(),
		}).Error("Invalid purgeOffset")
		return nil, yarpcerrors.InvalidArgumentErrorf("Invalid PurgeOffset %d after BeginOffset %d",
			req.GetPurgeOffset(), req.GetBeginOffset())
	}

	// Get and return data.
	beginOffset := req.GetBeginOffset()
	limit := req.GetLimit()
	log.WithFields(log.Fields{
		"head":         head,
		"tail":         tail,
		"begin_offset": beginOffset,
		"limit":        limit,
	}).Debug("getitemsbyrange")
	items, err := h.circularBuffer.GetItemsByRange(beginOffset, beginOffset+uint64(limit)-1)
	if err != nil {
		h.metrics.WaitForEventsFailed.Inc(1)
		return nil, yarpcerrors.OutOfRangeErrorf("BeginOffset %d is out of range [%d, %d]",
			beginOffset, tail, head-1)
	}
	if len(items) > 0 {
		log.WithField("number", len(items)).Debug("got events")
	}

	var events []*pbevent.Event
	for i, item := range items {
		if pe, ok := item.Value.(*pbevent.Event); ok {
			log.WithFields(log.Fields{
				"index":  i,
				"offset": item.SequenceID,
			}).Debug("event")
			e := &pbevent.Event{
				Offset:   item.SequenceID,
				PodEvent: pe.PodEvent,
			}
			events = append(events, e)
		}
	}
	h.metrics.WaitForEventsSuccess.Inc(1)

	h.purgeEvents(clientName, req.GetPurgeOffset())
	return &pbeventstreamsvc.WaitForEventsResponse{Events: events}, nil
}

// purgeData scans the min of the purgeOffset for each client, and move the
// buffer tail to the minPurgeOffset.
func (h *Handler) purgeEvents(
	clientName string,
	purgeOffset uint64,
) {
	var minPurgeOffset uint64
	var clientWithMinPurgeOffset string

	h.clientPurgeOffsets[clientName] = purgeOffset
	minPurgeOffset = math.MaxUint64
	for c, p := range h.clientPurgeOffsets {
		if minPurgeOffset > p {
			minPurgeOffset = p
			clientWithMinPurgeOffset = c
		}
	}

	head, tail := h.circularBuffer.GetRange()
	if minPurgeOffset >= tail && minPurgeOffset <= head {
		purgedItems, err := h.circularBuffer.MoveTail(minPurgeOffset)
		if err != nil {
			log.WithField("min_purge_offset", minPurgeOffset).Error("Invalid minPurgeOffset")
			h.metrics.PurgeEventError.Inc(1)
		} else {
			for _, item := range purgedItems {
				if event, ok := item.Value.(*pbevent.Event); ok {
					delete(h.eventIndex, event.EventId)
				}
			}
			if h.purgedEventsProcessor != nil {
				h.purgedEventsProcessor.EventPurged(purgedItems)
			}
		}
	} else {
		log.WithFields(log.Fields{
			"min_purge_offset":             minPurgeOffset,
			"client_with_min_purge_offset": clientWithMinPurgeOffset,
			"tail":                         tail,
			"head":                         head,
		}).Error("minPurgeOffset incorrect")
		h.metrics.PurgeEventError.Inc(1)
	}

	h.metrics.Tail.Update(float64(tail))
	h.metrics.Size.Update(float64(head - tail))
}

// NewTestHandler returns an empty new Handler ptr for testing.
func NewTestHandler() *Handler {
	return &Handler{}
}
