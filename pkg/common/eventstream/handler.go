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
	"fmt"
	"math"
	"sync"

	pb_eventstream "github.com/uber/peloton/.gen/peloton/private/eventstream"

	"github.com/uber/peloton/pkg/common/cirbuf"

	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

// PurgedEventsProcessor is the interface to handle the purged data
type PurgedEventsProcessor interface {
	EventPurged(events []*cirbuf.CircularBufferItem)
}

// Handler holds a circular buffer and serves request to pull data.
// This component is used in hostmgr and resmgr
type Handler struct {
	sync.RWMutex
	// streamID is created to identify this stream lifecycle
	streamID string
	// clients that the stream expects them to consume
	expectedClients []string
	circularBuffer  *cirbuf.CircularBuffer
	// uuid of events in circularBuffer.
	// TODO: we probably should keep the recently acked events in a LRU cache.
	eventIndex map[string]struct{}
	//  Tracks the purge offset per client
	clientPurgeOffsets    map[string]uint64
	purgedEventsProcessor PurgedEventsProcessor

	metrics *HandlerMetrics
}

// NewEventStreamHandler creates an EventStreamHandler
func NewEventStreamHandler(
	bufferSize int,
	expectedClients []string,
	purgedEventsProcessor PurgedEventsProcessor,
	parentScope tally.Scope) *Handler {
	handler := Handler{
		streamID:              uuid.New(),
		circularBuffer:        cirbuf.NewCircularBuffer(bufferSize),
		eventIndex:            make(map[string]struct{}),
		clientPurgeOffsets:    make(map[string]uint64),
		purgedEventsProcessor: purgedEventsProcessor,
		expectedClients:       expectedClients,
		metrics:               NewHandlerMetrics(parentScope.SubScope("EventStreamHandler")),
	}
	handler.metrics.Capacity.Update(float64(handler.circularBuffer.Capacity()))
	for _, client := range expectedClients {
		handler.clientPurgeOffsets[client] = uint64(0)
	}
	return &handler
}

// Check if the client is expected
func (h *Handler) isClientExpected(clientName string) bool {
	for _, ok := h.clientPurgeOffsets[clientName]; ok; {
		return true
	}
	log.WithField("Request clientName", clientName).Error("Client not supported")
	h.metrics.UnexpectedClientError.Inc(1)
	return false
}

// AddEvent adds a task Event or mesos status update into the
// inner circular buffer
func (h *Handler) AddEvent(event *pb_eventstream.Event) error {
	if event == nil {
		return errors.New("event is nil")
	}
	h.metrics.AddEventAPI.Inc(1)
	log.WithFields(log.Fields{
		"Type": event.Type,
	}).Debug("Adding eventstream event")

	h.Lock()
	defer h.Unlock()

	var uid string
	if event.GetType() == pb_eventstream.Event_MESOS_TASK_STATUS {
		uid = uuid.UUID(event.GetMesosTaskStatus().GetUuid()).String()
		if _, ok := h.eventIndex[uid]; ok {
			h.metrics.AddEventDeDupe.Inc(1)
			return nil
		}
	}
	item, err := h.circularBuffer.AddItem(event)
	if err != nil {
		h.metrics.AddEventFail.Inc(1)
		return err
	}
	if uid != "" {
		h.eventIndex[uid] = struct{}{}
	}
	h.metrics.AddEventSuccess.Inc(1)
	head, tail := h.circularBuffer.GetRange()
	h.metrics.Head.Update(float64(head))
	h.metrics.Tail.Update(float64(tail))
	h.metrics.Size.Update(float64(head - tail))
	log.WithField("Current head", item.SequenceID).Debug("Event added")
	return nil
}

// GetEvents returns all the events pending in circular buffer
// This method is primarily for debugging purpose
func (h *Handler) GetEvents() ([]*pb_eventstream.Event, error) {
	h.Lock()
	defer h.Unlock()

	// Get and return data
	head, tail := h.circularBuffer.GetRange()
	items, err := h.circularBuffer.GetItemsByRange(tail, head)
	if err != nil {
		return nil, err
	}

	events := make([]*pb_eventstream.Event, 0, len(items))
	for _, item := range items {
		if event, ok := item.Value.(*pb_eventstream.Event); ok {
			e := &pb_eventstream.Event{
				Type:             event.Type,
				MesosTaskStatus:  event.MesosTaskStatus,
				PelotonTaskEvent: event.PelotonTaskEvent,
				HostEvent:        event.HostEvent,
				Offset:           item.SequenceID,
			}
			events = append(events, e)
		}
	}
	return events, nil
}

// InitStream handles the initstream request
func (h *Handler) InitStream(
	ctx context.Context,
	req *pb_eventstream.InitStreamRequest) (*pb_eventstream.InitStreamResponse, error) {
	h.Lock()
	defer h.Unlock()
	log.WithField("InitStream request", req).Debug("request")
	h.metrics.InitStreamAPI.Inc(1)
	var response pb_eventstream.InitStreamResponse
	clientName := req.ClientName
	clientSupported := h.isClientExpected(clientName)
	if !clientSupported {
		response.Error = &pb_eventstream.InitStreamResponse_Error{
			ClientUnsupported: &pb_eventstream.ClientUnsupported{
				Message: fmt.Sprintf("Client %v not supported, valid clients : %v", clientName, h.expectedClients),
			},
		}
		h.metrics.InitStreamFail.Inc(1)
		return &response, nil
	}
	response.StreamID = h.streamID
	_, tail := h.circularBuffer.GetRange()
	response.MinOffset = tail
	response.PreviousPurgeOffset = h.clientPurgeOffsets[clientName]
	log.WithField("InitStream response", response).Debug("")
	h.metrics.InitStreamSuccess.Inc(1)
	return &response, nil

}

// WaitForEvents handles the WaitForEvents request
func (h *Handler) WaitForEvents(
	ctx context.Context,
	req *pb_eventstream.WaitForEventsRequest) (*pb_eventstream.WaitForEventsResponse, error) {

	h.Lock()
	defer h.Unlock()
	h.metrics.WaitForEventsAPI.Inc(1)
	var response pb_eventstream.WaitForEventsResponse
	// Validate client
	clientName := req.ClientName
	clientSupported := h.isClientExpected(clientName)
	if !clientSupported {
		response.Error = &pb_eventstream.WaitForEventsResponse_Error{
			ClientUnsupported: &pb_eventstream.ClientUnsupported{
				Message: fmt.Sprintf("Client %v not supported, valid clients : %v", clientName, h.expectedClients),
			},
		}
		h.metrics.WaitForEventsFailed.Inc(1)
		return &response, nil
	}
	// Validate stream id
	streamID := req.StreamID
	if streamID != h.streamID {
		log.WithField("request_streamID", streamID).
			WithField("client_name", clientName).
			Warn("Invalid streamID")
		response.Error = &pb_eventstream.WaitForEventsResponse_Error{
			InvalidStreamID: &pb_eventstream.InvalidStreamID{
				CurrentStreamID: h.streamID,
			},
		}
		h.metrics.WaitForEventsFailed.Inc(1)
		h.metrics.InvalidStreamIDError.Inc(1)
		return &response, nil
	}
	// Get and return data
	head, tail := h.circularBuffer.GetRange()
	beginOffset := req.BeginOffset
	limit := req.Limit
	items, err := h.circularBuffer.GetItemsByRange(beginOffset, beginOffset+uint64(limit)-1)
	if err != nil {
		response.Error = &pb_eventstream.WaitForEventsResponse_Error{
			OutOfRange: &pb_eventstream.OffsetOutOfRange{
				MinOffset:       tail,
				MaxOffset:       head - 1,
				StreamID:        h.streamID,
				OffsetRequested: beginOffset,
			},
		}
		h.metrics.WaitForEventsFailed.Inc(1)
		return &response, nil
	}
	var events []*pb_eventstream.Event
	for _, item := range items {
		if event, ok := item.Value.(*pb_eventstream.Event); ok {
			e := &pb_eventstream.Event{
				Type:             event.Type,
				MesosTaskStatus:  event.MesosTaskStatus,
				PelotonTaskEvent: event.PelotonTaskEvent,
				HostEvent:        event.HostEvent,
				Offset:           item.SequenceID,
			}
			events = append(events, e)
		}
	}
	h.metrics.WaitForEventsSuccess.Inc(1)
	response.Events = events
	// Purge old data if specified
	if req.PurgeOffset > req.BeginOffset {
		log.WithFields(log.Fields{
			"purgeOffset":          req.PurgeOffset,
			"request begin offset": tail}).
			Error("Invalid purgeOffset")
		response.Error = &pb_eventstream.WaitForEventsResponse_Error{
			InvalidPurgeOffset: &pb_eventstream.InvalidPurgeOffset{
				PurgeOffset: req.PurgeOffset,
				BeginOffset: req.BeginOffset,
			},
		}
	}
	h.purgeEvents(clientName, req.PurgeOffset)
	return &response, nil
}

// purgeData scans the min of the purgeOffset for each client, and move the buffer tail
// to the minPurgeOffset
func (h *Handler) purgeEvents(clientName string, purgeOffset uint64) {
	h.clientPurgeOffsets[clientName] = purgeOffset
	var clientWithMinPurgeOffset string
	var minPurgeOffset uint64 = math.MaxUint64
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
				if event, ok := item.Value.(*pb_eventstream.Event); ok {
					if event.GetType() == pb_eventstream.Event_MESOS_TASK_STATUS {
						uid := uuid.UUID(event.GetMesosTaskStatus().GetUuid()).String()
						delete(h.eventIndex, uid)
					}
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
