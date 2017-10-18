package eventstream

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"

	"code.uber.internal/infra/peloton/common/cirbuf"

	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

const (
	_maxWaitForEventsIdle = 200 * time.Millisecond
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
	//  Tracks the purge offset per client
	clientPurgeOffsets   map[string]uint64
	purgedEventProcessor PurgedEventsProcessor

	metrics *HandlerMetrics

	cond *sync.Cond
}

// NewEventStreamHandler creates an EventStreamHandler
func NewEventStreamHandler(
	bufferSize int,
	expectedClients []string,
	purgedEventProcessor PurgedEventsProcessor,
	parentScope tally.Scope) *Handler {
	handler := &Handler{
		streamID:             uuid.New(),
		circularBuffer:       cirbuf.NewCircularBuffer(bufferSize),
		clientPurgeOffsets:   make(map[string]uint64),
		purgedEventProcessor: purgedEventProcessor,
		expectedClients:      expectedClients,
		metrics:              NewHandlerMetrics(parentScope.SubScope("EventStreamHandler")),
	}
	handler.cond = sync.NewCond(handler)
	handler.metrics.Capacity.Update(float64(handler.circularBuffer.Capacity()))
	for _, client := range expectedClients {
		handler.clientPurgeOffsets[client] = uint64(0)
	}
	return handler
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
	item, err := h.circularBuffer.AddItem(event)
	h.cond.Broadcast()
	h.Unlock()
	if err != nil {
		h.metrics.AddEventFail.Inc(1)
		log.WithFields(log.Fields{
			"Type":  event.Type,
			"error": err}).
			Error("Adding event failed")
		return err
	}
	h.metrics.AddEventSuccess.Inc(1)
	head, tail := h.circularBuffer.GetRange()
	h.metrics.Head.Update(float64(head))
	h.metrics.Tail.Update(float64(tail))
	h.metrics.Size.Update(float64(head - tail))
	log.WithField("Current head", item.SequenceID).Debug("Event added")
	return nil
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

	running := true
	timer := time.AfterFunc(_maxWaitForEventsIdle, func() {
		h.Lock()
		running = false
		h.cond.Broadcast()
		h.Unlock()
	})
	defer timer.Stop()

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

	// Purge old data if specified
	if req.PurgeOffset > req.BeginOffset {
		log.WithFields(log.Fields{
			"purgeOffset": req.PurgeOffset}).
			Error("Invalid purgeOffset")
		response.Error = &pb_eventstream.WaitForEventsResponse_Error{
			InvalidPurgeOffset: &pb_eventstream.InvalidPurgeOffset{
				PurgeOffset: req.PurgeOffset,
				BeginOffset: req.BeginOffset,
			},
		}
		return &response, nil
	}
	h.purgeEvents(clientName, req.PurgeOffset)

	beginOffset := req.BeginOffset
	limit := req.Limit

	for running {
		// Get and return data
		head, tail := h.circularBuffer.GetRange()
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

		// Wait until events are available.
		if len(items) == 0 {
			h.cond.Wait()
			continue
		}

		var events []*pb_eventstream.Event
		for _, item := range items {
			if event, ok := item.Value.(*pb_eventstream.Event); ok {
				e := &pb_eventstream.Event{
					Type:             event.Type,
					MesosTaskStatus:  event.MesosTaskStatus,
					PelotonTaskEvent: event.PelotonTaskEvent,
					Offset:           item.SequenceID,
				}
				events = append(events, e)
			}
		}
		h.metrics.WaitForEventsSuccess.Inc(1)
		response.Events = events
		break
	}
	return &response, nil
}

// purgeData scans the min of the purgeOffset for each client, and move the buffer tail
// to the minPurgeOffset
func (h *Handler) purgeEvents(clientName string, purgeOffset uint64) {
	h.clientPurgeOffsets[clientName] = purgeOffset
	var minPurgeOffset uint64
	minPurgeOffset = math.MaxUint64
	for _, p := range h.clientPurgeOffsets {
		if minPurgeOffset > p {
			minPurgeOffset = p
		}
	}
	head, tail := h.circularBuffer.GetRange()
	if minPurgeOffset >= tail && minPurgeOffset <= head {
		purgedItems, err := h.circularBuffer.MoveTail(minPurgeOffset)
		if err != nil {
			log.WithField("minPurgeOffset", minPurgeOffset).Error("Invalid minPurgeOffset")
			h.metrics.PurgeEventError.Inc(1)
		} else {
			if h.purgedEventProcessor != nil {
				h.purgedEventProcessor.EventPurged(purgedItems)
			}
		}
	} else {
		log.WithFields(log.Fields{
			"minPurgeOffset": minPurgeOffset,
			"tail":           tail,
			"head":           head,
		}).Error("minPurgeOffset incorrect")
		h.metrics.PurgeEventError.Inc(1)
	}
	h.metrics.Tail.Update(float64(tail))
	h.metrics.Size.Update(float64(head - tail))
}
