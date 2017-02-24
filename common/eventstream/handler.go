package eventstream

import (
	"code.uber.internal/infra/peloton/common/cirbuf"
	"context"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
	"go.uber.org/yarpc"
	"math"
	mesos "mesos/v1"
	pb_eventstream "peloton/private/eventstream"
	"sync"
)

// PurgedEventsProcessor is the interface to handle the purged data
type PurgedEventsProcessor interface {
	EventPurged(events []*cirbuf.CircularBufferItem)
}

// Handler holds a circular buffer and serves request to pull data.
// This component is used in hostmgr and resmgr
// TODO:slu Add monitoring data point
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
}

// NewEventStreamHandler creates an EventStreamHandler
func NewEventStreamHandler(
	bufferSize int,
	expectedClients []string,
	purgedEventProcessor PurgedEventsProcessor) *Handler {
	handler := Handler{
		streamID:             uuid.NewUUID().String(),
		circularBuffer:       cirbuf.NewCircularBuffer(bufferSize),
		clientPurgeOffsets:   make(map[string]uint64),
		purgedEventProcessor: purgedEventProcessor,
		expectedClients:      expectedClients,
	}
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
	log.WithField("request clientName", clientName).Error("Client not supported")
	return false
}

// AddStatusUpdate adds an task status update into the inner circular buffer
func (h *Handler) AddStatusUpdate(taskStatus *mesos.TaskStatus) error {
	log.WithFields(log.Fields{
		"mesosTaskId": taskStatus.TaskId,
		"uuid":        taskStatus.Uuid}).
		Debug("Adding taskStatus")
	item, err := h.circularBuffer.AddItem(taskStatus)
	if err != nil {
		log.WithFields(log.Fields{
			"mesosTaskId": taskStatus.TaskId,
			"uuid":        taskStatus.Uuid,
			"error":       err}).
			Error("Adding taskStatus failed")
		return err
	}
	log.WithField("Current head", item.SequenceID).Debug("Event added")
	return nil

}

// InitStream handles the initstream request
func (h *Handler) InitStream(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *pb_eventstream.InitStreamRequest) (*pb_eventstream.InitStreamResponse, yarpc.ResMeta, error) {
	h.Lock()
	defer h.Unlock()
	var response pb_eventstream.InitStreamResponse
	clientName := req.ClientName
	clientSupported := h.isClientExpected(clientName)
	if !clientSupported {
		response.Error = &pb_eventstream.InitStreamResponse_Error{
			ClientUnsupported: &pb_eventstream.ClientUnsupported{
				Message: fmt.Sprintf("Client %v not supported, valid clients : %v", clientName, h.expectedClients),
			},
		}
		return &response, nil, nil
	}
	response.StreamID = h.streamID
	_, tail := h.circularBuffer.GetRange()
	response.MinOffset = tail
	return &response, nil, nil

}

// WaitForEvents handles the WaitForEvents request
func (h *Handler) WaitForEvents(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *pb_eventstream.WaitForEventsRequest) (*pb_eventstream.WaitForEventsResponse, yarpc.ResMeta, error) {
	h.Lock()
	defer h.Unlock()
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
		return &response, nil, nil
	}
	// Validate stream id
	streamID := req.StreamID
	if streamID != h.streamID {
		log.WithField("request streamID", clientName).Error("Invalid streamID")
		response.Error = &pb_eventstream.WaitForEventsResponse_Error{
			InvalidStreamID: &pb_eventstream.InvalidStreamID{
				CurrentStreamID: h.streamID,
			},
		}
		return &response, nil, nil
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
		return &response, nil, nil
	}
	var events []*pb_eventstream.Event
	for _, item := range items {
		e := pb_eventstream.Event{
			Offset:     item.SequenceID,
			TaskStatus: item.Value.(*mesos.TaskStatus),
		}
		events = append(events, &e)
	}
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
	return &response, nil, nil
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
	}
}
